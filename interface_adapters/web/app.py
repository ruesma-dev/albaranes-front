# interface_adapters/web/app.py
from __future__ import annotations

import calendar
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import httpx
from fastapi import Body, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator

from application.services.review_service import ReviewService
from config.settings import Settings
from domain.models.contrato_refetch_models import ContratoRefetchOutcome
from domain.models.review_models import (
    ConciliacionOverridePayload,
    DocumentDetailPayload,
    DocumentListFilters,
    HealthResponse,
    MergeDocumentUpdatePayload,
    SaveResponse,
    VIEW_MODE_MERGE,
    normalize_view_mode,
)
from domain.ports.contrato_refetch_port import ContratoRefetchClient
from domain.ports.orchestrator_port import OrchestratorClient
from infrastructure.database.review_repository import AlbaranReviewRepository
from infrastructure.database.session_factory import SessionFactory
from infrastructure.database.workflow_runs_purger import WorkflowRunsPurger
from infrastructure.graph.token_provider import GraphTokenProvider
from infrastructure.colas.colas_orchestrator_client import ColasOrchestratorClient
from infrastructure.colas.colas_refetch_client import ColasRefetchClient
from infrastructure.sigrid.sigrid_lookup_client import SigridLookupClient
from infrastructure.sigrid.sigrid_api_contrato_client import (
    SigridApiContratoClient,
)
from infrastructure.sigrid.local_refetch_client import (
    LocalContratoRefetchClient,
)
from ruesma_comun.colas import construir_publicador
from ruesma_comun.colas.arranque import ConfiguracionColasAusenteError
from ruesma_comun.colas.publicador import PublicadorBestEffort

logger = logging.getLogger(__name__)


VIEW_LABELS = {
    VIEW_MODE_MERGE: "Consolidado (merge)",
    "openai": "OpenAI",
    "gemini": "Gemini",
    "claude": "Claude",
}


def _view_label(view_mode: str) -> str:
    return VIEW_LABELS.get(view_mode, view_mode.replace("_", " ").title())


# ------------------------------------------------------------------ #
# Filtros Jinja2 para formatear en servidor.
# ------------------------------------------------------------------ #
def _format_fecha_int_iso(value: Any) -> str:
    """INT YYYYMMDD (20260115) -> 'YYYY-MM-DD' (2026-01-15). 0/None -> '—'."""
    if value is None or value == "" or value == 0 or value == "0":
        return "—"
    try:
        n = int(value)
    except (TypeError, ValueError):
        return "—"
    if n < 1_000_00_01 or n > 9999_12_31:
        return "—"
    year = n // 10000
    month = (n // 100) % 100
    day = n % 100
    if month < 1 or month > 12 or day < 1 or day > 31:
        return "—"
    return f"{year:04d}-{month:02d}-{day:02d}"


def _format_importe_eur(value: Any) -> str:
    """float -> '335.370,42 €' (locale es-ES determinista). None/'' -> '—'."""
    if value is None or value == "":
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    formatted = "{:,.2f}".format(number)
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{formatted} €"


def _madrid_local(dt_utc: datetime) -> datetime:
    """UTC aware -> hora local de Madrid (CET/CEST) aplicando la regla DST
    de la UE, SIN depender de la base de datos de zonas (tzdata): así
    funciona igual en local y en Azure Functions. DST: del último domingo
    de marzo (01:00 UTC) al último domingo de octubre (01:00 UTC) -> UTC+2;
    el resto del año -> UTC+1.
    """
    def _last_sunday_0100(year: int, month: int) -> datetime:
        last_day = calendar.monthrange(year, month)[1]
        d = datetime(year, month, last_day, 1, 0, tzinfo=timezone.utc)
        # weekday(): lunes=0 … domingo=6. Retrocede hasta el domingo.
        return d - timedelta(days=(d.weekday() - 6) % 7)

    year = dt_utc.year
    dst_start = _last_sunday_0100(year, 3)
    dst_end = _last_sunday_0100(year, 10)
    offset_h = 2 if dst_start <= dt_utc < dst_end else 1
    return dt_utc + timedelta(hours=offset_h)


def _format_fecha_hora_local(value: Any) -> str:
    """ISO UTC ('2026-03-09T13:32:45.123+00:00') -> 'DD/MM/YYYY HH:MM' en
    hora de Madrid. Vacío / None / no parseable -> '' (cadena vacía, para
    que la plantilla pueda omitir la línea).
    """
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return _madrid_local(dt).strftime("%d/%m/%Y %H:%M")


# NOTA REFACTOR (mayo 2026): se eliminó aquí
# ``_build_contrato_pdf_storage`` y todos los imports/clases asociadas
# (SharePointContratoPdfStorage, SigridApiContratoClient,
# ContratoRefetchService local). El sv4 ya NO descarga ni sube PDFs
# de contrato — esa responsabilidad la tiene íntegramente el sv3,
# que lo hace durante el refetch (a través del endpoint
# /v1/albaranes/{id}/re-fetch-contratos). El sv4 solo conserva la
# preview del PDF del albarán propio, no del contrato asociado.


class ConciliacionEditBody(BaseModel):
    """Body del PATCH de edición de la fila salmon (jun 2026).

    Todos los campos opcionales: el front manda los visibles. Los
    numéricos llegan ya como número (el JS convierte coma decimal), pero
    por robustez toleramos cadena vacía, "—" o números con coma decimal.

    IMPORTANTE: definido a NIVEL DE MÓDULO (no dentro de build_app). Una
    clase Pydantic local provoca un ForwardRef que Pydantic 2.12 no puede
    resolver al usarla como Body(...), dando un 500 al guardar.
    """

    codigo_partida: str | None = None
    descripcion: str | None = None
    cantidad: float | None = None
    unidad: str | None = None
    precio_unitario: float | None = None
    descuento: float | None = None
    codigo_externo: str | None = None

    @field_validator(
        "cantidad", "precio_unitario", "descuento", mode="before"
    )
    @classmethod
    def _num_or_none(cls, v: object) -> float | None:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s or s == "\u2014":  # "" o "—"
            return None
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    @field_validator(
        "codigo_partida", "descripcion", "unidad", "codigo_externo",
        mode="before",
    )
    @classmethod
    def _str_or_none(cls, v: object) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        return s or None


class _PublicadorColasNulo:
    """Publicador no-op para correr el portal en LOCAL sin colas (sin
    Azurite ni workers): ``solo-front``.

    Cumple la MISMA interfaz que ``PublicadorBestEffort`` (``.publicar(...)``
    devuelve ``bool``) y SIEMPRE devuelve ``False``: el dato fundamental ya
    se persiste en BBDD antes de publicar, y los endpoints informan con
    honestidad de que no se encoló (no dicen "encolada" si no lo está). Se
    usa solo como fallback cuando NO hay ``COLAS_CONNECTION_STRING`` ni
    ``COLAS_ACCOUNT_URL`` en el entorno; en producción JAMÁS se usa (allí
    ``COLAS_ACCOUNT_URL`` está definida por el Container App).
    """

    def publicar(self, nombre_cola: str, mensaje: Any) -> bool:
        logger.warning(
            "[colas] publicador NULO (modo solo-front): descarto mensaje "
            "tipo=%s document_id=%s -> %s. Define COLAS_CONNECTION_STRING "
            "(local) o COLAS_ACCOUNT_URL (nube) para encolar de verdad.",
            getattr(mensaje, "tipo", "?"),
            getattr(mensaje, "document_id", "?"),
            nombre_cola,
        )
        return False


def build_app(settings: Settings) -> FastAPI:
    session_factory = SessionFactory(
        database_url=settings.database_url,
        admin_database_url=settings.admin_database_url,
        target_database_name=settings.pg_db,
        auto_create_database=settings.auto_create_database,
    )
    repository = AlbaranReviewRepository(session_factory)
    review_service = ReviewService(repository, settings.default_reviewer)
    tables_ready = review_service.initialize()

    app = FastAPI(
        title=settings.app_title,
        version=settings.service_version,
    )
    app.state.settings = settings
    app.state.review_service = review_service
    app.state.tables_ready = tables_ready
    app.state.graph_token_provider = (
        GraphTokenProvider(settings.graph_key, settings.graph_timeout_s)
        if settings.preview_enabled and settings.graph_key
        else None
    )

    # ------------------------------------------------------------------ #
    # Publicador de COLAS (sustituye a los clientes HTTP a sv7 y sv3).
    #
    # ``construir_publicador`` lee del ENTORNO:
    #   - COLAS_ACCOUNT_URL        (managed identity en Azure), o
    #   - COLAS_CONNECTION_STRING  (local/Azurite).
    # Se envuelve en PublicadorBestEffort: el dato fundamental
    # (selected_contrato_codigo, approved=true, hard-delete) YA está en
    # BBDD antes de publicar; el mensaje es solo el disparador y su
    # pérdida es recuperable a mano. Si no publica, el endpoint informa
    # al revisor con honestidad (no dice "encolada" si no lo está).
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # FALLBACK solo-front (jun 2026): si NO hay COLAS_* en el entorno, el
    # portal arranca igualmente con un publicador NULO. Asi puedes correr
    # SOLO el front (uvicorn / main.py) contra el Postgres de dev para
    # iterar la UI, SIN Azurite ni workers. Las acciones que encolan
    # (valorar/refetch/aprobar) no enviaran mensajes — el dato se guarda en
    # BBDD y el endpoint lo dice con honestidad. En produccion siempre hay
    # COLAS_ACCOUNT_URL, asi que se usa el publicador real.
    # ------------------------------------------------------------------ #
    try:
        publicador = PublicadorBestEffort(
            construir_publicador(emitido_por="ca-sv4-front")
        )
    except ConfiguracionColasAusenteError:
        logger.warning(
            "[colas][wiring] COLAS_* no configuradas -> publicador NULO "
            "(modo solo-front, sin Azurite ni workers). El portal arranca y "
            "guarda en BBDD; las acciones que encolan no enviaran mensajes. "
            "Define COLAS_CONNECTION_STRING (local) o COLAS_ACCOUNT_URL (nube) "
            "para activarlas."
        )
        publicador = _PublicadorColasNulo()

    # Re-fetch de contratos (antes Sv3RefetchClient → sv3 HTTP síncrono).
    # Ahora re-publica q-persistencia con force=True (sv3 re-ejecuta su
    # enrichment con force_refetch=True). ASÍNCRONO: el outcome lleva
    # status="queued" y el front debe avisar "refresca en unos segundos".
    sv3_refetch_client: ContratoRefetchClient = ColasRefetchClient(
        publicador=publicador,
    )
    # ------------------------------------------------------------------ #
    # FALLBACK solo-front (jun 2026): si NO hay colas (publicador NULO)
    # pero SÍ hay credenciales Sigrid, el re-fetch de contratos se hace
    # LOCAL y SÍNCRONO (Sigrid directo → persiste contratos+líneas en
    # BBDD), para poder ASOCIAR contratos sin sv3 ni Azurite. Así, al
    # elegir un contrato encontrado en vivo, queda cacheado y el PUT ya
    # no anula la selección (se acaba el 409 "sin contrato"). OJO: esto
    # NO lanza la valoración — el orquestador también va por cola; en
    # solo-front el /valuate informará de que no se encoló. En producción
    # (con COLAS_*) se mantiene el ColasRefetchClient (asíncrono → sv3).
    # ------------------------------------------------------------------ #
    if isinstance(publicador, _PublicadorColasNulo) and settings.sigrid_lookup_enabled:
        try:
            _contrato_client = SigridApiContratoClient(
                base_url=settings.sigrid_api_base_url,
                function_key=settings.sigrid_api_function_key,
                database=settings.sigrid_api_database,
                timeout_s=settings.sigrid_api_timeout_s,
            )
            sv3_refetch_client = LocalContratoRefetchClient(
                sigrid_client=_contrato_client,
                repository=repository,
            )
            logger.info(
                "[contrato-refetch][wiring] FALLBACK LOCAL activado "
                "(solo-front + Sigrid): re-fetch síncrono sin sv3."
            )
        except Exception:
            logger.exception(
                "[contrato-refetch][wiring] no se pudo activar el fallback "
                "local; se mantiene el de cola (no operativo en solo-front)."
            )
    app.state.sv3_refetch_client = sv3_refetch_client

    # Orquestador (antes HttpOrchestratorClient → sv7, disuelto). Ahora:
    #   - contract-selected → MensajeValoracion(force=True) → q-valoracion
    #   - document-approved → MensajeFeedback → q-feedback
    #   - document-purged   → limpieza directa de workflow_runs (BBDD),
    #     que desbloquea el dedup por contenido tras el hard-delete.
    workflow_runs_purger = WorkflowRunsPurger(session_factory)
    orchestrator_client: OrchestratorClient = ColasOrchestratorClient(
        publicador=publicador,
        workflow_runs_purger=workflow_runs_purger,
    )
    app.state.orchestrator_client = orchestrator_client
    logger.info(
        "[colas][wiring] publicador best-effort + adapters de cola "
        "CABLEADOS (emitido_por=ca-sv4-front)"
    )

    # ------------------------------------------------------------------ #
    # Cliente de SOLO LECTURA a Sigrid para los desplegables de cabecera
    # (elegir proveedor con contrato en la obra / elegir obra). Se
    # construye solo si hay credenciales (SIGRID_API_*). Si no, queda
    # None y los endpoints /api/sigrid/* responden ok=false (el front
    # mantiene la entrada manual).
    #
    # Nota de arquitectura: el camino de ESCRITURA de contratos
    # (UPSERT/PDF) sigue yendo por sv3. Esto es un lookup de referencia
    # de solo lectura para la UI; no reintroduce aquel acoplamiento.
    # ------------------------------------------------------------------ #
    sigrid_lookup_client: SigridLookupClient | None = None
    if settings.sigrid_lookup_enabled:
        sigrid_lookup_client = SigridLookupClient(
            base_url=settings.sigrid_api_base_url,
            function_key=settings.sigrid_api_function_key,
            database=settings.sigrid_api_database,
            timeout_s=settings.sigrid_api_timeout_s,
        )
        logger.info(
            "[sigrid-lookup][wiring] CABLEADO base_url=%s database=%s",
            settings.sigrid_api_base_url,
            settings.sigrid_api_database,
        )
    else:
        logger.info(
            "[sigrid-lookup][wiring] DESACTIVADO (faltan SIGRID_API_*); "
            "los desplegables de cabecera quedaran en entrada manual."
        )
    app.state.sigrid_lookup_client = sigrid_lookup_client

    templates = Jinja2Templates(
        directory=str(Path(__file__).resolve().parents[2] / "templates")
    )
    templates.env.filters["tojson_pretty"] = lambda value: json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
    )
    templates.env.filters["fecha_int_iso"] = _format_fecha_int_iso
    templates.env.filters["importe_eur"] = _format_importe_eur
    templates.env.filters["fecha_hora_local"] = _format_fecha_hora_local
    templates.env.globals["view_label"] = _view_label
    # Cache-buster para CSS/JS: un valor único por arranque del
    # servicio. Cuando reiniciamos sv4, los navegadores ven una URL
    # nueva (...styles.css?v=1715000000) y recargan el archivo en vez
    # de servir la versión cacheada. En desarrollo es indispensable
    # — sin esto, Ctrl+R no basta y hay que Ctrl+F5 cada vez.
    import time as _time
    templates.env.globals["asset_version"] = str(int(_time.time()))
    app.mount(
        "/static",
        StaticFiles(directory=str(Path(__file__).resolve().parents[2] / "static")),
        name="static",
    )

    # ------------------------------------------------------------------ #
    # Identidad del revisor desde Easy Auth (Entra ID).
    #
    # Container Apps con Easy Auth inyecta el UPN del usuario autenticado
    # en la cabecera ``X-MS-CLIENT-PRINCIPAL-NAME`` en cada petición. La
    # usamos para atribuir quién aprueba/purga/valora/elimina. Fallback a
    # ``default_reviewer`` en local (sin Easy Auth) o si la cabecera no
    # llega.
    # ------------------------------------------------------------------ #
    def _reviewer_from_request(request: Request) -> str | None:
        principal = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if principal and principal.strip():
            return principal.strip()
        return settings.default_reviewer

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        ready = review_service.initialize()
        app.state.tables_ready = ready
        return HealthResponse(
            ok=True,
            service="albaranes-review-web",
            version=settings.service_version,
            tables_ready=ready,
            details={
                "database": settings.pg_db,
                "default_page_size": settings.default_page_size,
                "max_page_size": settings.max_page_size,
                "preview_enabled": settings.preview_enabled,
                "refetch_mode": "async-via-q-persistencia",
            },
        )

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/documents", status_code=302)

    # ------------------------------------------------------------------ #
    # Desplegables de cabecera: lookups de SOLO LECTURA a Sigrid.
    #   GET /api/sigrid/proveedores?obra={codigo}
    #   GET /api/sigrid/obras
    # Devuelven {ok, items:[...]} o {ok:false, error:...} (200 con ok=false
    # para que el front muestre aviso y mantenga la entrada manual, sin
    # romper la pagina).
    # ------------------------------------------------------------------ #
    @app.get("/api/sigrid/proveedores", include_in_schema=False)
    def sigrid_proveedores(obra: str = Query(default="")) -> JSONResponse:
        client = app.state.sigrid_lookup_client
        if client is None:
            return JSONResponse(
                {"ok": False, "error": "Sigrid no configurado en el sv4 "
                 "(faltan SIGRID_API_*).", "items": []}
            )
        codigo = (obra or "").strip()
        if not codigo:
            return JSONResponse(
                {"ok": False, "error": "Falta el codigo de obra.", "items": []}
            )
        try:
            proveedores = client.fetch_proveedores_por_obra(codigo_obra=codigo)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[sigrid-lookup] proveedores obra=%s fallo: %r", codigo, exc)
            return JSONResponse(
                {"ok": False, "error": f"Error consultando Sigrid: {exc}", "items": []}
            )
        items = [{"cif": p.cif, "nombre": p.nombre} for p in proveedores]
        return JSONResponse({"ok": True, "items": items})

    @app.get("/api/sigrid/obras", include_in_schema=False)
    def sigrid_obras() -> JSONResponse:
        client = app.state.sigrid_lookup_client
        if client is None:
            return JSONResponse(
                {"ok": False, "error": "Sigrid no configurado en el sv4 "
                 "(faltan SIGRID_API_*).", "items": []}
            )
        try:
            obras = client.fetch_obras()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[sigrid-lookup] obras fallo: %r", exc)
            return JSONResponse(
                {"ok": False, "error": f"Error consultando Sigrid: {exc}", "items": []}
            )
        items = [{"codigo": o.codigo, "nombre": o.nombre} for o in obras]
        return JSONResponse({"ok": True, "items": items})

    @app.get("/api/sigrid/partidas", include_in_schema=False)
    def sigrid_partidas(obra: str = Query(default="")) -> JSONResponse:
        # Partidas HOJA del presupuesto de la obra en Sigrid (obrparpar),
        # para el desplegable de partida de la fila salmón. SOLO lectura.
        client = app.state.sigrid_lookup_client
        if client is None:
            return JSONResponse(
                {"ok": False, "error": "Sigrid no configurado en el sv4 "
                 "(faltan SIGRID_API_*).", "items": []}
            )
        codigo = (obra or "").strip()
        if not codigo:
            return JSONResponse(
                {"ok": False, "error": "Falta el codigo de obra.", "items": []}
            )
        try:
            partidas = client.fetch_partidas_por_obra(codigo_obra=codigo)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[sigrid-lookup] partidas obra=%s fallo: %r", codigo, exc)
            return JSONResponse(
                {"ok": False, "error": f"Error consultando Sigrid: {exc}", "items": []}
            )
        items = [
            {
                "codigo": p.codigo,
                "descripcion": p.descripcion,
                "descripcion_agregada": p.descripcion_agregada,
            }
            for p in partidas
        ]
        return JSONResponse({"ok": True, "items": items})

    @app.get("/api/sigrid/contratos", include_in_schema=False)
    def sigrid_contratos(
        obra: str = Query(default=""),
        cif: str = Query(default=""),
    ) -> JSONResponse:
        # Búsqueda EN VIVO de contratos en Sigrid por obra (+ cif opcional),
        # igual que los desplegables de obra/proveedor. SOLO lectura; el
        # cacheo de líneas (para poder valorar) lo hace el re-fetch de sv3
        # cuando el front confirma la selección.
        client = app.state.sigrid_lookup_client
        if client is None:
            return JSONResponse(
                {"ok": False, "error": "Sigrid no configurado en el sv4 "
                 "(faltan SIGRID_API_*).", "items": []}
            )
        codigo = (obra or "").strip()
        if not codigo:
            return JSONResponse(
                {"ok": False, "error": "Falta el codigo de obra.", "items": []}
            )
        try:
            contratos = client.fetch_contratos(
                codigo_obra=codigo,
                cif=(cif or "").strip() or None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[sigrid-lookup] contratos obra=%s fallo: %r", codigo, exc
            )
            return JSONResponse(
                {"ok": False, "error": f"Error consultando Sigrid: {exc}", "items": []}
            )
        items = [
            {
                "codigo": c.codigo,
                "nombre": c.nombre,
                "cif": c.cif,
                "nombre_proveedor": c.nombre_proveedor,
            }
            for c in contratos
        ]
        return JSONResponse({"ok": True, "items": items})

    @app.get("/documents", response_class=HTMLResponse)
    def documents_list(
        request: Request,
        search: str | None = Query(default=None),
        proveedor: str | None = Query(default=None),
        fecha: str | None = Query(default=None),
        obra: str | None = Query(default=None),
        # Filtros EXACTOS de las vistas Obra/Proveedor (jul 2026).
        obra_codigo: str | None = Query(default=None),
        proveedor_cif: str | None = Query(default=None),
        albaran: str | None = Query(default=None),
        contrato: str | None = Query(default=None),
        lineas: str | None = Query(default=None),
        min_importe: str | None = Query(default=None),
        max_importe: str | None = Query(default=None),
        approved: str = Query(default="pending"),
        review_required: str = Query(default="all"),
        min_confidence: str | None = Query(default=None),
        max_confidence: str | None = Query(default=None),
        sort_by: str = Query(default="confidence_pct_calc"),
        sort_dir: str = Query(default="asc"),
        vista: str = Query(default="activos"),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(
            default=settings.default_page_size,
            ge=1,
            le=settings.max_page_size,
        ),
        message: str | None = Query(default=None),
    ) -> HTMLResponse:
        filters = DocumentListFilters(
            search=search,
            proveedor=proveedor,
            fecha=fecha,
            obra=obra,
            obra_codigo=obra_codigo,
            proveedor_cif=proveedor_cif,
            albaran=albaran,
            contrato=contrato,
            lineas=lineas,
            min_importe=_parse_optional_float(min_importe, field_name="min_importe"),
            max_importe=_parse_optional_float(max_importe, field_name="max_importe"),
            approved=approved,
            review_required=review_required,
            min_confidence=_parse_optional_float(min_confidence, field_name="min_confidence"),
            max_confidence=_parse_optional_float(max_confidence, field_name="max_confidence"),
            sort_by=sort_by,
            sort_dir=sort_dir,
            vista=vista,
            page=page,
            page_size=page_size,
        )
        result = review_service.list_documents(filters)
        current_query = _query_string(filters)
        context = {
            "request": request,
            "title": settings.app_title,
            "filters": filters,
            "result": result,
            "message": message,
            "current_query": current_query,
            "build_sort_query": lambda column: _query_string(
                filters,
                {
                    "sort_by": column,
                    "sort_dir": (
                        "desc"
                        if filters.sort_by == column and filters.sort_dir == "asc"
                        else "asc"
                    ),
                    "page": 1,
                },
            ),
            "build_page_query": lambda new_page: _query_string(
                filters,
                {"page": new_page},
            ),
        }
        return templates.TemplateResponse(
            request=request,
            name="documents_list.html",
            context=context,
        )

    # ---------------------------------------------------------------- #
    # Vistas agregadas (jul 2026): Obras y Proveedores. Cada fila enlaza
    # a la bandeja YA filtrada (filtros exactos obra_codigo /
    # proveedor_cif); el detalle del albaran es el mismo de siempre.
    # ---------------------------------------------------------------- #
    @app.get("/obras", response_class=HTMLResponse)
    def obras_list(
        request: Request,
        search: str | None = Query(default=None),
    ) -> HTMLResponse:
        items = review_service.list_obras_resumen(search=search)
        return templates.TemplateResponse(
            request=request,
            name="obras_list.html",
            context={
                "request": request,
                "title": settings.app_title,
                "items": items,
                "search": (search or "").strip(),
            },
        )

    @app.get("/proveedores", response_class=HTMLResponse)
    def proveedores_list(
        request: Request,
        search: str | None = Query(default=None),
    ) -> HTMLResponse:
        items = review_service.list_proveedores_resumen(search=search)
        return templates.TemplateResponse(
            request=request,
            name="proveedores_list.html",
            context={
                "request": request,
                "title": settings.app_title,
                "items": items,
                "search": (search or "").strip(),
            },
        )

    @app.post("/documents/{document_id}/approve", include_in_schema=False)
    def approve_from_list(
        document_id: str,
        request: Request,
        redirect_query: str = Form(default=""),
        approved_by: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.approve_document(
            document_id=document_id,
            approved_by=approved_by.strip() or _reviewer_from_request(request),
        )
        query = redirect_query.strip()
        message = urlencode({"message": "Documento aprobado"})
        if query:
            glue = "&" if query else ""
            return RedirectResponse(
                url=f"/documents?{query}{glue}&{message}".replace("?&", "?"),
                status_code=303,
            )
        return RedirectResponse(url=f"/documents?{message}", status_code=303)

    @app.post("/documents/{document_id}/unapprove", include_in_schema=False)
    def unapprove_from_list(
        document_id: str,
        redirect_query: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.unapprove_document(document_id=document_id)
        query = redirect_query.strip()
        message = urlencode({"message": "Documento marcado como pendiente"})
        if query:
            return RedirectResponse(
                url=f"/documents?{query}&{message}".replace("?&", "?"),
                status_code=303,
            )
        return RedirectResponse(url=f"/documents?{message}", status_code=303)

    # ------------------------------------------------------------------ #
    # Soft-delete (jun 2026): borrar (a papelera) y restaurar.
    # ------------------------------------------------------------------ #
    @app.post("/documents/{document_id}/delete", include_in_schema=False)
    def delete_from_list(
        document_id: str,
        request: Request,
        redirect_query: str = Form(default=""),
        deleted_by: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.delete_document(
            document_id=document_id,
            deleted_by=deleted_by.strip() or _reviewer_from_request(request),
        )
        query = redirect_query.strip()
        message = urlencode({"message": "Albarán movido a la papelera"})
        if query:
            return RedirectResponse(
                url=f"/documents?{query}&{message}".replace("?&", "?"),
                status_code=303,
            )
        return RedirectResponse(url=f"/documents?{message}", status_code=303)

    @app.post("/documents/{document_id}/restore", include_in_schema=False)
    def restore_from_list(
        document_id: str,
        redirect_query: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.restore_document(document_id=document_id)
        query = redirect_query.strip()
        message = urlencode({"message": "Albarán restaurado"})
        if query:
            return RedirectResponse(
                url=f"/documents?{query}&{message}".replace("?&", "?"),
                status_code=303,
            )
        return RedirectResponse(url=f"/documents?{message}", status_code=303)

    # ------------------------------------------------------------------ #
    # Vaciar papelera (jul 2026): purga TODOS los albaranes borrados.
    # Reutiliza hard_delete_document por documento (y su limpieza de
    # dedup vía orquestador), en vez de un DELETE masivo que duplicaría
    # el plan de borrado hijos→padres del repositorio.
    # ------------------------------------------------------------------ #
    @app.post("/documents/trash/empty", include_in_schema=False)
    def empty_trash(request: Request) -> RedirectResponse:
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        orch: OrchestratorClient = app.state.orchestrator_client
        purged_by = _reviewer_from_request(request)

        ids = review_service.list_trash_document_ids()
        purgados = 0
        fallidos = 0
        for doc_id in ids:
            try:
                source_sha256 = review_service.hard_delete_document(
                    document_id=doc_id,
                )
            except Exception:  # noqa: BLE001 - seguir con el resto
                logger.exception(
                    "[empty-trash] fallo purgando doc=%s; se continúa",
                    doc_id,
                )
                fallidos += 1
                continue
            purgados += 1
            outcome = orch.notify_document_purged(
                document_id=doc_id,
                source_sha256=source_sha256,
                purged_by=purged_by,
                purged_at_utc=now_utc,
            )
            if outcome is None:
                logger.warning(
                    "[empty-trash] no se confirmó la limpieza de dedup "
                    "doc=%s; el borrado local SÍ se aplicó.",
                    doc_id,
                )

        if fallidos:
            texto = (
                f"Papelera: {purgados} albarán(es) eliminados; "
                f"{fallidos} fallaron (reintenta)."
            )
        else:
            texto = f"Papelera vaciada: {purgados} albarán(es) eliminados."
        message = urlencode({"message": texto})
        return RedirectResponse(
            url=f"/documents?vista=papelera&approved=all&{message}",
            status_code=303,
        )

    @app.post("/documents/{document_id}/purge", include_in_schema=False)
    def purge_from_list(
        document_id: str,
        request: Request,
        redirect_query: str = Form(default=""),
    ) -> RedirectResponse:
        # Hard-delete: borrado físico irreversible. Solo desde la papelera.
        source_sha256 = review_service.hard_delete_document(
            document_id=document_id,
        )

        # ------------------------------------------------------------ #
        # Limpieza del dedup tras la purga.
        #
        # Sin esto, las filas de workflow_runs seguían vivas tras el
        # hard-delete y su dedup por attachment_sha256 respondía "PDF ya
        # procesado" al reenviar el mismo albarán, aunque ya no existía
        # en el portal. Ahora el adapter de colas borra esas filas
        # directamente (misma BBDD). Best-effort: si falla, la purga
        # local YA está hecha y se puede reintentar.
        # ------------------------------------------------------------ #
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        orch: OrchestratorClient = app.state.orchestrator_client
        outcome = orch.notify_document_purged(
            document_id=document_id,
            source_sha256=source_sha256,
            purged_by=_reviewer_from_request(request),
            purged_at_utc=now_utc,
        )
        if outcome is None:
            logger.warning(
                "[purge] no se confirmó la limpieza de dedup doc=%s; el "
                "borrado local SÍ se aplicó. Si reenvías el mismo PDF y "
                "aparece como duplicado, revisa el log del purger "
                "(tabla/columna de workflow_runs) y reintenta la purga.",
                document_id,
            )

        query = redirect_query.strip()
        message = urlencode({"message": "Albarán eliminado definitivamente"})
        if query:
            return RedirectResponse(
                url=f"/documents?{query}&{message}".replace("?&", "?"),
                status_code=303,
            )
        return RedirectResponse(url=f"/documents?{message}", status_code=303)

    @app.get("/documents/{document_id}", response_class=HTMLResponse)
    def document_detail(
        request: Request,
        document_id: str,
        view: str = Query(default=VIEW_MODE_MERGE),
        message: str | None = Query(default=None),
    ) -> HTMLResponse:
        requested_view = normalize_view_mode(view)
        document = review_service.get_document(
            document_id,
            view_mode=requested_view,
        )
        if document is None:
            raise HTTPException(status_code=404, detail="Documento no encontrado")

        # -------------------------------------------------------------- #
        # Navegacion anterior/siguiente (jun 2026): recorre el MISMO
        # conjunto ordenado+filtrado de la bandeja (cruzando paginas). Los
        # filtros viajan en la query del enlace que abrio este detalle
        # (ver documents_list.html). Si no hay query (acceso directo al
        # detalle), se usan los filtros por defecto del modelo.
        # -------------------------------------------------------------- #
        nav_filters = _filters_from_query(
            request, default_page_size=settings.default_page_size
        )
        nav_query = _query_string(nav_filters)
        prev_id, next_id = review_service.get_neighbor_ids(
            document_id=document_id, filters=nav_filters
        )

        def _detail_url(doc_id: str) -> str:
            extra: dict[str, Any] = {}
            if requested_view != VIEW_MODE_MERGE:
                extra["view"] = requested_view
            tail = urlencode(extra)
            q = "&".join(part for part in (nav_query, tail) if part)
            return f"/documents/{doc_id}?{q}" if q else f"/documents/{doc_id}"

        context = {
            "request": request,
            "title": settings.app_title,
            "document": document,
            "document_json": json.dumps(document.model_dump(), ensure_ascii=False),
            "message": message,
            "preview_enabled": settings.preview_enabled,
            "document_preview_url": f"/documents/{document.id}/preview",
            "current_view": document.view_mode,
            "available_views": document.available_views,
            "view_label": _view_label,
            "back_to_list_url": f"/documents?{nav_query}" if nav_query else "/documents",
            "nav_prev_url": _detail_url(prev_id) if prev_id else None,
            "nav_next_url": _detail_url(next_id) if next_id else None,
        }
        return templates.TemplateResponse(
            request=request,
            name="document_detail.html",
            context=context,
        )

    @app.get("/documents/{document_id}/contrato-pdf")
    def document_contrato_pdf(document_id: str) -> Response:
        """Abre el PDF del contrato en SharePoint. Resuelve la URL por
        consulta directa a BBDD y redirige (302). Asi el boton del front
        funciona aunque el web_url no llegue al payload del detalle."""
        url = review_service.get_contrato_pdf_url(document_id)
        if not url:
            raise HTTPException(
                status_code=404,
                detail="Este albaran no tiene PDF de contrato en SharePoint.",
            )
        return RedirectResponse(url, status_code=302)

    @app.get("/documents/{document_id}/preview", response_class=Response)
    def document_preview(document_id: str) -> Response:
        document = review_service.get_document(document_id)
        if document is None:
            raise HTTPException(status_code=404, detail="Documento no encontrado")

        if not settings.preview_enabled:
            return _preview_error_response(
                title="Vista previa no configurada",
                message=(
                    "Faltan GRAPH_KEY o SHAREPOINT_DRIVE_ID en el .env del servicio."
                ),
                external_url=document.document_url,
            )

        relative_path = (document.document_storage_ref or "").strip()
        if not relative_path:
            return _preview_error_response(
                title="Documento sin referencia interna",
                message=(
                    "El registro no tiene document_storage_ref y no se puede leer "
                    "el archivo desde SharePoint por Graph."
                ),
                external_url=document.document_url,
            )

        token_provider: GraphTokenProvider | None = app.state.graph_token_provider
        if token_provider is None:
            return _preview_error_response(
                title="Token provider no disponible",
                message="No se pudo inicializar el acceso a Microsoft Graph.",
                external_url=document.document_url,
            )

        encoded_path = quote(relative_path.lstrip("/"), safe="/")
        metadata_url = (
            f"https://graph.microsoft.com/v1.0/drives/{settings.sharepoint_drive_id}"
            f"/root:/{encoded_path}"
        )

        try:
            token = token_provider.get_token()
            headers = {"Authorization": f"Bearer {token}"}
            timeout = httpx.Timeout(
                settings.graph_timeout_s,
                connect=min(20, settings.graph_timeout_s),
            )
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                metadata_response = client.get(metadata_url, headers=headers)
                if metadata_response.status_code >= 300:
                    logger.warning(
                        "Preview metadata error. status=%s body=%s path=%s",
                        metadata_response.status_code,
                        metadata_response.text[:500],
                        relative_path,
                    )
                    return _preview_error_response(
                        title="No se pudo localizar el archivo",
                        message=(
                            f"Graph devolvió {metadata_response.status_code} al "
                            f"resolver el documento en SharePoint."
                        ),
                        external_url=document.document_url,
                    )

                payload = metadata_response.json()
                item_id = str(payload["id"])
                content_url = (
                    f"https://graph.microsoft.com/v1.0/drives/"
                    f"{settings.sharepoint_drive_id}/items/{item_id}/content"
                )
                content_response = client.get(content_url, headers=headers)
                if content_response.status_code >= 300:
                    logger.warning(
                        "Preview content error. status=%s body=%s item_id=%s path=%s",
                        content_response.status_code,
                        content_response.text[:500],
                        item_id,
                        relative_path,
                    )
                    return _preview_error_response(
                        title="No se pudo descargar el archivo",
                        message=(
                            f"Graph devolvió {content_response.status_code} al "
                            f"descargar el documento."
                        ),
                        external_url=document.document_url,
                    )

                media_type = _guess_media_type(document.source_filename or relative_path)
                return Response(
                    content=content_response.content,
                    media_type=media_type,
                    headers={
                        "Content-Disposition": (
                            "inline; "
                            f'filename="{document.source_filename or Path(relative_path).name}"'
                        ),
                        "Cache-Control": "no-store",
                    },
                )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Error obteniendo preview desde SharePoint. document_id=%s path=%s",
                document_id,
                relative_path,
            )
            return _preview_error_response(
                title="Error obteniendo vista previa",
                message=str(exc),
                external_url=document.document_url,
            )

    @app.get("/api/documents/{document_id}", response_model=DocumentDetailPayload)
    def document_detail_api(
        document_id: str,
        view: str = Query(default=VIEW_MODE_MERGE),
    ) -> DocumentDetailPayload:
        document = review_service.get_document(
            document_id,
            view_mode=normalize_view_mode(view),
        )
        if document is None:
            raise HTTPException(status_code=404, detail="Documento no encontrado")
        return document

    @app.put("/api/documents/{document_id}", response_model=SaveResponse)
    async def save_document_api(
        document_id: str,
        payload: MergeDocumentUpdatePayload,
    ) -> SaveResponse:
        try:
            detail = review_service.save_document(
                document_id=document_id,
                payload=payload,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return SaveResponse(
            ok=True,
            document_id=detail.id,
            approved=detail.approved,
            redirect_url=(
                f"/documents/{detail.id}?message="
                f"{'Documento guardado y aprobado' if detail.approved else 'Documento guardado'}"
            ),
            message=(
                "Documento guardado y aprobado"
                if detail.approved
                else "Documento guardado"
            ),
        )

    # ------------------------------------------------------------------ #
    # Re-fetch manual de contratos desde el portal.
    #
    # Delega íntegramente en el sv3 vía HTTP. El sv3 es ahora el
    # único responsable de hablar con Sigrid y persistir contratos.
    # El sv4 sigue exponiendo el mismo path para no romper el front.
    #
    # Códigos de respuesta:
    #   * 200 + outcome JSON — el sv3 procesó correctamente la
    #     petición (con o sin contratos encontrados). El campo
    #     outcome.status detalla.
    #   * 404 — el documento no existe en BBDD.
    #   * 5xx — fallo irrecuperable (raro: el cliente HTTP traduce
    #     casi todos los fallos a outcomes con status=sigrid_error).
    # ------------------------------------------------------------------ #
    @app.post("/api/documents/{document_id}/re-fetch-contratos")
    def refetch_contratos_api(document_id: str) -> dict:
        # Pre-validamos que el documento existe LOCALMENTE para dar
        # un 404 rápido sin pegarle al sv3 con peticiones inútiles.
        preview = review_service.get_document(document_id)
        if preview is None:
            raise HTTPException(
                status_code=404,
                detail="Documento no encontrado",
            )

        client: ContratoRefetchClient = app.state.sv3_refetch_client
        try:
            outcome: ContratoRefetchOutcome = client.refetch(
                document_id=document_id,
            )
        except KeyError as exc:
            # El sv3 dijo 404 (no debería pasar si la pre-validación
            # local pasó, pero por si las moscas — race condition con
            # un delete entre la pre-validación y la llamada).
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        logger.info(
            "[contrato-refetch][api] document_id=%s outcome=%s count=%s "
            "selected=%s",
            document_id,
            outcome.status,
            outcome.count,
            outcome.selected_contrato_codigo,
        )
        return {
            "status": outcome.status,
            "count": outcome.count,
            "selected_contrato_codigo": outcome.selected_contrato_codigo,
            "message": outcome.message,
            "cif": outcome.cif,
            "obra_codigo": outcome.obra_codigo,
        }

    # ------------------------------------------------------------------ #
    # ENDPOINT: Botón "Valorar ahora" del portal.
    #
    # Flujo:
    #   1. El revisor selecciona un contrato (o ya lo tenía seleccionado).
    #   2. Pulsa "Valorar ahora".
    #   3. El sv4 emite ContractSelectedEvent al sv7.
    #   4. El sv7 transita el workflow a VALUING y lanza la valoración
    #      en background (sv6 → sv5 → genera líneas sintéticas).
    #   5. El front muestra toast "Valoración encolada" y el revisor
    #      refresca tras unos segundos.
    #
    # Semánticamente equivale a "como si el revisor acabara de elegir
    # el contrato". El sv7 ya maneja todos los sub-casos:
    #   - workflow en AWAITING_CONTRACT_SELECTION → transición a VALUING
    #   - workflow en VALUATION_FAILED → reabrir
    #   - workflow en AWAITING_APPROVAL o terminal → spawn revaluation
    #
    # Best-effort: si sv7 está caído, el endpoint NO falla; loguea y
    # devuelve 200 con un mensaje informativo. El estado (contrato
    # seleccionado) ya está persistido en BBDD; cuando sv7 vuelva, el
    # revisor puede pulsar otra vez.
    # ------------------------------------------------------------------ #
    @app.post("/api/documents/{document_id}/valuate")
    def valuate_document_api(document_id: str, request: Request) -> dict:
        # 1) Verificar que el documento existe localmente.
        preview = review_service.get_document(document_id)
        if preview is None:
            raise HTTPException(
                status_code=404,
                detail="Documento no encontrado",
            )

        # 2) Leer el contrato seleccionado directamente de BBDD (no del
        #    payload, para evitar que el front mande algo distinto).
        codigo_contrato: str | None = getattr(
            preview, "selected_contrato_codigo", None,
        )
        if not codigo_contrato:
            raise HTTPException(
                status_code=409,
                detail=(
                    "El documento no tiene contrato seleccionado. "
                    "Elige un contrato antes de valorar."
                ),
            )

        # 2b) (jul 2026) Guardia: sin líneas leídas del albarán no hay
        #     nada que valorar. Antes se encolaba igualmente, sv5
        #     respondía 400, el mensaje quedaba envenenado en la cola
        #     (reintento eterno cada 10 min) y el front mostraba
        #     "Valorando..." infinito. sv6 también cierra ya estos
        #     casos como 'failed' (defensa en profundidad); esta
        #     guardia evita directamente el viaje inútil.
        if not getattr(preview, "lines", None):
            return {
                "ok": False,
                "accepted": False,
                "document_id": document_id,
                "message": (
                    "El documento no tiene líneas leídas del albarán: "
                    "no hay nada que valorar. Revisa la extracción o "
                    "reprocesa el documento antes de valorar."
                ),
            }

        # 3) Componer el evento. El selected_at_utc es el INSTANTE del
        #    click, no la fecha del save anterior. Esto sirve como
        #    correlation_key del sv7: dos clicks separados → dos
        #    revaluations (legítimas); doble-click rápido (mismo
        #    timestamp ISO al segundo) → idempotente.
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        client: OrchestratorClient = app.state.orchestrator_client
        reviewer = _reviewer_from_request(request)
        outcome: dict | None = None
        try:
            outcome = client.notify_contract_selected(
                document_id=document_id,
                codigo_contrato=codigo_contrato,
                selected_by=reviewer,
                selected_at_utc=now_utc,
            )
        except Exception:
            # El cliente ya es best-effort; este try es por si en una
            # versión futura cambia el contrato. NO propagamos: el
            # usuario pulsó un botón y queremos darle feedback.
            logger.exception(
                "[valuate][api] error inesperado publicando valoración "
                "document_id=%s codigo=%s",
                document_id, codigo_contrato,
            )

        # ------------------------------------------------------------ #
        # FIX (jun 2026) — honestidad del "Valorar ahora".
        #
        # Antes este endpoint SIEMPRE respondía "Valoración encolada"
        # aunque sv7 estuviera caído o respondiera action='no_op'
        # (p.ej. documento sin workflow asociado). El front se quedaba
        # sondeando 2 minutos una valoración que nunca iba a llegar
        # (bug reportado: "dice que lanza valorar, pero no lo hace").
        # Ahora el resultado del orquestador viaja en la respuesta y el
        # front decide si sondear o avisar del problema.
        # ------------------------------------------------------------ #
        orchestrator_ok = outcome is not None
        action = (outcome or {}).get("action")
        detail = (outcome or {}).get("detail")
        if not orchestrator_ok:
            message = (
                "No se pudo encolar la valoración. La selección de "
                "contrato SÍ está guardada; reintenta en unos segundos."
            )
        elif action == "no_op":
            message = (
                "No se encontró nada que valorar: "
                f"{detail or 'sin detalle'}."
            )
        else:
            message = (
                "Valoración encolada. Refresca la página en unos "
                "segundos para ver el resultado."
            )

        logger.info(
            "[valuate][api] document_id=%s codigo=%s reviewer=%s "
            "orchestrator_ok=%s action=%s",
            document_id, codigo_contrato, reviewer,
            orchestrator_ok, action,
        )
        return {
            "accepted": orchestrator_ok and action != "no_op",
            "orchestrator_ok": orchestrator_ok,
            "orchestrator_action": action,
            "orchestrator_detail": detail,
            "workflow_id": (outcome or {}).get("workflow_id"),
            "document_id": document_id,
            "codigo_contrato": codigo_contrato,
            "selected_at_utc": now_utc,
            "message": message,
        }

    @app.patch(
        "/api/documents/{document_id}/lines/{valuation_line_id}/conciliacion"
    )
    def set_line_conciliacion_api(
        document_id: str,
        valuation_line_id: int,
        payload: ConciliacionOverridePayload = Body(...),
    ) -> dict:
        ok = review_service.set_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            mode=payload.mode,
            matched_contrato_line_id=payload.matched_contrato_line_id,
            descripcion=payload.descripcion,
            precio_unitario=payload.precio_unitario,
        )
        if not ok:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No se pudo aplicar la conciliacion (linea, contrato "
                    "o modo no validos)."
                ),
            )
        return {
            "ok": True,
            "document_id": document_id,
            "valuation_line_id": valuation_line_id,
            "mode": payload.mode,
        }

    # IMPORTANTE: path PROPIO (.../conciliacion/campos), distinto del PATCH
    # de arriba (set_line_conciliacion, que espera 'mode'). Antes ambos
    # compartían .../conciliacion como PATCH y FastAPI enrutaba los dos al
    # primero (el de 'mode'), de modo que el "Guardar" de la fila salmón
    # (sin 'mode') caía en el handler equivocado y daba 422 mode required.
    @app.patch(
        "/api/documents/{document_id}/lines/{valuation_line_id}/conciliacion/campos"
    )
    def update_line_conciliacion_api(
        document_id: str,
        valuation_line_id: int,
        payload: ConciliacionEditBody = Body(...),
    ) -> dict:
        ok = review_service.update_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            codigo_partida=payload.codigo_partida,
            descripcion=payload.descripcion,
            cantidad=payload.cantidad,
            unidad=payload.unidad,
            precio_unitario=payload.precio_unitario,
            descuento=payload.descuento,
            codigo_externo=payload.codigo_externo,
        )
        if not ok:
            raise HTTPException(
                status_code=404,
                detail="Línea de valoración no encontrada",
            )
        return {
            "ok": True,
            "document_id": document_id,
            "valuation_line_id": valuation_line_id,
        }

    @app.delete(
        "/api/documents/{document_id}/lines/{valuation_line_id}/conciliacion"
    )
    def remove_line_conciliacion_api(
        document_id: str,
        valuation_line_id: int,
    ) -> dict:
        ok = review_service.remove_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
        )
        if not ok:
            raise HTTPException(
                status_code=400,
                detail="No se pudo borrar la conciliacion (linea no valida).",
            )
        return {
            "ok": True,
            "document_id": document_id,
            "valuation_line_id": valuation_line_id,
        }

    @app.post(
        "/api/documents/{document_id}/lines/by-merge/{merge_line_id}/conciliacion"
    )
    def add_conciliacion_for_merge_line_api(
        document_id: str,
        merge_line_id: int,
        payload: ConciliacionOverridePayload,
    ) -> dict:
        # Trae una conciliacion a una linea NO casada. Admite:
        #  - mode=contract_line + matched_contrato_line_id (salmon Sigrid)
        #  - mode=nueva (salmon Nueva, copia la blanca del albaran)
        if payload.mode == "contract_line" and payload.matched_contrato_line_id is None:
            raise HTTPException(
                status_code=400,
                detail="Se requiere matched_contrato_line_id para mode=contract_line.",
            )
        if payload.mode not in ("contract_line", "nueva"):
            raise HTTPException(
                status_code=400,
                detail="mode no valido (contract_line | nueva).",
            )
        ok = review_service.add_conciliacion_for_merge_line(
            document_id=document_id,
            merge_line_id=merge_line_id,
            mode=payload.mode,
            matched_contrato_line_id=payload.matched_contrato_line_id,
            precio_unitario=payload.precio_unitario,
            descripcion=payload.descripcion,
        )
        if not ok:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No se pudo traer la linea de contrato (linea de albaran "
                    "o de contrato no validas)."
                ),
            )
        return {
            "ok": True,
            "document_id": document_id,
            "merge_line_id": merge_line_id,
            "matched_contrato_line_id": payload.matched_contrato_line_id,
        }

    @app.post("/api/documents/{document_id}/lines/standalone")
    def add_standalone_line_api(document_id: str) -> dict:
        # Crea una linea salmon SUELTA (sin linea blanca): una valuation
        # line nueva con conciliacion "Nueva" en blanco. El usuario la
        # rellena o la engancha a una linea de contrato desde el combo de
        # descripcion de la salmon. NO toca ninguna linea blanca.
        new_id = review_service.add_standalone_valuation_line(
            document_id=document_id,
        )
        if new_id is None:
            raise HTTPException(
                status_code=400,
                detail="No se pudo crear la linea salmon (documento no valido).",
            )
        return {
            "ok": True,
            "document_id": document_id,
            "valuation_line_id": new_id,
        }

    # ------------------------------------------------------------------ #
    # DESHACER (jul 2026, mismo contrato de API que en partes-front):
    # historial persistente de las ediciones manuales y revertido del
    # más reciente. El widget flotante de base.html los consume.
    # ------------------------------------------------------------------ #
    @app.get("/api/undo/list", include_in_schema=False)
    def undo_list() -> JSONResponse:
        try:
            items = review_service.list_undo(limit=15)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[undo] list falló: %r", exc)
            items = []
        return JSONResponse({"items": items})

    @app.post("/api/undo", include_in_schema=False)
    def undo_apply() -> JSONResponse:
        try:
            res = review_service.undo_last()
        except Exception as exc:  # noqa: BLE001
            logger.exception("[undo] fallo al deshacer")
            res = {"ok": False, "error": str(exc)}
        return JSONResponse(res)

    @app.post("/api/documents/{document_id}/lines/from-contrato")
    def add_lines_from_contrato_api(
        document_id: str,
        payload: dict = Body(...),
    ) -> dict:
        # Traer líneas contrato (jul 2026): crea una línea salmón YA
        # casada (badge Sigrid) por cada línea de contrato seleccionada
        # en el desplegable multiselección del detalle, prefijada con
        # partida/descripción/unidad/precio del contrato. La cantidad
        # queda en blanco para el revisor.
        raw_ids = payload.get("contrato_line_ids") or []
        try:
            ids = [int(x) for x in raw_ids]
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail="contrato_line_ids debe ser una lista de enteros.",
            )
        if not ids:
            raise HTTPException(
                status_code=400,
                detail="Selecciona al menos una línea de contrato.",
            )
        creadas = review_service.add_valuation_lines_from_contrato(
            document_id=document_id,
            contrato_line_ids=ids,
        )
        if creadas == 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Ninguna de las líneas seleccionadas pertenece a un "
                    "contrato de este albarán."
                ),
            )
        return {
            "ok": True,
            "document_id": document_id,
            "created": creadas,
        }

    @app.exception_handler(KeyError)
    async def key_error_handler(_: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    return app


def _parse_optional_float(value: str | None, *, field_name: str) -> float | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "type": "float_parsing",
                    "loc": ["query", field_name],
                    "msg": "Input should be a valid number, unable to parse string as a number",
                    "input": value,
                }
            ],
        ) from exc


def _query_string(
    filters: DocumentListFilters,
    overrides: dict[str, Any] | None = None,
) -> str:
    payload = filters.model_dump()
    if overrides:
        payload.update(overrides)
    return urlencode(
        {
            key: value
            for key, value in payload.items()
            if value not in (None, "")
        }
    )


def _filters_from_query(
    request: Request,
    *,
    default_page_size: int,
) -> DocumentListFilters:
    """Reconstruye los filtros de la BANDEJA a partir de la query del
    detalle. Sirve para que los botones anterior/siguiente recorran el
    mismo conjunto ordenado que el revisor venia mirando en la lista.

    A diferencia de la ruta ``/documents`` (que usa ``Query(...)`` con
    validacion estricta y devuelve 422 si algo no parsea), aqui somos
    TOLERANTES: un parametro roto en la URL no debe tumbar la pagina de
    detalle. Los numeros que no parsean quedan en None y page/page_size
    se acotan al rango valido del modelo.
    """
    q = request.query_params

    def _f(name: str) -> float | None:
        raw = q.get(name)
        if raw is None or not raw.strip():
            return None
        try:
            return float(raw.strip())
        except ValueError:
            return None

    def _i(name: str, default: int) -> int:
        raw = q.get(name)
        if raw is None or not raw.strip():
            return default
        try:
            return int(raw.strip())
        except ValueError:
            return default

    page = max(1, _i("page", 1))
    page_size = min(100, max(1, _i("page_size", default_page_size)))

    return DocumentListFilters(
        search=q.get("search"),
        proveedor=q.get("proveedor"),
        fecha=q.get("fecha"),
        obra=q.get("obra"),
        obra_codigo=q.get("obra_codigo"),
        proveedor_cif=q.get("proveedor_cif"),
        albaran=q.get("albaran"),
        contrato=q.get("contrato"),
        lineas=q.get("lineas"),
        min_importe=_f("min_importe"),
        max_importe=_f("max_importe"),
        approved=q.get("approved", "pending"),
        review_required=q.get("review_required", "all"),
        min_confidence=_f("min_confidence"),
        max_confidence=_f("max_confidence"),
        sort_by=q.get("sort_by", "confidence_pct_calc"),
        sort_dir=q.get("sort_dir", "asc"),
        vista=q.get("vista", "activos"),
        page=page,
        page_size=page_size,
    )


def _guess_media_type(filename: str) -> str:
    suffix = Path(filename or "").suffix.lower()
    if suffix == ".pdf":
        return "application/pdf"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    return "application/octet-stream"


def _preview_error_response(
    *,
    title: str,
    message: str,
    external_url: str | None,
) -> HTMLResponse:
    safe_title = html.escape(title)
    safe_message = html.escape(message)
    safe_external = html.escape(external_url) if external_url else None
    open_link_html = (
        f'<p><a href="{safe_external}" target="_blank" rel="noreferrer">'
        "Abrir documento en SharePoint"
        "</a></p>"
        if safe_external
        else ""
    )
    payload = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="utf-8">
        <title>Vista previa no disponible</title>
        <style>
            body {{ font-family: Arial, sans-serif; background: #f8fafc; color: #1f2937; margin: 0; padding: 24px; }}
            .card {{ max-width: 720px; margin: 0 auto; background: #ffffff; border: 1px solid #dbe4f0; border-radius: 12px; padding: 24px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06); }}
            h1 {{ margin-top: 0; font-size: 22px; }}
            p {{ line-height: 1.5; }}
            a {{ color: #2563eb; text-decoration: none; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>{safe_title}</h1>
            <p>{safe_message}</p>
            {open_link_html}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=payload, status_code=200)
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
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

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
from infrastructure.graph.token_provider import GraphTokenProvider
from infrastructure.http.orchestrator_client import HttpOrchestratorClient
from infrastructure.http.sv3_refetch_client import Sv3RefetchClient
from infrastructure.sigrid.sigrid_lookup_client import SigridLookupClient

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
    # Wiring del cliente HTTP al sv3 para el re-fetch manual de
    # contratos desde el portal.
    #
    # Antes el sv4 mantenía su propio cliente Sigrid + ContratoRefetchService
    # + SharePointContratoPdfStorage, duplicando código del sv3 y
    # pisando los sigrid_ide del UPSERT. Tras el refactor (mayo 2026)
    # el sv4 delega ÍNTEGRAMENTE en el sv3: hace POST al endpoint
    # /v1/albaranes/{id}/re-fetch-contratos y reenvía el outcome al
    # front.
    #
    # El cliente HTTP se construye SIEMPRE (no depende de credenciales
    # — el sv4 no tiene que conocerlas). Si el sv3 está caído o no
    # tiene Sigrid cableado, el cliente devuelve outcomes con
    # status=sigrid_error y mensajes útiles; el portal no se rompe.
    # ------------------------------------------------------------------ #
    sv3_refetch_client: ContratoRefetchClient = Sv3RefetchClient(
        base_url=settings.sv3_base_url,
        path=settings.sv3_path_refetch_contratos,
        timeout_s=settings.sv3_timeout_s,
    )
    logger.info(
        "[contrato-refetch][wiring] Sv3RefetchClient CABLEADO base_url=%s "
        "path=%s timeout=%ss",
        settings.sv3_base_url,
        settings.sv3_path_refetch_contratos,
        settings.sv3_timeout_s,
    )
    app.state.sv3_refetch_client = sv3_refetch_client

    # ------------------------------------------------------------------ #
    # Cliente al orquestador (sv7).
    #
    # Antes existía como archivo (infrastructure/http/orchestrator_client.py)
    # pero NUNCA se instanciaba ni se llamaba — el sv4 jamás notificaba
    # eventos al sv7 y por tanto la valoración nunca arrancaba desde el
    # portal. Aquí lo activamos.
    #
    # Best-effort: el cliente captura cualquier fallo HTTP/red y lo
    # loguea sin propagar. La razón es que el dato fundamental
    # (selected_contrato_codigo, approved=true) YA está persistido en
    # albaran_documents_merge antes de llamar al sv7. El evento es solo
    # el "trigger" para que sv7 actúe; si se pierde, no se pierde estado.
    # ------------------------------------------------------------------ #
    orchestrator_client: OrchestratorClient = HttpOrchestratorClient(
        base_url=settings.sv7_base_url,
        path_contract_selected=settings.sv7_path_contract_selected,
        path_document_approved=settings.sv7_path_document_approved,
        path_document_purged=settings.sv7_path_document_purged,
        timeout_s=settings.sv7_timeout_s,
    )
    logger.info(
        "[orchestrator][wiring] HttpOrchestratorClient CABLEADO base_url=%s "
        "timeout=%ss",
        settings.sv7_base_url,
        settings.sv7_timeout_s,
    )
    app.state.orchestrator_client = orchestrator_client

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
                "sv3_refetch_client_url": settings.sv3_base_url,
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

    @app.post("/documents/{document_id}/approve", include_in_schema=False)
    def approve_from_list(
        document_id: str,
        redirect_query: str = Form(default=""),
        approved_by: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.approve_document(
            document_id=document_id,
            approved_by=approved_by.strip() or settings.default_reviewer,
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
        redirect_query: str = Form(default=""),
        deleted_by: str = Form(default=""),
    ) -> RedirectResponse:
        review_service.delete_document(
            document_id=document_id,
            deleted_by=deleted_by.strip() or settings.default_reviewer,
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

    @app.post("/documents/{document_id}/purge", include_in_schema=False)
    def purge_from_list(
        document_id: str,
        redirect_query: str = Form(default=""),
    ) -> RedirectResponse:
        # Hard-delete: borrado físico irreversible. Solo desde la papelera.
        source_sha256 = review_service.hard_delete_document(
            document_id=document_id,
        )

        # ------------------------------------------------------------ #
        # FIX (jun 2026) — avisar a sv7 de la purga.
        #
        # Sin este evento, los workflow_runs del orquestador seguían
        # vivos tras el hard-delete y su dedup por attachment_sha256
        # respondía "PDF ya procesado" al reenviar el mismo albarán,
        # aunque ya no existía en el portal (bug reportado). sv7 marca
        # esos workflows como 'purged' y deja de bloquear el contenido.
        # Best-effort: si sv7 está caído, la purga local YA está hecha;
        # el operador puede purgar de nuevo otro documento o reintentar.
        # ------------------------------------------------------------ #
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        orch: OrchestratorClient = app.state.orchestrator_client
        outcome = orch.notify_document_purged(
            document_id=document_id,
            source_sha256=source_sha256,
            purged_by=settings.default_reviewer,
            purged_at_utc=now_utc,
        )
        if outcome is None:
            logger.warning(
                "[purge] sv7 no confirmó document-purged doc=%s; el "
                "borrado local SÍ se aplicó. Si reenvías el mismo PDF y "
                "sv7 lo marca como duplicado, reintenta la purga con sv7 "
                "levantado.",
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
        }
        return templates.TemplateResponse(
            request=request,
            name="document_detail.html",
            context=context,
        )

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
    def valuate_document_api(document_id: str) -> dict:
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

        # 3) Componer el evento. El selected_at_utc es el INSTANTE del
        #    click, no la fecha del save anterior. Esto sirve como
        #    correlation_key del sv7: dos clicks separados → dos
        #    revaluations (legítimas); doble-click rápido (mismo
        #    timestamp ISO al segundo) → idempotente.
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        client: OrchestratorClient = app.state.orchestrator_client
        outcome: dict | None = None
        try:
            outcome = client.notify_contract_selected(
                document_id=document_id,
                codigo_contrato=codigo_contrato,
                selected_by=settings.default_reviewer,
                selected_at_utc=now_utc,
            )
        except Exception:
            # El cliente ya es best-effort; este try es por si en una
            # versión futura cambia el contrato. NO propagamos: el
            # usuario pulsó un botón y queremos darle feedback.
            logger.exception(
                "[valuate][api] error inesperado notificando sv7 "
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
                "No se pudo contactar con el orquestador (sv7). La "
                "selección de contrato SÍ está guardada; arranca el "
                "sv7 y pulsa \"Valorar ahora\" de nuevo."
            )
        elif action == "no_op":
            message = (
                "El orquestador no encontró nada que valorar: "
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
            document_id, codigo_contrato, settings.default_reviewer,
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
        payload: ConciliacionOverridePayload,
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

    class ConciliacionEditBody(BaseModel):
        """Body del PATCH de edición de la fila salmon (jun 2026).

        Todos los campos opcionales: el front manda los visibles. Los
        numéricos llegan ya como número (el JS convierte coma decimal).
        """

        codigo_partida: str | None = None
        descripcion: str | None = None
        cantidad: float | None = None
        unidad: str | None = None
        precio_unitario: float | None = None
        descuento: float | None = None
        codigo_externo: str | None = None

    @app.patch(
        "/api/documents/{document_id}/lines/{valuation_line_id}/conciliacion"
    )
    def update_line_conciliacion_api(
        document_id: str,
        valuation_line_id: int,
        payload: ConciliacionEditBody,
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

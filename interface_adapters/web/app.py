# interface_adapters/web/app.py
from __future__ import annotations

import html
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import httpx
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from application.services.review_service import ReviewService
from config.settings import Settings
from domain.models.contrato_refetch_models import ContratoRefetchOutcome
from domain.models.review_models import (
    DocumentDetailPayload,
    DocumentListFilters,
    HealthResponse,
    MergeDocumentUpdatePayload,
    SaveResponse,
    VIEW_MODE_MERGE,
    normalize_view_mode,
)
from domain.ports.contrato_refetch_port import ContratoRefetchClient
from infrastructure.database.review_repository import AlbaranReviewRepository
from infrastructure.database.session_factory import SessionFactory
from infrastructure.graph.token_provider import GraphTokenProvider
from infrastructure.http.sv3_refetch_client import Sv3RefetchClient

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

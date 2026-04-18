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
from domain.models.review_models import (
    DocumentDetailPayload,
    DocumentListFilters,
    HealthResponse,
    MergeDocumentUpdatePayload,
    SaveResponse,
    VIEW_MODE_MERGE,
    normalize_view_mode,
)
from infrastructure.database.review_repository import AlbaranReviewRepository
from infrastructure.database.session_factory import SessionFactory
from infrastructure.graph.token_provider import GraphTokenProvider

logger = logging.getLogger(__name__)


# Etiquetas legibles para mostrar en el desplegable de selector de vista.
# Cualquier provider_origin no listado aquí se mostrará con su nombre en crudo.
VIEW_LABELS = {
    VIEW_MODE_MERGE: "Consolidado (merge)",
    "openai": "OpenAI",
    "gemini": "Gemini",
    "claude": "Claude",
}


def _view_label(view_mode: str) -> str:
    return VIEW_LABELS.get(view_mode, view_mode.replace("_", " ").title())


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

    templates = Jinja2Templates(
        directory=str(Path(__file__).resolve().parents[2] / "templates")
    )
    templates.env.filters["tojson_pretty"] = lambda value: json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
    )
    templates.env.globals["view_label"] = _view_label
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
            min_confidence=_parse_optional_float(min_confidence, field_name='min_confidence'),
            max_confidence=_parse_optional_float(max_confidence, field_name='max_confidence'),
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
        # El preview se sirve SIEMPRE desde la información del merge porque es
        # ahí donde viven las referencias a SharePoint (todos los proveedores
        # comparten el mismo PDF original).
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
        # La escritura solo aplica al merge (fuente de verdad para la revisión).
        # El front oculta los botones de guardar/aprobar en las vistas por
        # proveedor; aquí simplemente se escribe contra el merge por id.
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
            body {{
                font-family: Arial, sans-serif;
                background: #f8fafc;
                color: #1f2937;
                margin: 0;
                padding: 24px;
            }}
            .card {{
                max-width: 720px;
                margin: 0 auto;
                background: #ffffff;
                border: 1px solid #dbe4f0;
                border-radius: 12px;
                padding: 24px;
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
            }}
            h1 {{ margin-top: 0; font-size: 22px; }}
            p {{ line-height: 1.5; }}
            code {{
                display: block;
                white-space: pre-wrap;
                background: #f1f5f9;
                border-radius: 8px;
                padding: 12px;
                color: #0f172a;
            }}
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

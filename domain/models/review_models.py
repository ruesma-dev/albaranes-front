# domain/models/review_models.py
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

VIEW_MODE_MERGE = "merge"
KNOWN_PROVIDER_VIEWS = ("openai", "gemini", "claude")
ALLOWED_VIEW_MODES = (VIEW_MODE_MERGE, *KNOWN_PROVIDER_VIEWS)


class DocumentListFilters(BaseModel):
    search: str | None = None
    approved: str = Field(default="pending")
    review_required: str = Field(default="all")
    min_confidence: float | None = None
    max_confidence: float | None = None
    sort_by: str = Field(default="confidence_pct_calc")
    sort_dir: str = Field(default="asc")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=25, ge=1, le=100)

    @field_validator("approved")
    @classmethod
    def validate_approved(cls, value: str) -> str:
        value = (value or "all").strip().lower()
        return value if value in {"all", "approved", "pending"} else "all"

    @field_validator("review_required")
    @classmethod
    def validate_review_required(cls, value: str) -> str:
        value = (value or "all").strip().lower()
        return value if value in {"all", "yes", "no"} else "all"

    @field_validator("sort_by")
    @classmethod
    def validate_sort_by(cls, value: str) -> str:
        allowed = {
            "created_at_utc",
            "fecha",
            "proveedor_nombre",
            "obra_codigo",
            "numero_albaran",
            "confidence_pct_calc",
            "approved",
        }
        value = (value or "confidence_pct_calc").strip()
        return value if value in allowed else "confidence_pct_calc"

    @field_validator("sort_dir")
    @classmethod
    def validate_sort_dir(cls, value: str) -> str:
        value = (value or "asc").strip().lower()
        return value if value in {"asc", "desc"} else "asc"


class DocumentListItem(BaseModel):
    id: str
    source_document_id: str | None = None
    source_filename: str
    proveedor_nombre: str | None = None
    fecha: str | None = None
    obra_codigo: str | None = None
    obra_nombre: str | None = None
    numero_albaran: str | None = None
    confidence_pct_calc: float | None = None
    review_required: bool | None = None
    approved: bool = False
    provider_origin: str
    created_at_utc: str
    document_url: str | None = None


class ProviderSnapshot(BaseModel):
    id: str
    provider_origin: str
    model_name: str
    proveedor_nombre: str | None = None
    fecha: str | None = None
    numero_albaran: str | None = None
    obra_codigo: str | None = None
    raw_extraction_json: str | None = None
    ia_output_json: str | None = None


class MergeLinePayload(BaseModel):
    id: int | None = None
    line_index: int | None = None
    external_line_id: str | None = None
    cabecera_id: str | None = None
    codigo: str | None = None
    cantidad: float | None = None
    concepto: str | None = None
    precio: float | None = None
    descuento: float | None = None
    precio_neto: float | None = None
    codigo_imputacion: str | None = None
    confianza_pct: float | None = None
    confidence_pct_calc: float | None = None
    line_match_score: float | None = None
    comparison_status_json: str | None = None
    field_scores_json: str | None = None


class ContratoPayload(BaseModel):
    """Datos de un contrato asociado al (proveedor, obra) del documento.

    Los tipos numéricos ``fecha_*`` vienen como INT YYYYMMDD (p.e.
    20241122) — la capa de presentación los formatea a DD/MM/YYYY. Un
    valor de ``0`` en vigencia significa "sin vigencia establecida".
    """

    id: int
    codigo_contrato: str
    nombre_contrato: str | None = None
    fecha_alta_contrato: int | None = None
    fecha_contrato: int | None = None
    vigencia_desde: int | None = None
    vigencia_hasta: int | None = None
    importe_total: float | None = None
    cif_proveedor: str | None = None
    nombre_proveedor: str | None = None
    codigo_obra: str | None = None
    nombre_obra: str | None = None


class MergeDocumentUpdatePayload(BaseModel):
    proveedor_nombre: str | None = None
    proveedor_cif: str | None = None
    fecha: str | None = None
    numero_albaran: str | None = None
    forma_pago: str | None = None
    obra_codigo: str | None = None
    obra_nombre: str | None = None
    obra_direccion: str | None = None
    # Contrato seleccionado por el usuario. None = sin seleccionar.
    # Se valida contra los contratos cargados del documento al guardar.
    selected_contrato_codigo: str | None = None
    review_notes: str | None = None
    approved: bool = False
    approved_by: str | None = None
    lines: list[MergeLinePayload] = Field(default_factory=list)


class DocumentDetailPayload(BaseModel):
    id: str
    view_mode: str = Field(default=VIEW_MODE_MERGE)
    available_views: list[str] = Field(default_factory=list)
    is_editable: bool = True
    provider_document_id: str | None = None
    source_document_id: str | None = None
    document_storage_ref: str | None = None
    source_filename: str
    provider_origin: str
    model_name: str
    proveedor_nombre: str | None = None
    proveedor_cif: str | None = None
    fecha: str | None = None
    numero_albaran: str | None = None
    forma_pago: str | None = None
    obra_codigo: str | None = None
    obra_nombre: str | None = None
    obra_direccion: str | None = None
    document_url: str | None = None
    confidence_pct_calc: float | None = None
    review_required: bool | None = None
    review_reasons_json: str | None = None
    comparison_summary_json: str | None = None
    raw_extraction_json: str | None = None
    ia_output_json: str | None = None
    approved: bool = False
    approved_at_utc: str | None = None
    approved_by: str | None = None
    reviewed_at_utc: str | None = None
    review_notes: str | None = None
    created_at_utc: str
    lines: list[MergeLinePayload] = Field(default_factory=list)
    provider_snapshots: list[ProviderSnapshot] = Field(default_factory=list)
    # Bloque de contratos enriquecidos por el servicio 3 desde la BBDD on-prem.
    contratos: list[ContratoPayload] = Field(default_factory=list)
    selected_contrato_codigo: str | None = None


class PaginatedDocuments(BaseModel):
    items: list[DocumentListItem]
    total: int
    page: int
    page_size: int
    total_pages: int
    approved_count: int
    pending_count: int
    review_required_count: int


class SaveResponse(BaseModel):
    ok: bool
    document_id: str
    approved: bool
    redirect_url: str
    message: str


class HealthResponse(BaseModel):
    ok: bool
    service: str
    version: str
    tables_ready: bool
    details: dict[str, Any]


def normalize_view_mode(value: str | None) -> str:
    if not value:
        return VIEW_MODE_MERGE
    cleaned = str(value).strip().lower()
    if cleaned in ALLOWED_VIEW_MODES:
        return cleaned
    # Permitir cualquier provider_origin futuro (azure_di, etc.). La
    # validación real contra datos reales la hace el repositorio.
    return cleaned or VIEW_MODE_MERGE

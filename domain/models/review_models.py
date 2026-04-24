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
    pdf_sharepoint_relative_path: str | None = None
    pdf_sharepoint_web_url: str | None = None


# ====================================================================== #
# Sub-tanda 2D — ValuationLineUpdate
#
# Payload con los campos editables de una línea sintética cuando el
# revisor la toca en la tabla del formulario. Identifica la fila por
# su valuation_line_id (PK de albaran_line_valuations). El servicio 4
# hace UPDATE limitado a estos campos.
#
# Se coloca antes de MergeDocumentUpdatePayload para que éste pueda
# referenciarlo directamente sin forward-reference.
# ====================================================================== #

class ValuationLineUpdate(BaseModel):
    valuation_line_id: int
    codigo_partida_final: str | None = None
    descripcion_linea: str | None = None
    cantidad_albaran: float | None = None
    unidad_contrato: str | None = None
    precio_unitario_final: float | None = None
    importe_calculado: float | None = None


class MergeDocumentUpdatePayload(BaseModel):
    proveedor_nombre: str | None = None
    proveedor_cif: str | None = None
    fecha: str | None = None
    numero_albaran: str | None = None
    forma_pago: str | None = None
    obra_codigo: str | None = None
    obra_nombre: str | None = None
    obra_direccion: str | None = None
    selected_contrato_codigo: str | None = None
    review_notes: str | None = None
    approved: bool = False
    approved_by: str | None = None
    lines: list[MergeLinePayload] = Field(default_factory=list)
    # Sub-tanda 2D — ediciones del revisor sobre filas sintéticas.
    # Se aplican a ``albaran_line_valuations`` por valuation_line_id.
    # Si la lista llega vacía o ausente, no se toca ninguna sintética.
    valuation_line_updates: list[ValuationLineUpdate] = Field(
        default_factory=list,
    )


# ====================================================================== #
# NUEVO — bloque de VALORACIÓN.
#
# Se rellena cuando existe una fila en ``albaran_valuations`` para el
# document_id. Si no hay valoración todavía, ``valuation=None`` y el
# front pinta los campos con los valores extraídos del albarán (como
# antes). No añade UI de edición/relanzado — solo lectura.
# ====================================================================== #

class LineValuationPayload(BaseModel):
    """Una fila de ``albaran_line_valuations`` (servicio 6)."""

    # ---- sub-tanda 2D ----
    # id de la fila en albaran_line_valuations. Necesario para que el
    # revisor pueda editar las sintéticas (UPDATE por id).
    # Opcional para compatibilidad con payloads ya construidos
    # (se fue añadiendo progresivamente).
    valuation_line_id: int | None = None

    # merge_line_id:
    #   - int: línea 'from_albaran' (viene del albarán, línea del merge).
    #   - None: línea 'synthetic_modifier' (generada por el valorador
    #     para representar un modificador implícito: incremento por
    #     año, consistencia, árido, aditivo, residuos, tiempo).
    merge_line_id: int | None = None

    matched_contrato_line_id: int | None = None
    derived_contrato_line_id: int | None = None

    precio_unitario_contrato_db: float | None = None
    precio_unitario_pdf_inferido: float | None = None
    precio_unitario_final: float | None = None
    precio_unitario_source: str | None = None
    precio_unitario_agreement: str | None = None

    unidad_albaran: str | None = None
    unidad_contrato: str | None = None
    unidad_categoria: str | None = None
    unidad_category_match: bool | None = None

    cantidad_albaran: float | None = None
    cantidad_convertida: float | None = None
    factor_conversion: float | None = None

    importe_calculado: float | None = None
    importe_albaran_declarado: float | None = None
    importe_source: str | None = None

    codigo_partida_albaran: str | None = None
    codigo_partida_final: str | None = None
    partida_action: str | None = None

    match_confidence_pct: float | None = None
    match_method: str | None = None
    review_required: bool | None = None

    # ---- sub-tanda 2D: identificación de línea sintética ----
    # Opcionales por compatibilidad con valoraciones anteriores a 2D.
    line_kind: str = "from_albaran"
    parent_merge_line_id: int | None = None
    modifier_source: str | None = None
    modifier_reason: str | None = None
    descripcion_linea: str | None = None


class ValuationPayload(BaseModel):
    """Cabecera + mapa ``merge_line_id -> LineValuationPayload``."""

    valuation_id: str
    contrato_codigo: str | None = None
    status: str
    provider_ia: str | None = None
    model_name: str | None = None
    total_valorado: float = 0.0
    total_lines: int = 0
    lines_matched_exact: int = 0
    lines_matched_semantic: int = 0
    lines_matched_price_only: int = 0
    lines_unmatched: int = 0
    review_required: bool = False
    created_at_utc: str | None = None
    updated_at_utc: str | None = None
    lines_by_merge_line_id: dict[int, LineValuationPayload] = Field(
        default_factory=dict,
    )
    # ---- sub-tanda 2D ----
    # Líneas sintéticas (line_kind='synthetic_modifier'). No están en
    # lines_by_merge_line_id porque no tienen merge_line_id. El template
    # las itera aparte o las mezcla en display_lines.
    synthetic_lines: list[LineValuationPayload] = Field(default_factory=list)


# ====================================================================== #
# Sub-tanda 2D — DisplayLine
#
# DTO uniforme para la tabla de líneas del detalle. Mezcla las líneas
# del merge (from_albaran) con las sintéticas de valoración, en el
# orden correcto para la UI (cada sintética justo después de su base).
#
# El template itera display_lines y trata todas las filas por igual.
# El campo ``line_kind`` y los ids internos (``merge_line_id`` para
# from_albaran, ``valuation_line_id`` para sintéticas) permiten al JS
# del front saber a qué endpoint enviar las ediciones al guardar.
# ====================================================================== #

class DisplayLine(BaseModel):
    """Una fila visible en la tabla de líneas del detalle."""

    # Identificación
    line_kind: str  # 'from_albaran' | 'synthetic_modifier'
    merge_line_id: int | None = None       # solo si from_albaran
    valuation_line_id: int | None = None   # solo si synthetic_modifier
    line_index: int | None = None          # orden visual (para base y
                                            # para la sintética se usa
                                            # el line_index de la base +
                                            # orden de modificador)

    # Datos a mostrar / editar
    codigo_imputacion: str | None = None
    concepto: str | None = None
    cantidad: float | None = None
    unidad: str | None = None
    precio_unitario: float | None = None
    importe: float | None = None
    descuento: float | None = None
    codigo: str | None = None
    confianza_pct: float | None = None

    # Sub-campos que el template solo usa en modo traza (no de entrada).
    is_valued: bool = False                # True si hay valoración ligada
    parent_merge_line_id: int | None = None  # sintéticas: a qué base cuelgan


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
    # Sub-tanda 2D — lista unificada para la UI: mezcla líneas del
    # merge (from_albaran) con las sintéticas de valoración en orden.
    # El template la usa en lugar de ``lines`` para pintar la tabla
    # en el detalle. ``lines`` se mantiene por compatibilidad (API).
    display_lines: list[DisplayLine] = Field(default_factory=list)
    provider_snapshots: list[ProviderSnapshot] = Field(default_factory=list)
    contratos: list[ContratoPayload] = Field(default_factory=list)
    selected_contrato_codigo: str | None = None
    # NUEVO — None mientras no exista valoración en BBDD.
    valuation: ValuationPayload | None = None


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
    return cleaned or VIEW_MODE_MERGE

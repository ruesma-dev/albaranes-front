# infrastructure/database/orm_models.py
from __future__ import annotations

from sqlalchemy import Boolean, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class AlbaranDocumentMergeOrm(Base):
    __tablename__ = "albaran_documents_merge"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    provider_origin: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_document_id: Mapped[str | None] = mapped_column(String(64), index=True)
    document_storage_ref: Mapped[str | None] = mapped_column(String(1024))
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    source_mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    prompt_key: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(100), nullable=False)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)

    proveedor_nombre: Mapped[str | None] = mapped_column(String(255))
    proveedor_cif: Mapped[str | None] = mapped_column(String(64))
    fecha: Mapped[str | None] = mapped_column(String(32))
    numero_albaran: Mapped[str | None] = mapped_column(String(128))
    forma_pago: Mapped[str | None] = mapped_column(String(128))
    obra_codigo: Mapped[str | None] = mapped_column(String(128))
    obra_nombre: Mapped[str | None] = mapped_column(String(255))
    obra_direccion: Mapped[str | None] = mapped_column(String(255))

    # Código del contrato elegido para este albarán (ref. soft, no FK).
    # NULL = sin elegir (0 contratos encontrados, o >1 sin elegir aún).
    # Se auto-setea al único contrato si ``albaran_contratos_merge`` trae
    # exactamente 1 fila para (cif, obra).
    selected_contrato_codigo: Mapped[str | None] = mapped_column(String(64))

    sharepoint_relative_path: Mapped[str | None] = mapped_column(String(1024))
    sharepoint_web_url: Mapped[str | None] = mapped_column(String(1024))
    sharepoint_share_url: Mapped[str | None] = mapped_column(String(1024))

    raw_extraction_json: Mapped[str] = mapped_column(Text, nullable=False)
    confidence_pct_calc: Mapped[float | None] = mapped_column(Float)
    review_required: Mapped[bool | None] = mapped_column(Boolean)
    review_reasons_json: Mapped[str | None] = mapped_column(Text)
    comparison_summary_json: Mapped[str | None] = mapped_column(Text)

    approved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    approved_at_utc: Mapped[str | None] = mapped_column(String(64))
    approved_by: Mapped[str | None] = mapped_column(String(255))
    reviewed_at_utc: Mapped[str | None] = mapped_column(String(64))
    last_modified_at_utc: Mapped[str | None] = mapped_column(String(64))
    review_notes: Mapped[str | None] = mapped_column(Text)

    created_at_utc: Mapped[str] = mapped_column(String(64), nullable=False)

    lines: Mapped[list["AlbaranLineMergeOrm"]] = relationship(
        back_populates="document",
        cascade="all, delete-orphan",
        order_by="AlbaranLineMergeOrm.line_index",
    )

    # Contratos enriquecidos por el servicio 3 (read-only desde este
    # servicio — el servicio 4 NO los escribe, solo los lee). El pipeline
    # de enrichment del servicio 3 es quien los inserta/actualiza.
    contratos: Mapped[list["AlbaranContratoMergeOrm"]] = relationship(
        back_populates="document",
        order_by="AlbaranContratoMergeOrm.codigo_contrato",
        viewonly=True,
    )


class AlbaranLineMergeOrm(Base):
    __tablename__ = "albaran_lines_merge"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("albaran_documents_merge.id"),
        nullable=False,
        index=True,
    )
    provider_origin: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    line_index: Mapped[int] = mapped_column(Integer, nullable=False)
    external_line_id: Mapped[str | None] = mapped_column(String(64))
    cabecera_id: Mapped[str | None] = mapped_column(String(64))
    codigo: Mapped[str | None] = mapped_column(String(64))
    cantidad: Mapped[float | None] = mapped_column(Float)
    concepto: Mapped[str | None] = mapped_column(Text)
    precio: Mapped[float | None] = mapped_column(Float)
    descuento: Mapped[float | None] = mapped_column(Float)
    precio_neto: Mapped[float | None] = mapped_column(Float)
    codigo_imputacion: Mapped[str | None] = mapped_column(String(128))
    confianza_pct: Mapped[float | None] = mapped_column(Float)
    confidence_pct_calc: Mapped[float | None] = mapped_column(Float)
    line_match_score: Mapped[float | None] = mapped_column(Float)
    comparison_status_json: Mapped[str | None] = mapped_column(Text)
    field_scores_json: Mapped[str | None] = mapped_column(Text)

    document: Mapped[AlbaranDocumentMergeOrm] = relationship(back_populates="lines")


class AlbaranContratoMergeOrm(Base):
    """Contratos (proveedor × obra) asociados al merge doc.

    Tabla creada por el servicio 3 (``scripts/diagnose_sigrid_contrato.py``
    valida la query que la puebla). Relación 1:N con el merge doc. Mapeo
    read-only desde aquí; si la tabla no existe todavía en BBDD, falla
    solo al consultarla — no al construir el engine.
    """

    __tablename__ = "albaran_contratos_merge"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("albaran_documents_merge.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    codigo_contrato: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    nombre_contrato: Mapped[str | None] = mapped_column(Text)
    fecha_alta_contrato: Mapped[int | None] = mapped_column(Integer)
    fecha_contrato: Mapped[int | None] = mapped_column(Integer)
    vigencia_desde: Mapped[int | None] = mapped_column(Integer)
    vigencia_hasta: Mapped[int | None] = mapped_column(Integer)
    importe_total: Mapped[float | None] = mapped_column(Float)
    cif_proveedor: Mapped[str | None] = mapped_column(String(32))
    nombre_proveedor: Mapped[str | None] = mapped_column(String(255))
    codigo_obra: Mapped[str | None] = mapped_column(String(32))
    nombre_obra: Mapped[str | None] = mapped_column(String(255))
    fetched_at_utc: Mapped[str] = mapped_column(String(64), nullable=False)

    document: Mapped[AlbaranDocumentMergeOrm] = relationship(back_populates="contratos")


class AlbaranDocumentBaseOrm(Base):
    __tablename__ = "albaran_documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    provider_origin: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_document_id: Mapped[str | None] = mapped_column(String(64), index=True)
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    proveedor_nombre: Mapped[str | None] = mapped_column(String(255))
    fecha: Mapped[str | None] = mapped_column(String(32))
    numero_albaran: Mapped[str | None] = mapped_column(String(128))
    obra_codigo: Mapped[str | None] = mapped_column(String(128))
    raw_extraction_json: Mapped[str] = mapped_column(Text, nullable=False)
    ia_output_json: Mapped[str | None] = mapped_column(Text)
    created_at_utc: Mapped[str] = mapped_column(String(64), nullable=False)

    lines: Mapped[list["AlbaranLineBaseOrm"]] = relationship(
        order_by="AlbaranLineBaseOrm.line_index",
        viewonly=True,
    )


class AlbaranLineBaseOrm(Base):
    __tablename__ = "albaran_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("albaran_documents.id"),
        nullable=False,
        index=True,
    )
    provider_origin: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    line_index: Mapped[int] = mapped_column(Integer, nullable=False)
    external_line_id: Mapped[str | None] = mapped_column(String(64))
    cabecera_id: Mapped[str | None] = mapped_column(String(64))
    codigo: Mapped[str | None] = mapped_column(String(64))
    cantidad: Mapped[float | None] = mapped_column(Float)
    concepto: Mapped[str | None] = mapped_column(Text)
    precio: Mapped[float | None] = mapped_column(Float)
    descuento: Mapped[float | None] = mapped_column(Float)
    precio_neto: Mapped[float | None] = mapped_column(Float)
    codigo_imputacion: Mapped[str | None] = mapped_column(String(128))
    confianza_pct: Mapped[float | None] = mapped_column(Float)

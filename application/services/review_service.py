# application/services/review_service.py
from __future__ import annotations

from domain.models.review_models import (
    DocumentDetailPayload,
    DocumentListFilters,
    MergeDocumentUpdatePayload,
    PaginatedDocuments,
    VIEW_MODE_MERGE,
)
from infrastructure.database.review_repository import AlbaranReviewRepository


class ReviewService:
    def __init__(self, repository: AlbaranReviewRepository, default_reviewer: str | None) -> None:
        self._repository = repository
        self._default_reviewer = (default_reviewer or "").strip() or None

    def initialize(self) -> bool:
        return self._repository.initialize()

    def list_documents(self, filters: DocumentListFilters) -> PaginatedDocuments:
        return self._repository.list_documents(filters)

    def get_document(
        self,
        document_id: str,
        *,
        view_mode: str = VIEW_MODE_MERGE,
    ) -> DocumentDetailPayload | None:
        return self._repository.get_document_detail(
            document_id,
            view_mode=view_mode,
        )

    def get_contrato_pdf_url(self, document_id: str) -> str | None:
        """URL del PDF del contrato en SharePoint para abrir desde el front.
        Resuelta por consulta directa en el repositorio (robusta)."""
        return self._repository.get_contrato_pdf_url(document_id)

    def save_document(
        self,
        *,
        document_id: str,
        payload: MergeDocumentUpdatePayload,
    ) -> DocumentDetailPayload:
        if payload.approved and not payload.approved_by:
            payload.approved_by = self._default_reviewer
        return self._repository.update_document(document_id=document_id, payload=payload)

    def approve_document(self, *, document_id: str, approved_by: str | None) -> None:
        self._repository.set_approved(
            document_id=document_id,
            approved=True,
            approved_by=approved_by or self._default_reviewer,
        )

    def delete_document(
        self, *, document_id: str, deleted_by: str | None
    ) -> None:
        """Soft-delete: mueve el albarán a la papelera (is_active=False)."""
        self._repository.soft_delete_document(
            document_id=document_id,
            deleted_by=deleted_by or self._default_reviewer,
        )

    def restore_document(self, *, document_id: str) -> None:
        """Restaura un albarán desde la papelera (is_active=True)."""
        self._repository.restore_document(document_id=document_id)

    def hard_delete_document(self, *, document_id: str) -> str | None:
        """Purga DEFINITIVA: borra físicamente el albarán y todo lo que
        cuelga de él (líneas, valoración, contratos asociados, cruda).
        Irreversible. Pensado para usarse solo desde la papelera.

        Devuelve el ``source_sha256`` del documento purgado (o None) —
        el endpoint del portal lo reenvía a sv7 en el evento
        ``document-purged`` para desbloquear el dedup por contenido."""
        return self._repository.hard_delete_document(document_id=document_id)

    def update_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
        codigo_partida: str | None = None,
        descripcion: str | None = None,
        cantidad: float | None = None,
        unidad: str | None = None,
        precio_unitario: float | None = None,
        descuento: float | None = None,
        codigo_externo: str | None = None,
    ) -> bool:
        """Edición de la fila salmon (ver repositorio para la regla
        SIGRID→NUEVA al cambiar imputación/unidad/precio)."""
        return self._repository.update_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            codigo_partida=codigo_partida,
            descripcion=descripcion,
            cantidad=cantidad,
            unidad=unidad,
            precio_unitario=precio_unitario,
            descuento=descuento,
            codigo_externo=codigo_externo,
        )

    def remove_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
    ) -> bool:
        return self._repository.remove_line_valuation(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
        )

    def set_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
        mode: str,
        matched_contrato_line_id: int | None = None,
        descripcion: str | None = None,
        precio_unitario: float | None = None,
    ) -> bool:
        return self._repository.set_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            mode=mode,
            matched_contrato_line_id=matched_contrato_line_id,
            descripcion=descripcion,
            precio_unitario=precio_unitario,
        )

    def add_conciliacion_for_merge_line(
        self,
        *,
        document_id: str,
        merge_line_id: int,
        mode: str = "contract_line",
        matched_contrato_line_id: int | None = None,
        precio_unitario: float | None = None,
        descripcion: str | None = None,
    ) -> bool:
        return self._repository.add_conciliacion_for_merge_line(
            document_id=document_id,
            merge_line_id=merge_line_id,
            mode=mode,
            matched_contrato_line_id=matched_contrato_line_id,
            precio_unitario=precio_unitario,
            descripcion=descripcion,
        )

    def add_standalone_valuation_line(
        self,
        *,
        document_id: str,
    ) -> int | None:
        return self._repository.add_standalone_valuation_line(
            document_id=document_id,
        )

    def unapprove_document(self, *, document_id: str) -> None:
        self._repository.set_approved(
            document_id=document_id,
            approved=False,
            approved_by=None,
        )

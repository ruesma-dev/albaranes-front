# application/services/review_service.py
from __future__ import annotations

from domain.models.review_models import (
    DocumentDetailPayload,
    DocumentListFilters,
    MergeDocumentUpdatePayload,
    PaginatedDocuments,
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

    def get_document(self, document_id: str) -> DocumentDetailPayload | None:
        return self._repository.get_document_detail(document_id)

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

    def unapprove_document(self, *, document_id: str) -> None:
        self._repository.set_approved(
            document_id=document_id,
            approved=False,
            approved_by=None,
        )

# domain/ports/orchestrator_port.py — NUEVO sv4
"""Puerto de salida hacia sv7 (orchestrator-api).

sv4 emite dos tipos de evento al orquestador:
  - contract-selected: el revisor cambió/eligió el contrato.
  - document-approved: el revisor aprobó el documento.

Best-effort: si la llamada falla, sv4 loguea pero NO rompe el save.
La razón es que el revisor ya ha guardado en BBDD y la operación es
idempotente desde sv7 (correlation_key + estado del documento).
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class OrchestratorClient(ABC):
    @abstractmethod
    def notify_contract_selected(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        selected_by: str | None,
        selected_at_utc: str,
    ) -> None:
        """POST /v1/events/contract-selected. Idempotente en sv7.

        Best-effort: nunca lanza excepción al caller; loguea si falla.
        """
        raise NotImplementedError

    @abstractmethod
    def notify_document_approved(
        self,
        *,
        document_id: str,
        approved_by: str | None,
        approved_at_utc: str,
        review_notes: str | None,
    ) -> None:
        """POST /v1/events/document-approved. Idempotente en sv7."""
        raise NotImplementedError

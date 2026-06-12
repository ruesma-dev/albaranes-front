# domain/ports/orchestrator_port.py — NUEVO sv4
"""Puerto de salida hacia sv7 (orchestrator-api).

sv4 emite tres tipos de evento al orquestador:
  - contract-selected: el revisor cambió/eligió el contrato.
  - document-approved: el revisor aprobó el documento.
  - document-purged:   el revisor PURGÓ (hard-delete) el documento
    (jun 2026). Sin este evento, los workflow_runs del sv7 seguían
    vivos y el dedup por attachment_sha256 bloqueaba el reprocesado
    del mismo PDF con "ya procesado".

Best-effort: si la llamada falla, sv4 loguea pero NO rompe el save ni
la purga. La razón es que el revisor ya ha persistido en BBDD y la
operación es idempotente desde sv7 (correlation_key + estado del
documento).

jun 2026 — honestidad del botón "Valorar ahora": los métodos devuelven
ahora la respuesta parseada de sv7 (``dict``) o ``None`` si no se pudo
contactar / sv7 devolvió error. El endpoint /valuate del portal usa
ese retorno para NO decirle al revisor "valoración encolada" cuando en
realidad el orquestador no la aceptó (bug reportado: "dice que lanza
valorar, pero no lo hace").
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class OrchestratorClient(ABC):
    @abstractmethod
    def notify_contract_selected(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        selected_by: str | None,
        selected_at_utc: str,
    ) -> dict[str, Any] | None:
        """POST /v1/events/contract-selected. Idempotente en sv7.

        Best-effort: nunca lanza excepción al caller; loguea si falla.

        Returns
        -------
        dict | None
            El ``EventApplyResult`` de sv7 (claves: workflow_id,
            previous_state, new_state, action, detail) si el POST
            terminó en 2xx; ``None`` en cualquier otro caso (red caída,
            4xx/5xx, JSON inválido).
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
    ) -> dict[str, Any] | None:
        """POST /v1/events/document-approved. Idempotente en sv7."""
        raise NotImplementedError

    @abstractmethod
    def notify_document_purged(
        self,
        *,
        document_id: str,
        source_sha256: str | None,
        purged_by: str | None,
        purged_at_utc: str,
    ) -> dict[str, Any] | None:
        """POST /v1/events/document-purged. Idempotente en sv7.

        Marca los workflows del documento como ``purged`` para que el
        dedup por contenido (attachment_sha256) deje de bloquear el
        reprocesado del mismo PDF tras un hard-delete.
        """
        raise NotImplementedError

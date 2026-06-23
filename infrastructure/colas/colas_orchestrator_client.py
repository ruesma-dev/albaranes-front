# infrastructure/colas/colas_orchestrator_client.py
"""Adapter de COLAS del puerto :class:`OrchestratorClient`.

Sustituye al ``HttpOrchestratorClient`` (HTTP → sv7, disuelto). Mismos
métodos, misma filosofía best-effort: el dato fundamental ya está en
BBDD antes de notificar; la publicación/limpieza es solo el disparador.

Mapeo de eventos:
  - contract-selected → ``MensajeValoracion(force=True)`` → q-valoracion.
  - document-approved → ``MensajeFeedback`` → q-feedback.
  - document-purged   → limpieza directa de ``workflow_runs`` (BBDD), que
    desbloquea el dedup por contenido tras un hard-delete.

Honestidad del botón "Valorar ahora"
-------------------------------------
El endpoint ``/valuate`` del portal interpreta el ``dict`` devuelto: si
es ``None`` avisa "no se pudo encolar"; si ``action == "no_op"`` avisa
"nada que valorar"; en otro caso "valoración encolada". Para conservar
ese contrato sin sv7, devolvemos un dict equivalente al ``EventApplyResult``:
  - publicado OK → ``{"action": "queued", ...}``
  - no publicado → ``None``
"""
from __future__ import annotations

import logging
from typing import Any

from ruesma_comun.colas import COLA_FEEDBACK, COLA_VALORACION
from ruesma_comun.colas.mensajes import MensajeFeedback, MensajeValoracion
from ruesma_comun.colas.publicador import PublicadorBestEffort

from domain.ports.orchestrator_port import OrchestratorClient
from infrastructure.database.workflow_runs_purger import WorkflowRunsPurger

logger = logging.getLogger(__name__)


class ColasOrchestratorClient(OrchestratorClient):
    def __init__(
        self,
        *,
        publicador: PublicadorBestEffort,
        workflow_runs_purger: WorkflowRunsPurger | None = None,
        cola_valoracion: str = COLA_VALORACION,
        cola_feedback: str = COLA_FEEDBACK,
    ) -> None:
        self._pub = publicador
        self._purger = workflow_runs_purger
        self._cola_valoracion = cola_valoracion
        self._cola_feedback = cola_feedback

    # --------------------------------------------------------------- #
    # contract-selected → q-valoracion (force=True)
    # --------------------------------------------------------------- #
    def notify_contract_selected(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        selected_by: str | None,
        selected_at_utc: str,
    ) -> dict[str, Any] | None:
        mensaje = MensajeValoracion(
            document_id=document_id,
            codigo_contrato=codigo_contrato,
            force=True,
        )
        publicado = self._pub.publicar(self._cola_valoracion, mensaje)
        if not publicado:
            return None
        return {
            "action": "queued",
            "detail": (
                f"publicado en {self._cola_valoracion} "
                f"(contrato={codigo_contrato}, force=True)"
            ),
            "previous_state": None,
            "new_state": "valuing",
            "workflow_id": None,
        }

    # --------------------------------------------------------------- #
    # document-approved → q-feedback
    # --------------------------------------------------------------- #
    def notify_document_approved(
        self,
        *,
        document_id: str,
        approved_by: str | None,
        approved_at_utc: str,
        review_notes: str | None,
    ) -> dict[str, Any] | None:
        mensaje = MensajeFeedback(
            document_id=document_id,
            approved_by=approved_by,
        )
        publicado = self._pub.publicar(self._cola_feedback, mensaje)
        if not publicado:
            return None
        return {
            "action": "queued",
            "detail": f"publicado en {self._cola_feedback}",
            "previous_state": None,
            "new_state": "approved",
            "workflow_id": None,
        }

    # --------------------------------------------------------------- #
    # document-purged → limpieza directa de workflow_runs (BBDD)
    # --------------------------------------------------------------- #
    def notify_document_purged(
        self,
        *,
        document_id: str,
        source_sha256: str | None,
        purged_by: str | None,
        purged_at_utc: str,
    ) -> dict[str, Any] | None:
        if self._purger is None:
            logger.warning(
                "[sv4-colas][purge] sin purger wire-ado; no se limpia "
                "workflow_runs doc=%s",
                document_id,
            )
            return None
        try:
            borradas = self._purger.purge(
                document_id=document_id,
                source_sha256=source_sha256,
            )
        except Exception:  # noqa: BLE001 — best-effort explícito
            logger.exception(
                "[sv4-colas][purge] fallo limpiando workflow_runs doc=%s "
                "(el hard-delete local ya se aplicó)",
                document_id,
            )
            return None
        return {
            "action": "purged",
            "detail": f"workflow_runs eliminados={borradas}",
            "previous_state": None,
            "new_state": "purged",
            "workflow_id": None,
        }

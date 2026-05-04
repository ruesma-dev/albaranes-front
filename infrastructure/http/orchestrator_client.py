# infrastructure/http/orchestrator_client.py — NUEVO sv4
"""Adapter HTTP del puerto OrchestratorClient → sv7.

Best-effort: las llamadas a sv7 NUNCA lanzan excepción al caller.
Si el orquestador está caído, sv4 sigue funcionando (el revisor puede
guardar y aprobar; la valoración se reanudará cuando sv7 vuelva).

Razón: el dato fundamental (selected_contrato_codigo, approved=true)
ya está persistido por sv4 en albaran_documents_merge antes de
notificar. El evento es solo el "trigger" para que sv7 actúe; si se
pierde, hay un mecanismo de recuperación: sv7 puede tener un job que
busque documentos aprobados sin workflow asociado y los procese.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from domain.ports.orchestrator_port import OrchestratorClient

logger = logging.getLogger(__name__)


class HttpOrchestratorClient(OrchestratorClient):
    def __init__(
        self,
        *,
        base_url: str,
        path_contract_selected: str,
        path_document_approved: str,
        timeout_s: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._path_contract = path_contract_selected
        self._path_approved = path_document_approved
        self._timeout_s = timeout_s

    def notify_contract_selected(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        selected_by: str | None,
        selected_at_utc: str,
    ) -> None:
        body = {
            "document_id": document_id,
            "codigo_contrato": codigo_contrato,
            "selected_by": selected_by,
            "selected_at_utc": selected_at_utc,
        }
        self._post_best_effort(self._path_contract, body)

    def notify_document_approved(
        self,
        *,
        document_id: str,
        approved_by: str | None,
        approved_at_utc: str,
        review_notes: str | None,
    ) -> None:
        body = {
            "document_id": document_id,
            "approved_by": approved_by,
            "approved_at_utc": approved_at_utc,
            "review_notes": review_notes,
        }
        self._post_best_effort(self._path_approved, body)

    def _post_best_effort(self, path: str, body: dict[str, Any]) -> None:
        url = f"{self._base_url}{path}"
        try:
            with httpx.Client(timeout=self._timeout_s) as client:
                response = client.post(url, json=body)
            if response.status_code >= 400:
                logger.warning(
                    "[sv4→sv7] %s devolvió %s body=%s",
                    path,
                    response.status_code,
                    response.text[:300],
                )
            else:
                logger.info(
                    "[sv4→sv7] %s OK doc=%s",
                    path,
                    body.get("document_id"),
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            logger.warning(
                "[sv4→sv7] %s falló (best-effort): %s: %s",
                path,
                type(exc).__name__,
                exc,
            )
        except Exception:
            logger.exception("[sv4→sv7] %s error inesperado", path)

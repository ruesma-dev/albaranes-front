# infrastructure/http/orchestrator_client.py — NUEVO sv4
"""Adapter HTTP del puerto OrchestratorClient → sv7.

Best-effort: las llamadas a sv7 NUNCA lanzan excepción al caller.
Si el orquestador está caído, sv4 sigue funcionando (el revisor puede
guardar y aprobar; la valoración se reanudará cuando sv7 vuelva).

Razón: el dato fundamental (selected_contrato_codigo, approved=true,
o el propio hard-delete) ya está persistido por sv4 en BBDD antes de
notificar. El evento es solo el "trigger" para que sv7 actúe.

jun 2026 — los métodos devuelven la respuesta JSON de sv7 (dict) en
2xx, o ``None`` si no se pudo contactar / hubo error. El endpoint
/valuate del portal usa ese retorno para informar con honestidad al
revisor (antes siempre decía "valoración encolada" aunque sv7 hubiera
respondido action='no_op' o estuviera caído).
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
        path_document_purged: str = "/v1/events/document-purged",
        timeout_s: float = 5.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._path_contract = path_contract_selected
        self._path_approved = path_document_approved
        self._path_purged = path_document_purged
        self._timeout_s = timeout_s

    def notify_contract_selected(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        selected_by: str | None,
        selected_at_utc: str,
    ) -> dict[str, Any] | None:
        body = {
            "document_id": document_id,
            "codigo_contrato": codigo_contrato,
            "selected_by": selected_by,
            "selected_at_utc": selected_at_utc,
        }
        return self._post_best_effort(self._path_contract, body)

    def notify_document_approved(
        self,
        *,
        document_id: str,
        approved_by: str | None,
        approved_at_utc: str,
        review_notes: str | None,
    ) -> dict[str, Any] | None:
        body = {
            "document_id": document_id,
            "approved_by": approved_by,
            "approved_at_utc": approved_at_utc,
            "review_notes": review_notes,
        }
        return self._post_best_effort(self._path_approved, body)

    def notify_document_purged(
        self,
        *,
        document_id: str,
        source_sha256: str | None,
        purged_by: str | None,
        purged_at_utc: str,
    ) -> dict[str, Any] | None:
        body = {
            "document_id": document_id,
            "source_sha256": source_sha256,
            "purged_by": purged_by,
            "purged_at_utc": purged_at_utc,
        }
        return self._post_best_effort(self._path_purged, body)

    def _post_best_effort(
        self,
        path: str,
        body: dict[str, Any],
    ) -> dict[str, Any] | None:
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
                return None
            logger.info(
                "[sv4→sv7] %s OK doc=%s",
                path,
                body.get("document_id"),
            )
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001 — respuesta sin JSON
                return {}
            return payload if isinstance(payload, dict) else {}
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            logger.warning(
                "[sv4→sv7] %s falló (best-effort): %s: %s",
                path,
                type(exc).__name__,
                exc,
            )
            return None
        except Exception:
            logger.exception("[sv4→sv7] %s error inesperado", path)
            return None

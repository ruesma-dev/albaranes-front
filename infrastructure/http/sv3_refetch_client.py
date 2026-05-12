# infrastructure/http/sv3_refetch_client.py
from __future__ import annotations

import logging

import httpx

from domain.models.contrato_refetch_models import ContratoRefetchOutcome

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[sv3-refetch-client]"


class Sv3RefetchClient:
    """Cliente HTTP del sv4 contra el endpoint
    ``POST /v1/albaranes/{id}/re-fetch-contratos`` del sv3.

    Reemplaza el antiguo ``ContratoRefetchService`` del sv4 que
    llamaba directamente a Sigrid. La motivación del cambio está
    documentada en :mod:`config.settings` (refactor "el sv3 es dueño
    de los contratos", mayo 2026).

    Comportamiento de errores:
      * Si el sv3 devuelve 200 → parsea el JSON y construye el outcome.
      * Si el sv3 devuelve 404 → levanta ``KeyError`` (documento no
        existe; el endpoint del sv4 lo traduce a 404 al front).
      * Si el sv3 devuelve 503 → outcome con
        ``status="sigrid_error"`` y mensaje explicativo (al sv3 le
        falta cablear Sigrid). NO levanta excepción: queremos que el
        front pinte el mensaje, no que el portal cuelgue.
      * Si la conexión al sv3 falla (red, timeout, 5xx) → outcome
        con ``status="sigrid_error"`` y mensaje útil. NO levanta.

    Diseño síncrono porque el endpoint del portal es síncrono y el
    usuario espera ver el resultado del refetch en la misma petición.
    """

    def __init__(
        self,
        *,
        base_url: str,
        path: str,
        timeout_s: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        # ``path`` puede contener el placeholder {document_id}. Si no
        # lo trae, lo añadimos para tolerancia.
        if "{document_id}" not in path:
            path = path.rstrip("/") + "/{document_id}/re-fetch-contratos"
        self._path = path
        self._timeout_s = timeout_s

    def refetch(
        self, *, document_id: str,
    ) -> ContratoRefetchOutcome:
        url = self._base_url + self._path.format(document_id=document_id)
        logger.info("%s POST %s", _LOG_PREFIX, url)

        try:
            with httpx.Client(timeout=self._timeout_s) as client:
                response = client.post(url)
        except httpx.RequestError as exc:
            # Red caída, DNS, timeout en conexión, etc. NO propagamos:
            # queremos que el front pinte un mensaje claro.
            logger.warning(
                "%s no se pudo contactar con sv3: %s",
                _LOG_PREFIX, exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se pudo contactar con el servicio de "
                    "persistencia (sv3). Inténtalo de nuevo más tarde."
                ),
                cif=None,
                obra_codigo=None,
            )

        if response.status_code == 404:
            # Documento no existe en sv3 — propagamos para que el
            # endpoint del sv4 devuelva 404 al front.
            try:
                detail = response.json().get("detail")
            except Exception:
                detail = response.text
            raise KeyError(detail or f"Documento {document_id} no encontrado")

        if response.status_code == 503:
            # sv3 no tiene Sigrid cableado — outcome con status
            # informativo en lugar de 503 al front.
            logger.error(
                "%s sv3 devolvió 503: Sigrid no cableado en sv3.",
                _LOG_PREFIX,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "El servicio de persistencia (sv3) no tiene "
                    "configurado el acceso a Sigrid. Avisa al "
                    "administrador para que revise SIGRID_API_* en el "
                    ".env del sv3."
                ),
                cif=None,
                obra_codigo=None,
            )

        if response.status_code >= 500:
            logger.error(
                "%s sv3 devolvió %s: %s",
                _LOG_PREFIX,
                response.status_code,
                response.text[:200],
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    f"Error interno en el servicio de persistencia "
                    f"(sv3 status={response.status_code}). Inténtalo "
                    "de nuevo más tarde."
                ),
                cif=None,
                obra_codigo=None,
            )

        if response.status_code != 200:
            logger.error(
                "%s sv3 respondió con status inesperado %s: %s",
                _LOG_PREFIX,
                response.status_code,
                response.text[:200],
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    f"Respuesta inesperada del sv3 "
                    f"(status={response.status_code})."
                ),
                cif=None,
                obra_codigo=None,
            )

        try:
            data = response.json()
        except Exception as exc:
            logger.exception(
                "%s sv3 devolvió payload no-JSON: %s", _LOG_PREFIX, exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message="Respuesta no procesable del sv3.",
                cif=None,
                obra_codigo=None,
            )

        outcome = ContratoRefetchOutcome.from_dict(data)
        logger.info(
            "%s OK document_id=%s status=%s count=%s selected=%s",
            _LOG_PREFIX,
            document_id,
            outcome.status,
            outcome.count,
            outcome.selected_contrato_codigo,
        )
        return outcome

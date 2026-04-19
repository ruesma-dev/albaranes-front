# application/services/contrato_refetch_service.py
from __future__ import annotations

import logging

from application.services.obra_code_normalizer import normalize_obra_code
from domain.models.contrato_refetch_models import ContratoRefetchOutcome
from domain.ports.contrato_refetch_port import ContratoLookupClient
from infrastructure.database.review_repository import AlbaranReviewRepository

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[contrato-refetch]"


class ContratoRefetchService:
    """Orquesta la re-búsqueda manual de contratos desde el portal.

    Flujo (espejo del servicio 3 pero lanzado por el usuario):
      1. Lee (cif, obra_codigo) CUALES ESTÉN ACTUALMENTE EN EL MERGE
         (ojo: si el usuario acaba de guardar, son los valores nuevos).
      2. Normaliza obra_codigo con la MISMA regla que servicio 3.
      3. Si falta CIF o no valida obra → devuelve 'skipped_missing_data'
         (no llama a Sigrid — ahorra una llamada tonta).
      4. Llama a Sigrid.
      5. Reemplaza los contratos del documento (delete + insert).
      6. Si exactamente 1 → auto-selecciona. Si 0 o varios → NULL.
      7. Devuelve el outcome para que el endpoint dé feedback al front.

    A diferencia del servicio 3 (que NO propaga fallos — es best-effort
    durante la persistencia), aquí SÍ capturamos y devolvemos el estado
    'sigrid_error' para que el portal pueda mostrarlo al usuario.
    """

    def __init__(
        self,
        *,
        client: ContratoLookupClient,
        repository: AlbaranReviewRepository,
        enabled: bool = True,
    ) -> None:
        self._client = client
        self._repository = repository
        self._enabled = enabled
        logger.info(
            "%s ContratoRefetchService INSTANCIADO (enabled=%s)",
            _LOG_PREFIX,
            enabled,
        )

    def refetch(self, *, document_id: str) -> ContratoRefetchOutcome:
        logger.info("%s refetch() INVOCADO document_id=%s", _LOG_PREFIX, document_id)

        if not self._enabled:
            return ContratoRefetchOutcome(
                status="skipped_missing_data",
                count=0,
                selected_contrato_codigo=None,
                message="La búsqueda de contratos está deshabilitada en este servicio.",
                cif=None,
                obra_codigo=None,
            )

        cif, obra_raw = self._repository.get_merge_cif_and_obra(
            document_id=document_id,
        )
        cif_clean = (cif or "").strip().upper().replace(" ", "") or None
        obra_norm = normalize_obra_code(obra_raw)

        logger.info(
            "%s Leídos del merge: cif=%r obra_raw=%r → normalizados cif=%r obra=%r",
            _LOG_PREFIX,
            cif,
            obra_raw,
            cif_clean,
            obra_norm,
        )

        if not cif_clean or not obra_norm:
            message_parts = []
            if not cif_clean:
                message_parts.append("CIF del proveedor vacío")
            if not obra_norm:
                message_parts.append(
                    f"código de obra inválido ({obra_raw!r}); debe ser 1–4 dígitos"
                )
            return ContratoRefetchOutcome(
                status="skipped_missing_data",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se consultó el ERP: "
                    + " y ".join(message_parts)
                    + ". Corrige los datos del albarán y pulsa 'Guardar y volver a buscar'."
                ),
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        try:
            contratos = self._client.fetch_contratos(
                cif_proveedor=cif_clean,
                codigo_obra_normalizado=obra_norm,
            )
        except Exception as exc:
            logger.exception(
                "%s ERROR llamando a Sigrid. document_id=%s exc=%r",
                _LOG_PREFIX,
                document_id,
                exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se pudo contactar con el ERP para consultar contratos. "
                    "Inténtalo de nuevo en unos segundos. "
                    f"Detalle: {exc}"
                ),
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        logger.info("%s Sigrid devolvió %s contratos", _LOG_PREFIX, len(contratos))

        # Reemplazo atómico + auto-selección si hay uno solo
        selected = None
        if len(contratos) == 1:
            selected = contratos[0].codigo_contrato

        try:
            self._repository.replace_contratos_and_select(
                document_id=document_id,
                contratos=contratos,
                selected_codigo=selected,
            )
        except Exception as exc:
            logger.exception(
                "%s ERROR persistiendo contratos. document_id=%s exc=%r",
                _LOG_PREFIX,
                document_id,
                exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=len(contratos),
                selected_contrato_codigo=None,
                message=f"Error guardando los contratos en BBDD: {exc}",
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        if len(contratos) == 0:
            status = "no_results"
            message = (
                f"Se consultó el ERP con CIF {cif_clean} y obra {obra_norm}, "
                "pero no existe ningún contrato para esa combinación."
            )
        elif len(contratos) == 1:
            status = "found_single"
            message = (
                f"Se encontró 1 contrato y se ha asignado automáticamente "
                f"({contratos[0].nombre_contrato or contratos[0].codigo_contrato})."
            )
        else:
            status = "found_multiple"
            message = (
                f"Se encontraron {len(contratos)} contratos. "
                "Selecciona el que corresponda en el desplegable."
            )

        return ContratoRefetchOutcome(
            status=status,
            count=len(contratos),
            selected_contrato_codigo=selected,
            message=message,
            cif=cif_clean,
            obra_codigo=obra_norm,
        )

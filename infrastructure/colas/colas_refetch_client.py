# infrastructure/colas/colas_refetch_client.py
"""Adapter de COLAS del puerto :class:`ContratoRefetchClient`.

Sustituye al ``Sv3RefetchClient`` (HTTP síncrono → sv3). Cuando el
revisor edita CIF/obra y pulsa "Volver a buscar", en vez de llamar a sv3
y esperar el resultado, se re-publica ``MensajePersistencia(force=True)``
en q-persistencia. sv3 re-ejecuta su pipeline con ``force_refetch=True``
(bypasa la caché de contratos, re-consulta Sigrid, UPSERT por
``sigrid_ide``) y, si queda un contrato único, auto-dispara la valoración.

Cambio de semántica: SÍNCRONO → ASÍNCRONO
-----------------------------------------
Ya no hay un resultado inmediato (``found_single``/``found_multiple``/
``no_results``). El outcome devuelto lleva ``status="queued"``: el front
debe avisar "re-búsqueda encolada, refresca en unos segundos" en lugar de
reaccionar a un conteo. (Ajuste pendiente en el JS del portal.)

El puerto declara que ``refetch`` puede lanzar ``KeyError`` (doc no
existe) o ``RuntimeError`` (transporte). Aquí no aplican: la existencia
del documento ya la pre-valida el endpoint, y un fallo de publicación se
degrada a un outcome ``sigrid_error`` (best-effort) en vez de lanzar.
"""
from __future__ import annotations

import logging

from ruesma_comun.colas import COLA_PERSISTENCIA
from ruesma_comun.colas.mensajes import MensajePersistencia
from ruesma_comun.colas.publicador import PublicadorBestEffort

from domain.models.contrato_refetch_models import ContratoRefetchOutcome
from domain.ports.contrato_refetch_port import ContratoRefetchClient

logger = logging.getLogger(__name__)


class ColasRefetchClient(ContratoRefetchClient):
    def __init__(
        self,
        *,
        publicador: PublicadorBestEffort,
        cola_persistencia: str = COLA_PERSISTENCIA,
    ) -> None:
        self._pub = publicador
        self._cola = cola_persistencia

    def refetch(self, *, document_id: str) -> ContratoRefetchOutcome:
        mensaje = MensajePersistencia(document_id=document_id, force=True)
        publicado = self._pub.publicar(self._cola, mensaje)
        if not publicado:
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se pudo encolar la re-búsqueda de contratos. "
                    "Reintenta en unos segundos."
                ),
                cif=None,
                obra_codigo=None,
            )
        logger.info(
            "[sv4-colas][refetch] encolado document_id=%s → %s (force=True)",
            document_id,
            self._cola,
        )
        return ContratoRefetchOutcome(
            status="queued",
            count=0,
            selected_contrato_codigo=None,
            message=(
                "Re-búsqueda de contratos encolada. Refresca la página en "
                "unos segundos para ver el resultado."
            ),
            cif=None,
            obra_codigo=None,
        )

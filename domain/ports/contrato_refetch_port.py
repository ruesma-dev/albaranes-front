# domain/ports/contrato_refetch_port.py
from __future__ import annotations

from typing import Protocol

from domain.models.contrato_refetch_models import ContratoRefetchOutcome


class ContratoRefetchClient(Protocol):
    """Puerto del re-fetch de contratos desde el portal.

    Esta abstracción permite que el endpoint del sv4 no sepa si
    detrás hay una llamada HTTP al sv3 (caso real, post-refactor),
    un mock para tests, o una implementación que llame a Sigrid
    directamente (caso histórico, ya eliminado del wiring).

    Una sola operación: dado un ``document_id``, refrescar la lista
    de contratos asociados leyendo los datos actuales del merge
    (CIF + obra) y devolver el outcome.
    """

    def refetch(
        self, *, document_id: str,
    ) -> ContratoRefetchOutcome:
        """Re-busca contratos para el documento dado.

        Idempotente. Se asume que CIF y obra en el merge ya están
        actualizados (el portal guarda primero y refetch después).

        Raises
        ------
        KeyError
            Si el documento no existe en el merge (404 al front).
        RuntimeError
            Si el sv3 no está disponible u otro fallo de transporte
            no diagnosticable. NO se levanta para errores de Sigrid
            ni para "0 contratos encontrados" — eso va dentro del
            outcome (``status="sigrid_error"`` o ``"no_results"``).
        """
        ...

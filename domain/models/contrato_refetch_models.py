# domain/models/contrato_refetch_models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContratoRefetchOutcome:
    """Resultado de un re-fetch manual de contratos desde el portal.

    Siempre contiene la cuenta final de contratos y, si aplica, el código
    auto-seleccionado. El código ``status`` refleja qué sucedió para que
    el front pueda dar feedback al usuario:

      - ``skipped_missing_data``: faltan CIF/obra o no validan; no se
        llama a Sigrid.
      - ``no_results``: Sigrid respondió OK pero con 0 contratos.
      - ``found_single``: 1 contrato → auto-seleccionado.
      - ``found_multiple``: >1 contratos → usuario debe elegir.
      - ``sigrid_error``: fallo de red/5xx/etc. Se mantienen los
        contratos previos (si los había) sin tocar nada.

    ``message`` es texto corto pensado para pintarlo directamente.
    """

    status: str
    count: int
    selected_contrato_codigo: str | None
    message: str
    cif: str | None
    obra_codigo: str | None

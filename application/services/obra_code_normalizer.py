# application/services/obra_code_normalizer.py
from __future__ import annotations

import re

# Patrón: 4 dígitos exactos, puede venir con menos dígitos (se rellenan
# con 0 a la izquierda). Si viene con letras u otros caracteres, es
# inválido.
_OBRA_CODIGO_RE = re.compile(r"^\d{1,4}$")


def normalize_obra_code(value: str | None) -> str | None:
    """Normaliza un código de obra a 4 dígitos (zfill).

    Debe coincidir EXACTAMENTE con la función homónima del servicio 3
    para que re-ejecutar la búsqueda desde el portal dé los mismos
    resultados que la búsqueda automática al persistir.

    Ejemplos:
        '0695'   -> '0695'
        '695'    -> '0695'
        '12'     -> '0012'
        ''       -> None
        'abc'    -> None
        '12345'  -> None  (más de 4 dígitos, no esperado)
    """
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if not _OBRA_CODIGO_RE.match(cleaned):
        return None
    return cleaned.zfill(4)

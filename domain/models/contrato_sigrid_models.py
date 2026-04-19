# domain/models/contrato_sigrid_models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContratoFromSigrid:
    """Datos de un contrato tal cual devuelve la query de Sigrid.

    Mismo modelo que el servicio 3 usa para leer de Sigrid. Se duplica
    (misma forma, distinto módulo) porque servicio 4 es otro microservicio
    y no debe importar código del servicio 3.

    Las fechas vienen como INT YYYYMMDD (ej. 20241122); 0 significa
    "sin vigencia establecida". ``importe_total`` es float.
    """

    codigo_contrato: str
    nombre_contrato: str | None
    fecha_alta_contrato: int | None
    fecha_contrato: int | None
    vigencia_desde: int | None
    vigencia_hasta: int | None
    importe_total: float | None
    cif_proveedor: str | None
    nombre_proveedor: str | None
    codigo_obra: str | None
    nombre_obra: str | None

# domain/models/contrato_sigrid_models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContratoLineFromSigrid:
    """Línea de detalle (``ctrpro``) de un contrato del ERP.

    Réplica del mismo modelo en el servicio 3. Duplicación intencional:
    son dos microservicios distintos y compartir código exigiría una
    librería interna que aún no merece la pena.
    """

    codigo_contrato: str
    linea: int | None
    numero_linea: int | None
    codigo_producto: str | None
    codigo_alternativo: str | None
    unidad_medida: str | None
    descripcion_linea: str | None
    uds: float | None
    cantidad_servida: float | None
    cantidad_facturada: float | None
    pendiente_servir: float | None
    precio_unitario: float | None
    precio_bruto: float | None
    descuentos: float | None
    importe_linea: float | None
    cuota_iva: float | None
    doc_origen: str | None


@dataclass(frozen=True)
class ContratoFromSigrid:
    """Contrato devuelto por Sigrid (cabecera + líneas).

    El cliente HTTP llama a la query ampliada (JOIN a ``ctrpro``) que
    trae cabecera + líneas en un solo resultset, los agrupa en memoria
    por ``codigo_contrato`` (único dentro de (cif, obra)) y devuelve
    estos DTOs ya completos.

    ``lines`` es lista vacía si el contrato no tiene líneas registradas
    en el ERP (LEFT JOIN no casó).
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
    lines: list[ContratoLineFromSigrid] = field(default_factory=list)

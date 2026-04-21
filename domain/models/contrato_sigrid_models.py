# domain/models/contrato_sigrid_models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContratoLineFromSigrid:
    """Línea de detalle de un contrato del ERP. Réplica simétrica del
    servicio 3 (duplicación consciente entre microservicios).
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
    codigo_partida: str | None
    descripcion_partida: str | None


@dataclass(frozen=True)
class ContratoFromSigrid:
    """Contrato devuelto por Sigrid (cabecera + líneas + PDF ref).

    ``importe_total`` = ``ctr.totbas`` (SIN IVA).
    ``gra_rep_ide`` = id del PDF en ``ruesma_rep.gra``.
    ``pdf_sharepoint_*`` se rellenan tras la descarga+subida del PDF
    (o se inyectan desde el estado previo en caso de reutilización).
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
    gra_rep_ide: int | None
    pdf_sharepoint_relative_path: str | None = None
    pdf_sharepoint_web_url: str | None = None
    lines: list[ContratoLineFromSigrid] = field(default_factory=list)

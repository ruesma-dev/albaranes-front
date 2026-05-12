# domain/models/contrato_refetch_models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContratoRefetchOutcome:
    """Resultado de un re-fetch manual de contratos.

    Antes este DTO se construía en el sv4 (que llamaba a Sigrid
    directamente). Tras el refactor "el sv3 es dueño de los contratos",
    el sv4 recibe este outcome del sv3 vía HTTP y lo reenvía al front
    sin modificarlo.

    Mantener el DTO local (en lugar de importarlo del sv3) preserva la
    INDEPENDENCIA del sv4 como microservicio: el sv3 cambia su modelo
    interno → el sv4 sigue funcionando mientras el JSON del wire
    contract no rompa estos campos.

    Códigos de ``status``:

      * ``skipped_missing_data`` — Faltan CIF/obra o no validan.
      * ``no_results`` — Sigrid OK pero 0 contratos.
      * ``found_single`` — 1 contrato → auto-seleccionado.
      * ``found_multiple`` — >1 contratos → el usuario debe elegir.
      * ``sigrid_error`` — Error de red / 5xx / SQL.
    """

    status: str
    count: int
    selected_contrato_codigo: str | None
    message: str
    cif: str | None
    obra_codigo: str | None

    @classmethod
    def from_dict(cls, data: dict) -> "ContratoRefetchOutcome":
        """Construye el outcome a partir del JSON devuelto por el sv3.

        Tolerante con campos ausentes (vienen por HTTP — defensa frente
        a versiones desfasadas de sv3 con campos nuevos / antiguos).
        """
        return cls(
            status=str(data.get("status") or "sigrid_error"),
            count=int(data.get("count") or 0),
            selected_contrato_codigo=data.get("selected_contrato_codigo"),
            message=str(data.get("message") or ""),
            cif=data.get("cif"),
            obra_codigo=data.get("obra_codigo"),
        )

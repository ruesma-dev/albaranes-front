# domain/ports/contrato_refetch_port.py
from __future__ import annotations

from typing import Protocol

from domain.models.contrato_sigrid_models import ContratoFromSigrid


class ContratoLookupClient(Protocol):
    """Puerto para el cliente que consulta contratos en el ERP on-prem.

    Implementado por ``infrastructure.sigrid.sigrid_api_contrato_client``
    pero podría sustituirse por otro origen (CRM, API de terceros, mock
    en tests) sin tocar el servicio.
    """

    def fetch_contratos(
        self,
        *,
        cif_proveedor: str,
        codigo_obra_normalizado: str,
    ) -> list[ContratoFromSigrid]:
        """Devuelve la lista de contratos que casen, vacía si no hay."""
        ...

# domain/ports/contrato_refetch_port.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from domain.models.contrato_sigrid_models import ContratoFromSigrid


@dataclass(frozen=True)
class ContratoPdfPayload:
    """Binario + nombre original del PDF descargado de Sigrid."""

    filename: str
    content: bytes
    content_type: str | None


class ContratoLookupClient(Protocol):
    """Cliente que consulta contratos y descarga sus PDFs del ERP."""

    def fetch_contratos(
        self,
        *,
        cif_proveedor: str,
        codigo_obra_normalizado: str,
    ) -> list[ContratoFromSigrid]:
        ...

    def download_contrato_pdf(
        self,
        *,
        gra_rep_ide: int,
    ) -> ContratoPdfPayload | None:
        """Descarga el PDF por ``ruesma_rep.gra.ide``. None si no hay."""
        ...


@dataclass(frozen=True)
class StoredContratoPdf:
    relative_path: str
    web_url: str | None


class ContratoPdfStorage(Protocol):
    """Puerto para subir el PDF de contrato al storage (SharePoint)."""

    def upload_contrato_pdf(
        self,
        *,
        filename: str,
        file_bytes: bytes,
        codigo_contrato: str,
        gra_rep_ide: int,
    ) -> StoredContratoPdf:
        ...

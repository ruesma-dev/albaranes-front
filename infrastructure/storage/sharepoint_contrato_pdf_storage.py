# infrastructure/storage/sharepoint_contrato_pdf_storage.py
from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import quote

import httpx

from domain.ports.contrato_refetch_port import StoredContratoPdf
from infrastructure.graph.token_provider import GraphTokenProvider

logger = logging.getLogger(__name__)

SharePointMode = Literal["drive_id", "folder_url", "site_path"]

_LOG_PREFIX = "[contrato-pdf][sp]"


@dataclass(frozen=True)
class _ResolvedFolder:
    drive_id: str
    item_id: str
    folder_name: str | None


class SharePointContratoPdfStorage:
    """Storage ligero dedicado a subir PDFs de contrato.

    Réplica mínima y focalizada del ``SharePointDocumentStorage`` del
    servicio 3 — solo implementa lo necesario para ``upload_contrato_pdf``.
    Comparte la MISMA configuración de entorno (``SHAREPOINT_*``) para
    escribir en la misma biblioteca.

    Ruta destino: ``<base>/<YYYY>/<MM>/contratos/<codigo>_<ide>_<name>.pdf``
    """

    def __init__(
        self,
        *,
        graph_key: str,
        timeout_s: int,
        mode: SharePointMode,
        hostname: str | None,
        site_path: str | None,
        drive_name: str,
        drive_id: str | None,
        folder_root: str,
        folder_url: str | None,
    ) -> None:
        self._token_provider = GraphTokenProvider(graph_key, timeout_s)
        self._client = httpx.Client(timeout=timeout_s)
        self._base = "https://graph.microsoft.com/v1.0"
        self._mode: SharePointMode = mode
        self._hostname = (hostname or "").strip() or None
        self._site_path = (site_path or "").strip() or None
        self._drive_name = (drive_name or "").strip()
        self._drive_id = (drive_id or "").strip() or None
        self._folder_root = (folder_root or "").replace("\\", "/").strip().strip("/")
        self._folder_url = (folder_url or "").strip() or None
        self._site_id_cache: str | None = None
        self._resolved_folder_cache: _ResolvedFolder | None = None
        self._validate_mode_config()
        logger.info(
            "%s Instanciado. mode=%s folder_root=%s drive_id_len=%s folder_url_set=%s",
            _LOG_PREFIX,
            self._mode,
            self._folder_root or "(vacío)",
            len(self._drive_id or ""),
            self._folder_url is not None,
        )

    def _validate_mode_config(self) -> None:
        if self._mode == "drive_id" and not self._drive_id:
            raise RuntimeError(
                "SHAREPOINT_MODE=drive_id exige SHAREPOINT_DRIVE_ID."
            )
        if self._mode == "folder_url" and not self._folder_url:
            raise RuntimeError(
                "SHAREPOINT_MODE=folder_url exige SHAREPOINT_FOLDER_URL."
            )
        if self._mode == "site_path":
            if not self._hostname or not self._site_path:
                raise RuntimeError(
                    "SHAREPOINT_MODE=site_path exige SHAREPOINT_HOSTNAME "
                    "y SHAREPOINT_SITE_PATH."
                )

    # ------------------------------------------------------------- #
    # Public API
    # ------------------------------------------------------------- #
    def upload_contrato_pdf(
        self,
        *,
        filename: str,
        file_bytes: bytes,
        codigo_contrato: str,
        gra_rep_ide: int,
    ) -> StoredContratoPdf:
        if not file_bytes:
            raise ValueError("upload_contrato_pdf: file_bytes vacío.")

        safe_codigo = self._safe_segment(codigo_contrato, fallback="contrato")
        safe_name = self._safe_filename(filename or f"contrato_{gra_rep_ide}.pdf")
        if not safe_name.lower().endswith(".pdf"):
            safe_name = f"{safe_name}.pdf"
        final_name = f"{safe_codigo}_{gra_rep_ide}_{safe_name}"

        now = datetime.now(timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")

        logger.info(
            "%s upload INICIO codigo=%s gra_rep_ide=%s bytes=%s name=%s",
            _LOG_PREFIX,
            codigo_contrato,
            gra_rep_ide,
            len(file_bytes),
            final_name,
        )

        if self._mode == "drive_id":
            drive_id = self._drive_id  # type: ignore[assignment]
            assert drive_id is not None
            base_parent_id = self._ensure_folder_path_from_root(
                drive_id=drive_id,
                folder_path=self._base_folder_path(None),
            )
            year_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=base_parent_id,
                folder_name=year,
            )
            month_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=year_id,
                folder_name=month,
            )
            contratos_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=month_id,
                folder_name="contratos",
            )
            uploaded = self._upload_file_by_parent(
                drive_id=drive_id,
                parent_item_id=contratos_id,
                filename=final_name,
                mime_type="application/pdf",
                file_bytes=file_bytes,
            )
            relative_path = str(
                PurePosixPath(self._base_folder_path(None))
                / year
                / month
                / "contratos"
                / final_name
            )

        elif self._mode == "folder_url":
            base_folder = self._resolve_folder_from_share_url()
            drive_id = base_folder.drive_id
            year_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=base_folder.item_id,
                folder_name=year,
            )
            month_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=year_id,
                folder_name=month,
            )
            contratos_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=month_id,
                folder_name="contratos",
            )
            uploaded = self._upload_file_by_parent(
                drive_id=drive_id,
                parent_item_id=contratos_id,
                filename=final_name,
                mime_type="application/pdf",
                file_bytes=file_bytes,
            )
            relative_path = str(
                PurePosixPath(base_folder.folder_name or "albaranes")
                / year
                / month
                / "contratos"
                / final_name
            )

        else:
            site_id = self._resolve_site_id()
            drive_id = self._resolve_drive_id(site_id)
            relative_path = str(
                PurePosixPath(self._base_folder_path(None))
                / year
                / month
                / "contratos"
                / final_name
            )
            uploaded = self._upload_file_by_relative_path(
                drive_id=drive_id,
                relative_path=relative_path,
                mime_type="application/pdf",
                file_bytes=file_bytes,
            )

        web_url = str(uploaded.get("webUrl") or "").strip() or None
        logger.info(
            "%s upload OK relative_path=%s web_url=%s",
            _LOG_PREFIX,
            relative_path,
            web_url,
        )
        return StoredContratoPdf(
            relative_path=relative_path,
            web_url=web_url,
        )

    # ------------------------------------------------------------- #
    # Helpers (idénticos al storage del servicio 3)
    # ------------------------------------------------------------- #
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token_provider.get_token()}"}

    @staticmethod
    def _safe_filename(filename: str) -> str:
        cleaned = "".join(
            char if char not in '<>:"\\|?*' else "_"
            for char in (filename or "document.bin")
        )
        return cleaned.strip().strip(".") or "document.bin"

    @staticmethod
    def _safe_segment(value: str, fallback: str) -> str:
        if not value:
            return fallback
        cleaned = re.sub(r'[<>:"/\\|?*]+', "_", value.strip())
        cleaned = re.sub(r"\s+", "_", cleaned)
        cleaned = cleaned.strip("._")
        return cleaned or fallback

    @staticmethod
    def _encode_sharing_url(url: str) -> str:
        raw = base64.b64encode(url.encode("utf-8")).decode("ascii")
        token = raw.rstrip("=").replace("/", "_").replace("+", "-")
        return f"u!{token}"

    def _base_folder_path(self, folder_label: str | None) -> str:
        return (self._folder_root or folder_label or "albaranes").strip().strip("/")

    def _resolve_site_id(self) -> str:
        if self._site_id_cache:
            return self._site_id_cache
        if not self._hostname or not self._site_path:
            raise RuntimeError(
                "Faltan SHAREPOINT_HOSTNAME/SHAREPOINT_SITE_PATH."
            )
        relative_path = self._site_path.lstrip("/")
        url = f"{self._base}/sites/{self._hostname}:/{relative_path}"
        response = self._client.get(url, headers=self._headers())
        if response.status_code >= 300:
            raise RuntimeError(
                f"Graph get site by path {response.status_code}: "
                f"{response.text[:500]}"
            )
        site_id = str((response.json() or {}).get("id") or "").strip()
        if not site_id:
            raise RuntimeError("Graph no devolvió site.id.")
        self._site_id_cache = site_id
        return site_id

    def _resolve_drive_id(self, site_id: str) -> str:
        if self._drive_id:
            return self._drive_id
        url = f"{self._base}/sites/{site_id}/drives"
        response = self._client.get(url, headers=self._headers())
        if response.status_code >= 300:
            raise RuntimeError(
                f"Graph list drives {response.status_code}: "
                f"{response.text[:500]}"
            )
        items = (response.json() or {}).get("value") or []
        for item in items:
            if str(item.get("name") or "").strip() == self._drive_name:
                self._drive_id = str(item["id"])
                return self._drive_id
        raise RuntimeError(
            f"No se encontró la biblioteca '{self._drive_name}'."
        )

    def _resolve_folder_from_share_url(self) -> _ResolvedFolder:
        if self._resolved_folder_cache:
            return self._resolved_folder_cache
        if not self._folder_url:
            raise RuntimeError("No se ha configurado SHAREPOINT_FOLDER_URL.")
        token = self._encode_sharing_url(self._folder_url)
        url = f"{self._base}/shares/{token}/driveItem"
        response = self._client.get(url, headers=self._headers())
        if response.status_code >= 300:
            raise RuntimeError(
                f"Graph get share driveItem {response.status_code}: "
                f"{response.text[:500]}"
            )
        payload = response.json() or {}
        if not isinstance(payload.get("folder"), dict):
            raise RuntimeError(
                "La URL en SHAREPOINT_FOLDER_URL no apunta a una carpeta."
            )
        item_id = str(payload.get("id") or "").strip()
        parent_reference = payload.get("parentReference") or {}
        drive_id = str(parent_reference.get("driveId") or "").strip()
        folder_name = str(payload.get("name") or "").strip() or None
        if not item_id or not drive_id:
            raise RuntimeError(
                "Graph no devolvió id/driveId al resolver SHAREPOINT_FOLDER_URL."
            )
        resolved = _ResolvedFolder(
            drive_id=drive_id,
            item_id=item_id,
            folder_name=folder_name,
        )
        self._resolved_folder_cache = resolved
        return resolved

    def _children_endpoint(self, *, drive_id: str, parent_item_id: str) -> str:
        if parent_item_id == "root":
            return f"{self._base}/drives/{drive_id}/root/children"
        return f"{self._base}/drives/{drive_id}/items/{parent_item_id}/children"

    def _list_children(
        self,
        *,
        drive_id: str,
        parent_item_id: str,
    ) -> list[dict]:
        url = self._children_endpoint(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
        )
        params = {"$select": "id,name,folder"}
        items: list[dict] = []
        while url:
            response = self._client.get(url, headers=self._headers(), params=params)
            params = None
            if response.status_code >= 300:
                raise RuntimeError(
                    f"Graph list children {response.status_code}: "
                    f"{response.text[:500]}"
                )
            payload = response.json() or {}
            values = payload.get("value") or []
            if isinstance(values, list):
                items.extend(item for item in values if isinstance(item, dict))
            url = str(payload.get("@odata.nextLink") or "").strip() or None
        return items

    def _find_child_folder(
        self,
        *,
        drive_id: str,
        parent_item_id: str,
        folder_name: str,
    ) -> dict | None:
        for item in self._list_children(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
        ):
            if str(item.get("name") or "").strip() != folder_name:
                continue
            if not isinstance(item.get("folder"), dict):
                continue
            return item
        return None

    def _create_folder(
        self,
        *,
        drive_id: str,
        parent_item_id: str,
        folder_name: str,
    ) -> str:
        url = self._children_endpoint(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
        )
        payload = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "fail",
        }
        response = self._client.post(
            url,
            headers=self._headers(),
            json=payload,
        )
        if response.status_code == 409:
            existing = self._find_child_folder(
                drive_id=drive_id,
                parent_item_id=parent_item_id,
                folder_name=folder_name,
            )
            if existing:
                existing_id = str(existing.get("id") or "").strip()
                if existing_id:
                    return existing_id
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                f"Graph create folder {response.status_code}: {response.text[:500]}"
            )
        folder_id = str((response.json() or {}).get("id") or "").strip()
        if not folder_id:
            raise RuntimeError("Graph no devolvió id al crear carpeta.")
        return folder_id

    def _ensure_child_folder(
        self,
        *,
        drive_id: str,
        parent_item_id: str,
        folder_name: str,
    ) -> str:
        existing = self._find_child_folder(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
            folder_name=folder_name,
        )
        if existing:
            folder_id = str(existing.get("id") or "").strip()
            if folder_id:
                return folder_id
        return self._create_folder(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
            folder_name=folder_name,
        )

    def _ensure_folder_path_from_root(
        self,
        *,
        drive_id: str,
        folder_path: str,
    ) -> str:
        parts = [
            part
            for part in PurePosixPath(folder_path).parts
            if part and part != "/"
        ]
        parent_id = "root"
        for folder_name in parts:
            parent_id = self._ensure_child_folder(
                drive_id=drive_id,
                parent_item_id=parent_id,
                folder_name=folder_name,
            )
        return parent_id

    def _upload_file_by_parent(
        self,
        *,
        drive_id: str,
        parent_item_id: str,
        filename: str,
        mime_type: str,
        file_bytes: bytes,
    ) -> dict:
        encoded_name = quote(filename, safe="")
        if parent_item_id == "root":
            url = f"{self._base}/drives/{drive_id}/root:/{encoded_name}:/content"
        else:
            url = (
                f"{self._base}/drives/{drive_id}/items/{parent_item_id}:/"
                f"{encoded_name}:/content"
            )
        headers = self._headers()
        headers["Content-Type"] = mime_type or "application/octet-stream"
        response = self._client.put(url, headers=headers, content=file_bytes)
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                f"Graph upload file {response.status_code}: {response.text[:500]}"
            )
        return response.json() or {}

    def _upload_file_by_relative_path(
        self,
        *,
        drive_id: str,
        relative_path: str,
        mime_type: str,
        file_bytes: bytes,
    ) -> dict:
        encoded_path = quote(relative_path, safe="/")
        url = f"{self._base}/drives/{drive_id}/root:/{encoded_path}:/content"
        headers = self._headers()
        headers["Content-Type"] = mime_type or "application/octet-stream"
        response = self._client.put(url, headers=headers, content=file_bytes)
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                f"Graph upload file {response.status_code}: {response.text[:500]}"
            )
        return response.json() or {}

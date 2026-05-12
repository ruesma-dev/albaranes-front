# config/settings.py
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


class Settings(BaseSettings):
    """Configuración del servicio 4 (portal de revisión humana).

    REFACTOR "el sv3 es dueño de los contratos"
    -------------------------------------------
    Antes el sv4 tenía aquí variables ``SIGRID_API_*`` y un wiring
    duplicado del sv3 (cliente Sigrid + repository.replace_contratos_and_select
    + SharePointContratoPdfStorage). Eso era un acoplamiento incorrecto:
    el sv3 ya tenía el ``ContratoEnrichmentService`` con UPSERT por
    ``sigrid_ide`` y manejaba los PDFs. El sv4 duplicaba esa lógica
    con un viejo DELETE+INSERT que rompía la deduplicación.

    Refactor (mayo 2026): el sv4 deja de llamar a Sigrid directamente
    y llama al sv3 vía HTTP (``POST /v1/albaranes/{id}/re-fetch-contratos``).
    El sv3 reutiliza su enrichment con ``force_refetch=True`` y devuelve
    un outcome (status/message/...) que el sv4 reenvía al front tal cual.

    Consecuencias en este settings:
      * NO hay variables ``SIGRID_*`` — esas vivien solo en el .env del sv3.
      * Nuevas variables ``SV3_*`` (base_url, path, timeout) — análogas
        a las ``SV7_*`` que ya teníamos.
      * Cero variables relacionadas con SharePoint para contratos —
        ese caso lo cubre el sv3.

    Lo que se mantiene
    -------------------
    PostgreSQL (mismo cluster que sv3, lectura del merge para pintar
    el portal); Graph + SharePoint solo para preview del PDF del
    propio albarán (no contratos); cliente al orquestador sv7 para
    eventos de revisión.
    """

    # ------------------------------------------------------------ #
    # BBDD compartida.
    # ------------------------------------------------------------ #
    pg_host: str = Field("localhost", alias="PG_HOST")
    pg_port: int = Field(5432, alias="PG_PORT")
    pg_db: str = Field("albaranes", alias="PG_DB")
    pg_user: str = Field("postgres", alias="PG_USER")
    pg_password: str = Field(..., alias="PG_PASSWORD")

    pg_admin_db: str = Field("postgres", alias="PG_ADMIN_DB")
    pg_admin_user: str = Field("postgres", alias="PG_ADMIN_USER")
    pg_admin_password: str = Field(..., alias="PG_ADMIN_PASSWORD")
    auto_create_database: bool = Field(True, alias="AUTO_CREATE_DATABASE")

    # ------------------------------------------------------------ #
    # Graph + SharePoint — SOLO para previsualización del PDF del
    # propio albarán en el portal. El PDF de contrato lo gestiona el
    # sv3 (no este servicio).
    # ------------------------------------------------------------ #
    graph_key: str | None = Field(None, alias="GRAPH_KEY")
    sharepoint_drive_id: str | None = Field(
        None, alias="SHAREPOINT_DRIVE_ID",
    )
    graph_timeout_s: int = Field(60, alias="GRAPH_TIMEOUT_S")

    # ------------------------------------------------------------ #
    # API + presentación + UX.
    # ------------------------------------------------------------ #
    api_host: str = Field("127.0.0.1", alias="API_HOST")
    api_port: int = Field(8004, alias="API_PORT")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_dir: str = Field("logs", alias="LOG_DIR")
    service_version: str = Field("1.0.0", alias="SERVICE_VERSION")
    app_title: str = Field(
        "Revisión de Albaranes IA", alias="APP_TITLE",
    )
    default_page_size: int = Field(25, alias="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(100, alias="MAX_PAGE_SIZE")
    default_reviewer: str | None = Field(None, alias="DEFAULT_REVIEWER")

    # ------------------------------------------------------------ #
    # Cliente al orquestador (sv7).
    # sv4 emite dos tipos de evento al orquestador:
    #   - contract-selected: el revisor cambió/eligió el contrato.
    #   - document-approved: el revisor aprobó el documento.
    # Las llamadas son best-effort (BackgroundTask + cliente que
    # silencia errores) — si sv7 está caído, el save del revisor
    # no se rompe.
    # ------------------------------------------------------------ #
    sv7_base_url: str = Field("http://127.0.0.1:8005", alias="SV7_BASE_URL")
    sv7_timeout_s: float = Field(5.0, alias="SV7_TIMEOUT_S")
    sv7_path_contract_selected: str = Field(
        "/v1/events/contract-selected",
        alias="SV7_PATH_CONTRACT_SELECTED",
    )
    sv7_path_document_approved: str = Field(
        "/v1/events/document-approved",
        alias="SV7_PATH_DOCUMENT_APPROVED",
    )

    # ------------------------------------------------------------ #
    # Cliente al persistencia/contratos (sv3).
    #
    # Usado por el endpoint del portal POST /api/documents/{id}/
    # re-fetch-contratos: cuando el revisor pulsa "Volver a buscar"
    # tras editar CIF/obra, el sv4 hace POST al sv3 que reutiliza su
    # ContratoEnrichmentService (con UPSERT por sigrid_ide).
    #
    # Timeout más generoso que sv7 porque esta llamada SÍ ejecuta
    # trabajo síncrono: consulta a Sigrid (puede tardar segundos) +
    # UPSERT en BBDD + opcional descarga/subida de PDF a SharePoint.
    # 60s es conservador.
    # ------------------------------------------------------------ #
    sv3_base_url: str = Field(
        "http://127.0.0.1:8001",
        alias="SV3_BASE_URL",
    )
    sv3_timeout_s: float = Field(60.0, alias="SV3_TIMEOUT_S")
    sv3_path_refetch_contratos: str = Field(
        "/v1/albaranes/{document_id}/re-fetch-contratos",
        alias="SV3_PATH_REFETCH_CONTRATOS",
    )

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        user = quote_plus(self.pg_user)
        password = quote_plus(self.pg_password)
        database = quote_plus(self.pg_db)
        return (
            f"postgresql+psycopg://{user}:{password}"
            f"@{self.pg_host}:{self.pg_port}/{database}"
        )

    @property
    def admin_database_url(self) -> str:
        user = quote_plus(self.pg_admin_user)
        password = quote_plus(self.pg_admin_password)
        database = quote_plus(self.pg_admin_db)
        return (
            f"postgresql+psycopg://{user}:{password}"
            f"@{self.pg_host}:{self.pg_port}/{database}"
        )

    @property
    def preview_enabled(self) -> bool:
        return bool(self.graph_key and self.sharepoint_drive_id)

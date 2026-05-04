# config/settings.py
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


class Settings(BaseSettings):
    pg_host: str = Field("localhost", alias="PG_HOST")
    pg_port: int = Field(5432, alias="PG_PORT")
    pg_db: str = Field("albaranes", alias="PG_DB")
    pg_user: str = Field("postgres", alias="PG_USER")
    pg_password: str = Field(..., alias="PG_PASSWORD")

    pg_admin_db: str = Field("postgres", alias="PG_ADMIN_DB")
    pg_admin_user: str = Field("postgres", alias="PG_ADMIN_USER")
    pg_admin_password: str = Field(..., alias="PG_ADMIN_PASSWORD")
    auto_create_database: bool = Field(True, alias="AUTO_CREATE_DATABASE")

    graph_key: str | None = Field(None, alias="GRAPH_KEY")
    sharepoint_drive_id: str | None = Field(None, alias="SHAREPOINT_DRIVE_ID")
    graph_timeout_s: int = Field(60, alias="GRAPH_TIMEOUT_S")

    api_host: str = Field("127.0.0.1", alias="API_HOST")
    # ----------------------------------------------------------- #
    # CAMBIO: default movido de 8002 → 8004 para evitar el conflicto
    # con sv5 (que también escucha en 8002). Si tu .env tiene
    # API_PORT=8004 explícito, este default no aplica.
    # ----------------------------------------------------------- #
    api_port: int = Field(8004, alias="API_PORT")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_dir: str = Field("logs", alias="LOG_DIR")
    service_version: str = Field("1.0.0", alias="SERVICE_VERSION")
    app_title: str = Field("Revisión de Albaranes IA", alias="APP_TITLE")
    default_page_size: int = Field(25, alias="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(100, alias="MAX_PAGE_SIZE")
    default_reviewer: str | None = Field(None, alias="DEFAULT_REVIEWER")

    # ----------------------------------------------------------- #
    # NUEVO: cliente al orquestador (sv7).
    # sv4 emite dos tipos de evento al orquestador:
    #   - contract-selected: el revisor cambió/eligió el contrato.
    #   - document-approved: el revisor aprobó el documento.
    # Las llamadas son best-effort (BackgroundTask + cliente que
    # silencia errores) — si sv7 está caído, el save del revisor
    # no se rompe.
    # Timeout corto (5s) porque sv7 responde inmediato (202 ack).
    # ----------------------------------------------------------- #
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

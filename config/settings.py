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
    # Colas (sustituyen a los antiguos clientes HTTP a sv7 y sv3).
    #
    # sv4 ya NO llama por HTTP a sv7 (disuelto) ni a sv3. En su lugar
    # publica mensajes en las colas del sistema mediante el publicador
    # de ``ruesma_comun.colas`` (best-effort):
    #   - Elegir/cambiar contrato  → MensajeValoracion(force=True) → q-valoracion
    #   - Aprobar documento        → MensajeFeedback              → q-feedback
    #   - Re-fetch de contratos    → MensajePersistencia(force=True) → q-persistencia
    #   - Purga (hard-delete)      → limpieza directa de workflow_runs (BBDD)
    #
    # La conexión a las colas la resuelve ``construir_publicador`` leyendo
    # del ENTORNO (no de este settings):
    #   - COLAS_ACCOUNT_URL          (managed identity en Azure)
    #   - COLAS_CONNECTION_STRING    (local/Azurite)
    # Por eso aquí no hay campos COLAS_*: ``extra="ignore"`` los tolera y
    # comun los consume directamente vía os.environ.
    # ------------------------------------------------------------ #
    # ------------------------------------------------------------ #
    # Sigrid API — SOLO LECTURA para los desplegables de cabecera
    # (elegir proveedor / obra). NO reintroduce el camino de escritura
    # de contratos (UPSERT/PDF), que sigue yendo por sv3. Estas
    # consultas (proveedores de una obra, lista de obras) son lookups
    # de referencia para la UI. Mismos valores que el .env del sv3.
    # Si falta alguna credencial, los endpoints /api/sigrid/* devuelven
    # ok=false y el front mantiene la entrada manual.
    # ------------------------------------------------------------ #
    sigrid_api_base_url: str | None = Field(
        default=None, alias="SIGRID_API_BASE_URL",
    )
    sigrid_api_function_key: str | None = Field(
        default=None, alias="SIGRID_API_FUNCTION_KEY",
    )
    sigrid_api_database: str | None = Field(
        default=None, alias="SIGRID_API_DATABASE",
    )
    sigrid_api_timeout_s: float = Field(30.0, alias="SIGRID_API_TIMEOUT_S")

    @property
    def sigrid_lookup_enabled(self) -> bool:
        return bool(
            (self.sigrid_api_base_url or "").strip()
            and (self.sigrid_api_function_key or "").strip()
            and (self.sigrid_api_database or "").strip()
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

# main.py
from __future__ import annotations

from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from config.logging_config import configure_logging
from config.settings import Settings
from interface_adapters.web.app import build_app


def main() -> int:
    # Vuelca el .env a os.environ antes de build_app: el publicador de
    # colas (construir_publicador, en ruesma_comun) lee de os.environ.
    # Así COLAS_CONNECTION_STRING vive en el .env y sv4 corre CON colas
    # (modo pipeline). Sin la variable, sigue el modo solo-front.
    load_dotenv(Path(__file__).resolve().parent / ".env")
    settings = Settings()
    configure_logging(Path(settings.log_dir), settings.log_level)
    app = build_app(settings)
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

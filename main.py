# main.py
from __future__ import annotations

from pathlib import Path

import uvicorn

from config.logging_config import configure_logging
from config.settings import Settings
from interface_adapters.web.app import build_app


def main() -> int:
    # Vuelca el .env a os.environ ANTES de construir Settings y, sobre todo,
    # antes de cablear las colas: ``ruesma_comun.colas.construir_publicador``
    # lee COLAS_CONNECTION_STRING / COLAS_ACCOUNT_URL directamente de
    # os.environ (no de Settings, que las ignora por diseno). pydantic-settings
    # lee el .env para el objeto Settings, pero NO lo exporta a os.environ;
    # por eso, como en los workers (sv2/sv3/sv6), cargamos el .env aqui.
    # En la nube no hay .env (lo excluye el .dockerignore) y load_dotenv()
    # es un no-op inofensivo: las COLAS_* llegan como env vars del Container App.
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

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

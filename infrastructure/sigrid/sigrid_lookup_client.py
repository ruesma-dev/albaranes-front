# infrastructure/sigrid/sigrid_lookup_client.py
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from domain.models.review_models import ContratoOption, ObraOption, ProveedorOption

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[sigrid-lookup]"


# Proveedores con contrato de compra para una obra. Mismo patron de
# joins que el contrato-client del sv3 (ctr + con + con_obr), pero SIN el
# filtro de cif: queremos TODOS los proveedores de la obra. emp=1 (empresa
# Construcciones Ruesma), igual que el contrato-client.
_SQL_PROVEEDORES_POR_OBRA = """\
SELECT DISTINCT
    ctr.entcif AS cif,
    ctr.entres AS nombre
FROM ctr
JOIN con AS con_ctr ON ctr.ide    = con_ctr.ide
JOIN con AS con_obr ON ctr.obride = con_obr.ide
WHERE con_obr.cod = ?
  AND con_ctr.emp = 1
ORDER BY ctr.entres
"""

# Lista de obras (codigo + nombre). Mismo join que el obra-client del sv3
# (obr + con). Sin filtro emp para no excluir obras.
#
# IMPORTANTE: NO usar SELECT DISTINCT aqui. ``obr.res`` es de tipo
# text/ntext en Sigrid y SQL Server lanza error 42000 ("el tipo de datos
# text/ntext no se puede seleccionar como DISTINCT porque no es
# comparable"). Por eso traemos todas las filas (una por obra; pueden
# repetirse con.cod si la obra se recreo) y DEDUPLICAMOS POR CODIGO en
# Python. ``ORDER BY con.cod`` si es valido porque ``cod`` es comparable.
_SQL_OBRAS = """\
SELECT
    con.cod AS codigo,
    obr.res AS nombre
FROM obr
JOIN con ON obr.ide = con.ide
WHERE con.cod IS NOT NULL
ORDER BY con.cod
"""

# Contratos de una obra (codigo + nombre del contrato + proveedor). Mismo
# join que proveedores (ctr + con_ctr + con_obr), filtrando por obra. emp=1
# (Construcciones Ruesma). NO usamos SELECT DISTINCT porque ``con_ctr.res``
# (nombre del contrato) es text/ntext en Sigrid y SQL Server no permite
# DISTINCT sobre ese tipo (error 42000); deduplicamos POR CODIGO en Python,
# igual que el desplegable de obras. El filtro por CIF se aplica en Python
# (laxo, normalizando) para tolerar diferencias de formato del CIF.
_SQL_CONTRATOS_POR_OBRA = """\
SELECT
    con_ctr.cod AS codigo,
    con_ctr.res AS nombre,
    ctr.entcif  AS cif,
    ctr.entres  AS nombre_proveedor
FROM ctr
JOIN con AS con_ctr ON ctr.ide    = con_ctr.ide
JOIN con AS con_obr ON ctr.obride = con_obr.ide
WHERE con_obr.cod = ?
  AND con_ctr.emp = 1
ORDER BY con_ctr.cod
"""


class SigridLookupClient:
    """Cliente HTTP de SOLO LECTURA contra la Function App ``sigrid-api``.

    Sirve los desplegables de cabecera del portal (elegir proveedor con
    contrato en la obra / elegir obra). No persiste nada ni toca el camino
    de escritura de contratos (eso es del sv3). Realiza POST a
    ``/api/sql/read`` con cabecera ``x-functions-key``.
    """

    def __init__(
        self,
        *,
        base_url: str,
        function_key: str,
        database: str,
        timeout_s: float = 30.0,
        max_rows: int = 5000,
    ) -> None:
        if not base_url:
            raise ValueError("SigridLookupClient requiere base_url no vacio")
        if not function_key:
            raise ValueError("SigridLookupClient requiere function_key no vacio")
        if not database:
            raise ValueError("SigridLookupClient requiere database no vacio")
        self._base_url = base_url.rstrip("/")
        self._function_key = function_key
        self._database = database
        self._timeout_s = float(timeout_s)
        self._max_rows = int(max_rows)
        logger.info(
            "%s Instanciado. base_url=%s database=%s timeout_s=%s max_rows=%s key_len=%s",
            _LOG_PREFIX,
            self._base_url,
            self._database,
            self._timeout_s,
            self._max_rows,
            len(function_key),
        )

    # ----------------------------------------------------------------- #
    # API publica
    # ----------------------------------------------------------------- #
    def fetch_proveedores_por_obra(
        self,
        *,
        codigo_obra: str,
    ) -> list[ProveedorOption]:
        codigo = (codigo_obra or "").strip()
        if not codigo:
            return []
        columns, rows = self._post_sql_read(
            sql=_SQL_PROVEEDORES_POR_OBRA,
            parameters=[codigo],
            label=f"proveedores_obra_{codigo}",
        )
        seen: set[str] = set()
        out: list[ProveedorOption] = []
        for row in rows:
            row_map = dict(zip(columns, row))
            cif = _opt_str(row_map.get("cif"))
            if not cif or cif in seen:
                continue
            seen.add(cif)
            out.append(
                ProveedorOption(cif=cif, nombre=_opt_str(row_map.get("nombre")))
            )
        logger.info(
            "%s proveedores_por_obra obra=%s -> %s proveedores",
            _LOG_PREFIX,
            codigo,
            len(out),
        )
        return out

    def fetch_obras(self) -> list[ObraOption]:
        columns, rows = self._post_sql_read(
            sql=_SQL_OBRAS,
            parameters=[],
            label="obras",
        )
        seen: set[str] = set()
        out: list[ObraOption] = []
        for row in rows:
            row_map = dict(zip(columns, row))
            cod = _opt_str(row_map.get("codigo"))
            if not cod or cod in seen:
                continue
            seen.add(cod)
            out.append(
                ObraOption(codigo=cod, nombre=_opt_str(row_map.get("nombre")))
            )
        logger.info("%s obras -> %s obras", _LOG_PREFIX, len(out))
        return out

    def fetch_contratos(
        self,
        *,
        codigo_obra: str,
        cif: str | None = None,
    ) -> list[ContratoOption]:
        """Contratos de una obra en Sigrid (codigo + nombre + proveedor).

        Si se pasa ``cif``, filtra (laxo, normalizado) a los contratos de
        ese proveedor; si no, devuelve todos los de la obra. Deduplica por
        codigo de contrato.
        """
        codigo = (codigo_obra or "").strip()
        if not codigo:
            return []
        columns, rows = self._post_sql_read(
            sql=_SQL_CONTRATOS_POR_OBRA,
            parameters=[codigo],
            label=f"contratos_obra_{codigo}",
        )

        def _norm_cif(value: str | None) -> str:
            return (value or "").strip().upper().replace(" ", "").replace("-", "")

        cif_filtro = _norm_cif(cif) if cif else None
        seen: set[str] = set()
        out: list[ContratoOption] = []
        for row in rows:
            row_map = dict(zip(columns, row))
            cod = _opt_str(row_map.get("codigo"))
            if not cod or cod in seen:
                continue
            row_cif = _opt_str(row_map.get("cif"))
            if cif_filtro and _norm_cif(row_cif) != cif_filtro:
                continue
            seen.add(cod)
            out.append(
                ContratoOption(
                    codigo=cod,
                    nombre=_opt_str(row_map.get("nombre")),
                    cif=row_cif,
                    nombre_proveedor=_opt_str(row_map.get("nombre_proveedor")),
                )
            )
        logger.info(
            "%s contratos_obra obra=%s cif=%s -> %s contratos",
            _LOG_PREFIX, codigo, cif_filtro or "(todos)", len(out),
        )
        return out

    # ----------------------------------------------------------------- #
    # HTTP primitive
    # ----------------------------------------------------------------- #
    def _post_sql_read(
        self,
        *,
        sql: str,
        parameters: list[Any],
        label: str,
    ) -> tuple[list[str], list[list[Any]]]:
        url = f"{self._base_url}/api/sql/read"
        payload = {
            "database": self._database,
            "sql": sql,
            "parameters": parameters,
            "timeout_seconds": int(self._timeout_s),
            "max_rows": self._max_rows,
        }
        headers = {
            "x-functions-key": self._function_key,
            "Content-Type": "application/json",
        }

        logger.info(
            "%s REQUEST [%s] -> POST %s db=%s params=%s",
            _LOG_PREFIX,
            label,
            url,
            self._database,
            parameters,
        )

        transport = httpx.HTTPTransport(retries=1)
        try:
            with httpx.Client(timeout=self._timeout_s, transport=transport) as client:
                response = client.post(url, json=payload, headers=headers)
        except Exception as exc:
            logger.exception(
                "%s FALLO de transporte [%s]. exc=%r", _LOG_PREFIX, label, exc
            )
            raise

        status = response.status_code
        body_text = response.text or ""
        if status >= 400:
            logger.warning(
                "%s RESPONSE [%s] status=%s preview=%s",
                _LOG_PREFIX,
                label,
                status,
                body_text[:300],
            )
            raise RuntimeError(f"sigrid-api respondio {status}: {body_text[:300]}")

        try:
            body: dict[str, Any] = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"sigrid-api respuesta no JSON: {body_text[:300]}"
            ) from exc

        if not body.get("ok", False):
            raise RuntimeError(f"sigrid-api devolvio ok=false: {body!r}")

        columns: list[str] = list(body.get("columns") or [])
        rows: list[list[Any]] = list(body.get("rows") or [])
        return columns, rows


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)

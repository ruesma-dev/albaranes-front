# infrastructure/sigrid/sigrid_api_contrato_client.py
from __future__ import annotations

import json
import logging
from collections import OrderedDict
from typing import Any

import httpx

from domain.models.contrato_sigrid_models import (
    ContratoFromSigrid,
    ContratoLineFromSigrid,
)
from domain.ports.contrato_refetch_port import ContratoPdfPayload

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[contrato-refetch][sigrid-client]"


_SQL_HEADER_AND_LINES = """\
SELECT
    ctr.ide             AS contrato_ide,
    con_ctr.cod         AS codigo_contrato,
    con_ctr.res         AS nombre_contrato,
    con_ctr.fec         AS fecha_alta_contrato,
    ctr.fecdoc          AS fecha_contrato,
    ctr.fecvig1         AS vigencia_desde,
    ctr.fecvig2         AS vigencia_hasta,
    ctr.totbas          AS importe_total,
    ctr.entcif          AS cif_proveedor,
    ctr.entres          AS nombre_proveedor,
    con_obr.cod         AS codigo_obra,
    con_obr.res         AS nombre_obra,
    ctrpro.pos          AS linea,
    ctrpro.numlin       AS numero_linea,
    con_pro.cod         AS codigo_producto,
    ctrpro.cod2         AS codigo_alternativo,
    ctrpro.unimed       AS unidad_medida,
    ctrpro.res          AS descripcion_linea,
    ctrpro.can          AS uds,
    ctrpro.canser       AS cantidad_servida,
    ctrpro.canfac       AS cantidad_facturada,
    (ctrpro.can - ISNULL(ctrpro.canser, 0)) AS pendiente_servir,
    ctrpro.pre          AS precio_unitario,
    ctrpro.tar          AS precio_bruto,
    ctrpro.dto          AS descuentos,
    ctrpro.tot          AS importe_linea,
    ctrpro.ivacuo       AS cuota_iva,
    ctrpro.docoricod    AS doc_origen,
    obrparpar.cod       AS codigo_partida,
    obrparpar.res       AS descripcion_partida
FROM ctr
JOIN con AS con_ctr       ON ctr.ide     = con_ctr.ide
JOIN con AS con_obr       ON ctr.obride  = con_obr.ide
JOIN prv                  ON ctr.entide  = prv.ide
LEFT JOIN ctrpro          ON ctrpro.docide = ctr.ide
LEFT JOIN pro             ON ctrpro.proide = pro.ide
LEFT JOIN con AS con_pro  ON pro.ide       = con_pro.ide
LEFT JOIN obrparpar       ON ctrpro.paride = obrparpar.ide
WHERE
    prv.cif       = ?
AND con_obr.cod   = ?
AND con_ctr.emp   = 1
ORDER BY con_ctr.cod, ctrpro.pos
"""

_SQL_GRA_COD_BY_CONTRATO = """\
SELECT
    rcg.pos             AS rcg_pos,
    gra.cod             AS gra_cod,
    gra.nom             AS gra_nom,
    gra.nomori          AS gra_nomori
FROM rcg
JOIN gra ON rcg.gra = gra.ide
WHERE rcg.con = ?
ORDER BY rcg.pos
"""

_SQL_GRA_REP_BY_COD = """\
SELECT
    ide                 AS gra_rep_ide,
    nom                 AS gra_nom,
    nomori              AS gra_nomori
FROM gra
WHERE cod = ?
"""


class SigridApiContratoClient:
    def __init__(
        self,
        *,
        base_url: str,
        function_key: str,
        database: str,
        timeout_s: float = 30.0,
        max_rows: int = 1000,
        database_rep: str = "ruesma_rep",
        pdf_timeout_s: float = 120.0,
    ) -> None:
        if not base_url:
            raise ValueError("SigridApiContratoClient requiere base_url")
        if not function_key:
            raise ValueError("SigridApiContratoClient requiere function_key")
        if not database:
            raise ValueError("SigridApiContratoClient requiere database")
        self._base_url = base_url.rstrip("/")
        self._function_key = function_key
        self._database = database
        self._database_rep = database_rep
        self._timeout_s = float(timeout_s)
        self._max_rows = int(max_rows)
        self._pdf_timeout_s = float(pdf_timeout_s)
        logger.info(
            "%s Instanciado. base_url=%s database=%s database_rep=%s "
            "max_rows=%s pdf_timeout_s=%s key_len=%s",
            _LOG_PREFIX,
            self._base_url,
            self._database,
            self._database_rep,
            self._max_rows,
            self._pdf_timeout_s,
            len(function_key),
        )

    def fetch_contratos(
        self,
        *,
        cif_proveedor: str,
        codigo_obra_normalizado: str,
    ) -> list[ContratoFromSigrid]:
        columns, rows = self._post_sql_read(
            sql=_SQL_HEADER_AND_LINES,
            parameters=[cif_proveedor, codigo_obra_normalizado],
            database=self._database,
            label="header_and_lines",
        )
        contrato_ides, results_without_pdf = self._group_rows_by_contrato(
            columns=columns,
            rows=rows,
        )

        if not results_without_pdf:
            return []

        enriched: list[ContratoFromSigrid] = []
        for contrato, contrato_ide in zip(results_without_pdf, contrato_ides):
            gra_rep_ide = self._safe_fetch_gra_rep_ide(contrato_ide=contrato_ide)
            enriched.append(
                ContratoFromSigrid(
                    codigo_contrato=contrato.codigo_contrato,
                    nombre_contrato=contrato.nombre_contrato,
                    fecha_alta_contrato=contrato.fecha_alta_contrato,
                    fecha_contrato=contrato.fecha_contrato,
                    vigencia_desde=contrato.vigencia_desde,
                    vigencia_hasta=contrato.vigencia_hasta,
                    importe_total=contrato.importe_total,
                    cif_proveedor=contrato.cif_proveedor,
                    nombre_proveedor=contrato.nombre_proveedor,
                    codigo_obra=contrato.codigo_obra,
                    nombre_obra=contrato.nombre_obra,
                    gra_rep_ide=gra_rep_ide,
                    pdf_sharepoint_relative_path=None,
                    pdf_sharepoint_web_url=None,
                    lines=contrato.lines,
                )
            )
        return enriched

    def download_contrato_pdf(
        self,
        *,
        gra_rep_ide: int,
    ) -> ContratoPdfPayload | None:
        url = f"{self._base_url}/api/documents/read"
        payload = {
            "database": self._database_rep,
            "schema": "dbo",
            "table": "gra",
            "id_column": "ide",
            "id_value": int(gra_rep_ide),
            "blob_column": "ima",
            "filename_columns": ["nomori", "nom"],
            "disposition": "attachment",
        }
        headers = {
            "x-functions-key": self._function_key,
            "Content-Type": "application/json",
        }

        logger.info(
            "%s DOWNLOAD REQUEST gra_rep_ide=%s",
            _LOG_PREFIX,
            gra_rep_ide,
        )

        try:
            with httpx.Client(timeout=self._pdf_timeout_s) as client:
                response = client.post(url, json=payload, headers=headers)
        except Exception as exc:
            logger.exception(
                "%s DOWNLOAD FALLO transporte gra_rep_ide=%s exc=%r",
                _LOG_PREFIX,
                gra_rep_ide,
                exc,
            )
            raise

        status = response.status_code
        content = response.content or b""
        content_type = response.headers.get("Content-Type", "") or None
        filename_header = response.headers.get("X-Document-Filename", "") or ""

        logger.info(
            "%s DOWNLOAD RESPONSE status=%s bytes=%s filename=%r",
            _LOG_PREFIX,
            status,
            len(content),
            filename_header,
        )

        if status == 404:
            return None
        if status >= 400:
            preview = content[:300].decode("utf-8", errors="replace")
            raise RuntimeError(
                f"sigrid-api /documents/read respondió {status}: {preview}"
            )
        if not content:
            return None

        filename = filename_header.strip() or f"contrato_{gra_rep_ide}.pdf"
        return ContratoPdfPayload(
            filename=filename,
            content=content,
            content_type=content_type,
        )

    def _post_sql_read(
        self,
        *,
        sql: str,
        parameters: list[Any],
        database: str,
        label: str,
    ) -> tuple[list[str], list[list[Any]]]:
        url = f"{self._base_url}/api/sql/read"
        payload = {
            "database": database,
            "sql": sql,
            "parameters": parameters,
            "timeout_seconds": int(self._timeout_s),
            "max_rows": self._max_rows,
        }
        headers = {
            "x-functions-key": self._function_key,
            "Content-Type": "application/json",
        }

        transport = httpx.HTTPTransport(retries=1)
        try:
            with httpx.Client(timeout=self._timeout_s, transport=transport) as client:
                response = client.post(url, json=payload, headers=headers)
        except Exception as exc:
            logger.exception("%s FALLO transporte [%s]. exc=%r", _LOG_PREFIX, label, exc)
            raise

        status = response.status_code
        body_text = response.text or ""
        logger.info(
            "%s RESPONSE [%s] <- status=%s body_len=%s",
            _LOG_PREFIX,
            label,
            status,
            len(body_text),
        )

        if status >= 400:
            raise RuntimeError(f"sigrid-api respondió {status}: {body_text[:500]}")

        try:
            body: dict[str, Any] = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"sigrid-api respuesta no JSON: {body_text[:500]}"
            ) from exc

        if not body.get("ok", False):
            raise RuntimeError(f"sigrid-api devolvió ok=false: {body!r}")

        columns: list[str] = list(body.get("columns") or [])
        rows: list[list[Any]] = list(body.get("rows") or [])
        return columns, rows

    @staticmethod
    def _group_rows_by_contrato(
        *,
        columns: list[str],
        rows: list[list[Any]],
    ) -> tuple[list[int], list[ContratoFromSigrid]]:
        buckets: "OrderedDict[str, tuple[dict[str, Any], list[ContratoLineFromSigrid]]]" = (
            OrderedDict()
        )

        for row in rows:
            row_map = dict(zip(columns, row))
            codigo = _opt_str(row_map.get("codigo_contrato"))
            if codigo is None:
                continue

            if codigo not in buckets:
                buckets[codigo] = (row_map, [])

            linea_value = row_map.get("linea")
            if linea_value is None:
                continue

            buckets[codigo][1].append(
                ContratoLineFromSigrid(
                    codigo_contrato=codigo,
                    linea=_opt_int(linea_value),
                    numero_linea=_opt_int(row_map.get("numero_linea")),
                    codigo_producto=_opt_str(row_map.get("codigo_producto")),
                    codigo_alternativo=_opt_str(row_map.get("codigo_alternativo")),
                    unidad_medida=_opt_str(row_map.get("unidad_medida")),
                    descripcion_linea=_opt_str(row_map.get("descripcion_linea")),
                    uds=_opt_float(row_map.get("uds")),
                    cantidad_servida=_opt_float(row_map.get("cantidad_servida")),
                    cantidad_facturada=_opt_float(row_map.get("cantidad_facturada")),
                    pendiente_servir=_opt_float(row_map.get("pendiente_servir")),
                    precio_unitario=_opt_float(row_map.get("precio_unitario")),
                    precio_bruto=_opt_float(row_map.get("precio_bruto")),
                    descuentos=_opt_float(row_map.get("descuentos")),
                    importe_linea=_opt_float(row_map.get("importe_linea")),
                    cuota_iva=_opt_float(row_map.get("cuota_iva")),
                    doc_origen=_opt_str(row_map.get("doc_origen")),
                    codigo_partida=_opt_str(row_map.get("codigo_partida")),
                    descripcion_partida=_opt_str(row_map.get("descripcion_partida")),
                )
            )

        contrato_ides: list[int] = []
        results: list[ContratoFromSigrid] = []
        for codigo, (header_row, lines) in buckets.items():
            contrato_ide = _opt_int(header_row.get("contrato_ide"))
            contrato_ides.append(contrato_ide if contrato_ide is not None else 0)
            results.append(
                ContratoFromSigrid(
                    codigo_contrato=codigo,
                    nombre_contrato=_opt_str(header_row.get("nombre_contrato")),
                    fecha_alta_contrato=_opt_int(header_row.get("fecha_alta_contrato")),
                    fecha_contrato=_opt_int(header_row.get("fecha_contrato")),
                    vigencia_desde=_opt_int(header_row.get("vigencia_desde")),
                    vigencia_hasta=_opt_int(header_row.get("vigencia_hasta")),
                    importe_total=_opt_float(header_row.get("importe_total")),
                    cif_proveedor=_opt_str(header_row.get("cif_proveedor")),
                    nombre_proveedor=_opt_str(header_row.get("nombre_proveedor")),
                    codigo_obra=_opt_str(header_row.get("codigo_obra")),
                    nombre_obra=_opt_str(header_row.get("nombre_obra")),
                    gra_rep_ide=None,
                    pdf_sharepoint_relative_path=None,
                    pdf_sharepoint_web_url=None,
                    lines=lines,
                )
            )
        return contrato_ides, results

    def _safe_fetch_gra_rep_ide(self, *, contrato_ide: int) -> int | None:
        if contrato_ide == 0:
            return None
        try:
            return self._fetch_gra_rep_ide(contrato_ide=contrato_ide)
        except Exception as exc:
            logger.warning(
                "%s PDF lookup falló contrato_ide=%s exc=%r",
                _LOG_PREFIX,
                contrato_ide,
                exc,
            )
            return None

    def _fetch_gra_rep_ide(self, *, contrato_ide: int) -> int | None:
        cols, rows = self._post_sql_read(
            sql=_SQL_GRA_COD_BY_CONTRATO,
            parameters=[contrato_ide],
            database=self._database,
            label=f"rcg_gra_for_ctr_{contrato_ide}",
        )
        pdf_cods: list[str] = []
        for row in rows:
            row_map = dict(zip(cols, row))
            cod = _opt_str(row_map.get("gra_cod"))
            if cod is None:
                continue
            name = (
                _opt_str(row_map.get("gra_nomori"))
                or _opt_str(row_map.get("gra_nom"))
                or ""
            )
            if name.lower().endswith(".pdf"):
                pdf_cods.append(cod)

        if not pdf_cods:
            return None

        for cod in pdf_cods:
            cols_rep, rows_rep = self._post_sql_read(
                sql=_SQL_GRA_REP_BY_COD,
                parameters=[cod],
                database=self._database_rep,
                label=f"gra_rep_for_cod_{cod}",
            )
            for row in rows_rep:
                row_map = dict(zip(cols_rep, row))
                gra_rep_ide = _opt_int(row_map.get("gra_rep_ide"))
                if gra_rep_ide is not None:
                    return gra_rep_ide
        return None


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

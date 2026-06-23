# infrastructure/database/workflow_runs_purger.py
"""Limpieza de ``workflow_runs`` tras un hard-delete en el portal (sv4).

Sustituye al evento ``document-purged`` que antes sv4 enviaba por HTTP a
sv7. Sin limpiar estas filas, el dedup por contenido
(``attachment_sha256``) seguiría respondiendo "PDF ya procesado" al
reenviar el mismo albarán tras purgarlo. Sin sv7, sv4 limpia esas filas
directamente (comparten la misma BBDD).

Esquema real de ``workflow_runs`` (confirmado jun 2026)
------------------------------------------------------
  - ``id``                PK
  - ``correlation_key``   UNIQUE  → match EXACTO email+adjunto (no sirve
                                    para dedup por contenido).
  - ``attachment_sha256`` índice no único → dedup por CONTENIDO. En PDFs
                          multipágina es el sha del ADJUNTO ENTERO,
                          compartido por todas las páginas/filas.
  - ``document_id``       índice no único → casa 1:1 con el documento
                          (página) que el portal purga.

Estrategia de borrado
---------------------
El ``source_sha256`` que devuelve ``hard_delete_document`` es por-documento
(página) y NO tiene por qué igualar el ``attachment_sha256`` del adjunto
entero en multipágina. Por eso NO filtramos por ``source_sha256``:

  1. Resolvemos el ``attachment_sha256`` REAL leyéndolo de la fila cuyo
     ``document_id`` casa con el documento purgado (fallback:
     ``source_sha256`` si la fila no lo tuviera).
  2. Borramos ``WHERE document_id = :doc OR attachment_sha256 = :att``,
     desbloqueando el dedup por contenido del PDF completo (single y
     multipágina) además de la propia fila del documento.

DEFENSIVO: comprueba tabla y columnas antes de tocar nada; si algo no
casa, loguea y devuelve 0 sin lanzar (el hard-delete local ya se aplicó).
Los identificadores de tabla/columna son constantes de configuración (no
entran por la API), así que interpolarlos no es vector de inyección; los
valores (``document_id``, sha) van siempre como bind params.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from infrastructure.database.session_factory import SessionFactory

logger = logging.getLogger(__name__)

_TABLA_DEFECTO = "workflow_runs"
_COL_DOC_DEFECTO = "document_id"
_COL_SHA_DEFECTO = "attachment_sha256"


class WorkflowRunsPurger:
    """Borra de ``workflow_runs`` las filas del documento/adjunto purgado."""

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        tabla: str = _TABLA_DEFECTO,
        col_doc: str = _COL_DOC_DEFECTO,
        col_sha: str = _COL_SHA_DEFECTO,
    ) -> None:
        self._sf = session_factory
        self._tabla = tabla
        self._col_doc = col_doc
        self._col_sha = col_sha

    def purge(self, *, document_id: str, source_sha256: str | None) -> int:
        """Elimina la fila del documento + las del mismo adjunto (contenido).

        Devuelve el nº de filas borradas (0 si no existe la tabla/columnas
        o no había filas). Nunca lanza.
        """
        with self._sf.create_session() as session:
            if session.execute(
                text("SELECT to_regclass(:t)"), {"t": self._tabla}
            ).scalar() is None:
                logger.warning(
                    "[workflow-runs-purger] la tabla %r no existe; no se "
                    "limpia el dedup. Ajusta WorkflowRunsPurger(tabla=...). "
                    "doc=%s",
                    self._tabla, document_id,
                )
                return 0

            columnas = {
                fila[0]
                for fila in session.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = :t"
                    ),
                    {"t": self._tabla},
                ).fetchall()
            }
            if self._col_doc not in columnas:
                logger.warning(
                    "[workflow-runs-purger] columna %r ausente en %r; no se "
                    "limpia. Ajusta WorkflowRunsPurger(col_doc=...). doc=%s",
                    self._col_doc, self._tabla, document_id,
                )
                return 0
            tiene_sha = self._col_sha in columnas

            # 1) Resolver el attachment_sha256 REAL por document_id (fuente
            #    de verdad para el dedup por contenido). Fallback: el sha
            #    que pasó sv4.
            att = source_sha256
            if tiene_sha:
                fila = session.execute(
                    text(
                        f"SELECT {self._col_sha} FROM {self._tabla} "
                        f"WHERE {self._col_doc} = :doc "
                        f"AND {self._col_sha} IS NOT NULL LIMIT 1"
                    ),
                    {"doc": document_id},
                ).first()
                if fila and fila[0]:
                    att = fila[0]

            # 2) Borrado: por document_id siempre; por attachment_sha256 si
            #    lo tenemos (desbloquea el PDF completo en multipágina).
            if tiene_sha and att:
                resultado = session.execute(
                    text(
                        f"DELETE FROM {self._tabla} "
                        f"WHERE {self._col_doc} = :doc "
                        f"OR {self._col_sha} = :att"
                    ),
                    {"doc": document_id, "att": att},
                )
            else:
                resultado = session.execute(
                    text(
                        f"DELETE FROM {self._tabla} "
                        f"WHERE {self._col_doc} = :doc"
                    ),
                    {"doc": document_id},
                )
            session.commit()
            borradas = resultado.rowcount or 0
            logger.info(
                "[workflow-runs-purger] borradas %s filas de %s "
                "(doc=%s att=%s)",
                borradas, self._tabla, document_id,
                (att[:12] + "…") if att else None,
            )
            return borradas
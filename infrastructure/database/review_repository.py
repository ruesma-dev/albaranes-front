# infrastructure/database/review_repository.py
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import (
    bindparam,
    delete,
    func,
    inspect,
    nullslast,
    or_,
    select,
    text,
)

from domain.models.review_models import (
    ConciliacionDisplay,
    ConciliacionSibling,
    ContratoLinePayload,
    ContratoPayload,
    DisplayLine,
    DocumentDetailPayload,
    DocumentListFilters,
    DocumentListItem,
    KNOWN_PROVIDER_VIEWS,
    LineValuationPayload,
    MergeDocumentUpdatePayload,
    MergeLinePayload,
    ObraOption,
    PaginatedDocuments,
    ProveedorOption,
    ProviderSnapshot,
    ValuationLineUpdate,
    ValuationPayload,
    VIEW_MODE_MERGE,
    normalize_view_mode,
)
from infrastructure.database.orm_models import (
    AlbaranContratoLineMergeOrm,
    AlbaranContratoMergeOrm,
    AlbaranDocumentBaseOrm,
    AlbaranDocumentMergeOrm,
    AlbaranLineBaseOrm,
    AlbaranLineMergeOrm,
)
from domain.services.confianza import compute_confianza_pct
from infrastructure.database.session_factory import SessionFactory


class AlbaranReviewRepository:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory
        self._initialized = False

    def initialize(self) -> bool:
        """Inicialización idempotente y robusta.

        Ejecuta los DDLs de compatibilidad del servicio 4 (añade
        columnas ``approved``, ``approved_at_utc``, etc., crea índices,
        etc.) sobre las tablas que ya creó el servicio 3.

        IMPORTANTE: sólo marcamos ``self._initialized = True`` CUANDO
        los DDLs se ejecutan con éxito. Si las tablas del servicio 3
        todavía no existen cuando arrancamos, devolvemos False y
        dejamos ``_initialized = False`` para reintentar en la próxima
        llamada (la primera GET /documents, por ejemplo).

        Antes, si el svc4 arrancaba antes que el svc3 creara las
        tablas, marcábamos initialized=True sin haber ejecutado nada,
        y cuando el svc3 ya había creado las tablas después, el svc4
        nunca llegaba a añadir sus columnas → la primera query cascaba
        por 'no existe columna approved'.
        """
        if self._initialized and self._tables_ready():
            return True

        # Rename legacy tables si existieran.
        self._rename_legacy_tables_if_needed()

        # Si las tablas del servicio 3 todavía no existen, no podemos
        # aplicar los ALTER. Devolvemos False (no cacheamos) para que
        # la próxima llamada reintente.
        if not self._tables_ready():
            return False

        with self._session_factory.create_session() as session:
            for ddl in self._review_schema_statements():
                session.execute(text(ddl))
            session.commit()

        self._initialized = True
        return True

    def _tables_ready(self) -> bool:
        inspector = inspect(self._session_factory.engine)
        return inspector.has_table("albaran_documents_merge") and inspector.has_table(
            "albaran_lines_merge"
        )

    def _rename_legacy_tables_if_needed(self) -> None:
        inspector = inspect(self._session_factory.engine)
        has_merge_docs = inspector.has_table("albaran_documents_merge")
        has_merge_lines = inspector.has_table("albaran_lines_merge")
        has_gem_docs = inspector.has_table("albaran_documents_gem")
        has_gem_lines = inspector.has_table("albaran_lines_gem")

        if (not has_merge_docs) and has_gem_docs:
            with self._session_factory.engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE albaran_documents_gem RENAME TO albaran_documents_merge")
                )
        if (not has_merge_lines) and has_gem_lines:
            with self._session_factory.engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE albaran_lines_gem RENAME TO albaran_lines_merge")
                )

    @staticmethod
    def _review_schema_statements() -> list[str]:
        return [
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS approved BOOLEAN",
            "UPDATE albaran_documents_merge SET approved = FALSE WHERE approved IS NULL",
            "ALTER TABLE albaran_documents_merge ALTER COLUMN approved SET DEFAULT FALSE",
            "ALTER TABLE albaran_documents_merge ALTER COLUMN approved SET NOT NULL",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS approved_at_utc VARCHAR(64)",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS approved_by VARCHAR(255)",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS reviewed_at_utc VARCHAR(64)",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS last_modified_at_utc VARCHAR(64)",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS review_notes TEXT",
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS selected_contrato_codigo VARCHAR(64)",
            "ALTER TABLE albaran_contratos_merge ADD COLUMN IF NOT EXISTS gra_rep_ide INTEGER",
            (
                "ALTER TABLE albaran_contratos_merge "
                "ADD COLUMN IF NOT EXISTS pdf_sharepoint_relative_path VARCHAR(1024)"
            ),
            (
                "ALTER TABLE albaran_contratos_merge "
                "ADD COLUMN IF NOT EXISTS pdf_sharepoint_web_url VARCHAR(1024)"
            ),
            """
            CREATE TABLE IF NOT EXISTS albaran_contrato_lines_merge (
                id                     SERIAL PRIMARY KEY,
                contrato_id            INTEGER NOT NULL
                    REFERENCES albaran_contratos_merge(id) ON DELETE CASCADE,
                codigo_contrato        VARCHAR(64) NOT NULL,
                linea                  INTEGER,
                numero_linea           INTEGER,
                codigo_producto        VARCHAR(64),
                codigo_alternativo     VARCHAR(64),
                unidad_medida          VARCHAR(32),
                descripcion_linea      TEXT,
                uds                    DOUBLE PRECISION,
                cantidad_servida       DOUBLE PRECISION,
                cantidad_facturada     DOUBLE PRECISION,
                pendiente_servir       DOUBLE PRECISION,
                precio_unitario        DOUBLE PRECISION,
                precio_bruto           DOUBLE PRECISION,
                descuentos             DOUBLE PRECISION,
                importe_linea          DOUBLE PRECISION,
                cuota_iva              DOUBLE PRECISION,
                doc_origen             VARCHAR(64),
                codigo_partida         VARCHAR(64),
                descripcion_partida    TEXT,
                fetched_at_utc         VARCHAR(64) NOT NULL
            )
            """,
            (
                "ALTER TABLE albaran_contrato_lines_merge "
                "ADD COLUMN IF NOT EXISTS codigo_partida VARCHAR(64)"
            ),
            (
                "ALTER TABLE albaran_contrato_lines_merge "
                "ADD COLUMN IF NOT EXISTS descripcion_partida TEXT"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_albaran_contrato_lines_merge_contrato_id "
                "ON albaran_contrato_lines_merge (contrato_id)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_albaran_contrato_lines_merge_codigo "
                "ON albaran_contrato_lines_merge (codigo_contrato)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_albaran_contrato_lines_merge_partida "
                "ON albaran_contrato_lines_merge (codigo_partida)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_albaran_documents_merge_approved "
                "ON albaran_documents_merge (approved)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_albaran_documents_merge_conf_calc "
                "ON albaran_documents_merge (confidence_pct_calc)"
            ),
        ]

    def list_documents(self, filters: DocumentListFilters) -> PaginatedDocuments:
        self.initialize()
        if not self._tables_ready():
            return PaginatedDocuments(
                items=[],
                total=0,
                page=filters.page,
                page_size=filters.page_size,
                total_pages=0,
                approved_count=0,
                pending_count=0,
                review_required_count=0,
            )

        with self._session_factory.create_session() as session:
            stmt = select(AlbaranDocumentMergeOrm)
            stmt = self._apply_filters(stmt=stmt, filters=filters)
            total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
            stmt = self._apply_sort(stmt=stmt, filters=filters)
            stmt = stmt.offset((filters.page - 1) * filters.page_size).limit(filters.page_size)
            rows = session.scalars(stmt).all()

            # Recalcular la confianza determinista de los documentos de esta
            # pagina que tengan valoracion, para que la LISTA muestre la
            # confianza de VALORACION (no la de extraccion IA) en cuanto el
            # pipeline valora, sin necesidad de abrir el documento.
            for _row in rows:
                try:
                    self._compute_and_persist_confianza(
                        session=session, merge_doc=_row
                    )
                except Exception:
                    # Best-effort: un documento problematico no debe tumbar
                    # el listado; conserva el valor previo.
                    pass

            approved_count = session.scalar(
                select(func.count()).select_from(AlbaranDocumentMergeOrm).where(
                    AlbaranDocumentMergeOrm.approved.is_(True)
                )
            ) or 0
            pending_count = session.scalar(
                select(func.count()).select_from(AlbaranDocumentMergeOrm).where(
                    AlbaranDocumentMergeOrm.approved.is_(False)
                )
            ) or 0
            review_required_count = session.scalar(
                select(func.count()).select_from(AlbaranDocumentMergeOrm).where(
                    AlbaranDocumentMergeOrm.review_required.is_(True)
                )
            ) or 0

            # Columnas nuevas de la lista: importe total valorado (de la
            # valoracion, por document_id) y nombre del contrato seleccionado
            # (por codigo, busqueda compartida). Dos consultas batch.
            doc_ids = [r.id for r in rows]
            totals_by_doc: dict[str, float | None] = {}
            names_by_code: dict[str, str | None] = {}
            if doc_ids:
                try:
                    for vr in session.execute(
                        text(
                            "SELECT document_id, total_valorado "
                            "FROM albaran_valuations "
                            "WHERE document_id IN :ids"
                        ).bindparams(bindparam("ids", expanding=True)),
                        {"ids": doc_ids},
                    ).mappings():
                        totals_by_doc[vr["document_id"]] = vr["total_valorado"]
                except Exception:
                    session.rollback()
                codes = sorted({
                    r.selected_contrato_codigo for r in rows
                    if getattr(r, "selected_contrato_codigo", None)
                })
                if codes:
                    try:
                        for cr in session.execute(
                            text(
                                "SELECT codigo_contrato, nombre_contrato "
                                "FROM albaran_contratos_merge "
                                "WHERE codigo_contrato IN :codes"
                            ).bindparams(bindparam("codes", expanding=True)),
                            {"codes": codes},
                        ).mappings():
                            names_by_code[cr["codigo_contrato"]] = (
                                cr["nombre_contrato"]
                            )
                    except Exception:
                        session.rollback()

        items = [
            self._to_list_item(
                row,
                total_valorado=totals_by_doc.get(row.id),
                contrato_codigo=getattr(row, "selected_contrato_codigo", None),
                contrato_nombre=names_by_code.get(
                    getattr(row, "selected_contrato_codigo", None)
                ),
            )
            for row in rows
        ]
        total_pages = math.ceil(total / filters.page_size) if total else 0
        return PaginatedDocuments(
            items=items,
            total=int(total),
            page=filters.page,
            page_size=filters.page_size,
            total_pages=total_pages,
            approved_count=int(approved_count),
            pending_count=int(pending_count),
            review_required_count=int(review_required_count),
        )

    def _compute_and_persist_confianza(
        self,
        *,
        session,
        merge_doc,
        valuation=None,
        conc_map=None,
    ):
        """Confianza de valoracion (determinista) de un documento.

        Reemplaza la confianza de extraccion (IA). Considera TODAS las
        lineas valoradas (base + complementarias + sinteticas) y la
        resolucion de cabecera (obra/proveedor + su origen). Persiste en
        ``confidence_pct_calc`` (lo que lee la lista). Si el documento aun
        no tiene valoracion devuelve None y NO toca el valor existente.
        Carga valoracion/mapa si no se le pasan (para reusar desde el
        detalle, que ya los tiene). Devuelve el porcentaje (o None).
        """
        if valuation is None:
            valuation = self._load_valuation_in_session(
                session=session, document_id=merge_doc.id
            )
        if valuation is None:
            return None
        if conc_map is None:
            conc_map = self._load_conciliation_map_in_session(
                session=session, valuation=valuation
            )

        conciliaciones = []
        for _vline in valuation.lines_by_merge_line_id.values():
            _vid = _vline.valuation_line_id
            conciliaciones.append(
                conc_map.get(_vid) if _vid is not None else None
            )
        for _syn in valuation.synthetic_lines:
            _vid = _syn.valuation_line_id
            conciliaciones.append(
                conc_map.get(_vid) if _vid is not None else None
            )

        confianza = compute_confianza_pct(
            obra_codigo=merge_doc.obra_codigo,
            obra_codigo_origen=getattr(merge_doc, "obra_codigo_origen", None),
            proveedor_cif=merge_doc.proveedor_cif,
            proveedor_cif_origen=getattr(
                merge_doc, "proveedor_cif_origen", None
            ),
            conciliaciones=conciliaciones,
        )
        if merge_doc.confidence_pct_calc != confianza:
            merge_doc.confidence_pct_calc = confianza
            session.commit()
        return confianza

    def get_document_detail(
        self,
        document_id: str,
        *,
        view_mode: str = VIEW_MODE_MERGE,
    ) -> DocumentDetailPayload | None:
        self.initialize()
        if not self._tables_ready():
            return None

        normalized_view = normalize_view_mode(view_mode)

        with self._session_factory.create_session() as session:
            merge_doc = session.get(AlbaranDocumentMergeOrm, document_id)
            if merge_doc is None:
                return None
            _ = merge_doc.lines  # eager-load

            contratos_orm = session.scalars(
                select(AlbaranContratoMergeOrm)
                .where(AlbaranContratoMergeOrm.document_id == merge_doc.id)
                .order_by(AlbaranContratoMergeOrm.codigo_contrato.asc())
            ).all()

            selected_contrato_codigo = getattr(
                merge_doc, "selected_contrato_codigo", None
            )

            # Fix (jun 2026): la cabecera de contrato es ÚNICA por
            # ``sigrid_ide`` en ``albaran_contratos_merge`` y su
            # ``document_id`` se sobrescribe con el del ÚLTIMO albarán
            # enriquecido (UPSERT por sigrid_ide en sv3). Cuando varios
            # albaranes comparten el mismo contrato, el panel de los que
            # NO son "dueños" de la fila veía 0 cabeceras (su document_id
            # ya no figura en la fila compartida) y mostraba el falso aviso
            # "no se encontró contrato" — aunque la valoración SÍ lo usó.
            #
            # Recuperamos la cabecera por ``codigo_contrato`` a partir del
            # contrato seleccionado del documento, que es la fuente de
            # verdad de "qué contrato usa este albarán". Solo entramos aquí
            # si no hay ninguna cabecera por document_id (no alteramos el
            # caso normal ni el de varios contratos).
            if not contratos_orm and selected_contrato_codigo:
                contratos_orm = session.scalars(
                    select(AlbaranContratoMergeOrm)
                    .where(
                        AlbaranContratoMergeOrm.codigo_contrato
                        == selected_contrato_codigo
                    )
                    .order_by(AlbaranContratoMergeOrm.codigo_contrato.asc())
                ).all()

            # Dedup por codigo_contrato (#1): Sigrid puede tener el mismo
            # contrato con varios ``sigrid_ide`` (se recrea al modificarlo),
            # lo que deja filas duplicadas en ``albaran_contratos_merge``
            # para el mismo documento. El panel y el desplegable deben
            # mostrar UNO por codigo. Conservamos la primera aparicion.
            _contratos_all = [
                self._contrato_orm_to_payload(item) for item in contratos_orm
            ]
            _seen_cod: set[str] = set()
            contratos_payload = []
            for _cp in _contratos_all:
                if _cp.codigo_contrato in _seen_cod:
                    continue
                _seen_cod.add(_cp.codigo_contrato)
                contratos_payload.append(_cp)

            # Lineas del contrato seleccionado, para el desplegable de
            # conciliacion editable del front (elegir otra linea o nueva).
            _lines_raw = [
                ContratoLinePayload(
                    id=int(r["id"]),
                    codigo_contrato=r.get("codigo_contrato") or "",
                    codigo_partida=r.get("codigo_partida"),
                    descripcion=r.get("descripcion_linea"),
                    precio_unitario=r.get("precio_unitario"),
                    unidad_medida=r.get("unidad_medida"),
                    codigo_producto=r.get("codigo_producto"),
                )
                for r in self._fetch_contrato_lines_for_codigo_in_session(
                    session, selected_contrato_codigo
                )
            ]
            # Dedup de opciones identicas (misma partida+descripcion+precio+
            # unidad): si habia contratos duplicados con sus lineas repetidas,
            # el desplegable no debe mostrar la misma opcion dos veces.
            _seen_ln: set[tuple] = set()
            contrato_lines_payload = []
            for _ln in _lines_raw:
                _key = (
                    _ln.codigo_partida,
                    _ln.descripcion,
                    _ln.precio_unitario,
                    _ln.unidad_medida,
                )
                if _key in _seen_ln:
                    continue
                _seen_ln.add(_key)
                contrato_lines_payload.append(_ln)

            provider_docs = session.scalars(
                select(AlbaranDocumentBaseOrm)
                .where(AlbaranDocumentBaseOrm.source_sha256 == merge_doc.source_sha256)
                .order_by(AlbaranDocumentBaseOrm.created_at_utc.asc())
            ).all()

            seen_providers = {doc.provider_origin for doc in provider_docs}
            available_views: list[str] = [VIEW_MODE_MERGE]
            for provider in KNOWN_PROVIDER_VIEWS:
                if provider in seen_providers:
                    available_views.append(provider)
            for provider in sorted(seen_providers):
                if provider not in available_views:
                    available_views.append(provider)

            if (
                normalized_view != VIEW_MODE_MERGE
                and normalized_view not in seen_providers
            ):
                normalized_view = VIEW_MODE_MERGE

            provider_snapshots_payload = [
                ProviderSnapshot(
                    id=item.id,
                    provider_origin=item.provider_origin,
                    model_name=item.model_name,
                    proveedor_nombre=item.proveedor_nombre,
                    fecha=item.fecha,
                    numero_albaran=item.numero_albaran,
                    obra_codigo=item.obra_codigo,
                    raw_extraction_json=item.raw_extraction_json,
                    ia_output_json=item.ia_output_json,
                )
                for item in provider_docs
            ]

            # Los desplegables de cabecera (proveedor / obra) ya NO se
            # rellenan desde la BBDD local (solo tenia los contratos ya
            # ingeridos). Ahora el front los pide BAJO DEMANDA a Sigrid via
            # los endpoints /api/sigrid/* (ver app.py). Dejamos las listas
            # vacias aqui.
            proveedores_opts, obras_opts = [], []

            # NUEVO: leer valoración (si existe) en la misma sesión.
            valuation_payload = self._load_valuation_in_session(
                session=session,
                document_id=merge_doc.id,
            )

            # NUEVO: cargar el mapa de conciliación (qué línea de
            # contrato — sigrid o derivada — casó con cada línea del
            # albarán) para mostrar inline en sv4. None si no hay
            # valoración o ninguna línea tiene matched/derived.
            conciliation_by_valuation_line_id = (
                self._load_conciliation_map_in_session(
                    session=session,
                    valuation=valuation_payload,
                )
                if valuation_payload is not None
                else {}
            )

            # CONFIANZA de valoracion (determinista; reemplaza la de IA).
            # Se recalcula sobre el estado actual y se persiste en
            # confidence_pct_calc (lo que lee el badge y la lista). Reusa la
            # valoracion y el mapa ya cargados arriba.
            self._compute_and_persist_confianza(
                session=session,
                merge_doc=merge_doc,
                valuation=valuation_payload,
                conc_map=conciliation_by_valuation_line_id,
            )

            if normalized_view == VIEW_MODE_MERGE:
                return self._build_merge_detail(
                    merge_doc=merge_doc,
                    available_views=available_views,
                    provider_snapshots=provider_snapshots_payload,
                    contratos=contratos_payload,
                    contrato_lines=contrato_lines_payload,
                    proveedores=proveedores_opts,
                    obras=obras_opts,
                    selected_contrato_codigo=selected_contrato_codigo,
                    valuation=valuation_payload,
                    conciliation_by_valuation_line_id=conciliation_by_valuation_line_id,
                )

            provider_doc = next(
                (doc for doc in provider_docs if doc.provider_origin == normalized_view),
                None,
            )
            if provider_doc is None:
                return self._build_merge_detail(
                    merge_doc=merge_doc,
                    available_views=available_views,
                    provider_snapshots=provider_snapshots_payload,
                    contratos=contratos_payload,
                    contrato_lines=contrato_lines_payload,
                    proveedores=proveedores_opts,
                    obras=obras_opts,
                    selected_contrato_codigo=selected_contrato_codigo,
                    valuation=valuation_payload,
                    conciliation_by_valuation_line_id=conciliation_by_valuation_line_id,
                )

            provider_lines = session.scalars(
                select(AlbaranLineBaseOrm)
                .where(AlbaranLineBaseOrm.document_id == provider_doc.id)
                .order_by(AlbaranLineBaseOrm.line_index.asc())
            ).all()

            return self._build_provider_detail(
                merge_doc=merge_doc,
                provider_doc=provider_doc,
                provider_lines=provider_lines,
                available_views=available_views,
                provider_snapshots=provider_snapshots_payload,
                view_mode=normalized_view,
                contratos=contratos_payload,
                selected_contrato_codigo=selected_contrato_codigo,
                valuation=valuation_payload,
            )

    # ------------------------------------------------------------------ #
    # NUEVO: opciones de cabecera (proveedores / obras del proyecto) para
    # los desplegables. DISTINCT sobre TODA la tabla de contratos, dedup
    # por cif (proveedor) y por codigo (obra). Tolerante a tabla ausente.
    # ------------------------------------------------------------------ #
    @staticmethod
    def _load_header_options_in_session(
        session: Any,
    ) -> tuple[list[ProveedorOption], list[ObraOption]]:
        proveedores: list[ProveedorOption] = []
        obras: list[ObraOption] = []
        try:
            prov_rows = session.execute(
                text(
                    "SELECT DISTINCT cif_proveedor, nombre_proveedor "
                    "FROM albaran_contratos_merge "
                    "WHERE cif_proveedor IS NOT NULL AND cif_proveedor <> '' "
                    "ORDER BY nombre_proveedor"
                )
            ).mappings().all()
            obra_rows = session.execute(
                text(
                    "SELECT DISTINCT codigo_obra, nombre_obra "
                    "FROM albaran_contratos_merge "
                    "WHERE codigo_obra IS NOT NULL AND codigo_obra <> '' "
                    "ORDER BY codigo_obra"
                )
            ).mappings().all()
        except Exception:
            session.rollback()
            return [], []

        _seen_cif: set[str] = set()
        for r in prov_rows:
            cif = (r["cif_proveedor"] or "").strip()
            if not cif or cif in _seen_cif:
                continue
            _seen_cif.add(cif)
            nombre = (r["nombre_proveedor"] or "").strip() or None
            proveedores.append(ProveedorOption(cif=cif, nombre=nombre))

        _seen_cod: set[str] = set()
        for r in obra_rows:
            cod = (r["codigo_obra"] or "").strip()
            if not cod or cod in _seen_cod:
                continue
            _seen_cod.add(cod)
            nombre = (r["nombre_obra"] or "").strip() or None
            obras.append(ObraOption(codigo=cod, nombre=nombre))

        return proveedores, obras

    # ------------------------------------------------------------------ #
    # NUEVO: lectura de valoración desde las tablas del servicio 6.
    # SQL crudo para no acoplarnos al ORM del 6 dentro del servicio 4.
    # Si las tablas no existen (p.e. BBDD vieja) devuelve None sin
    # romper la carga del detalle.
    # ------------------------------------------------------------------ #
    def _load_valuation_in_session(
        self,
        *,
        session: Any,
        document_id: str,
    ) -> ValuationPayload | None:
        try:
            header_row = session.execute(
                text(
                    "SELECT id, contrato_codigo, status, provider_ia, "
                    "       model_name, total_valorado, total_lines, "
                    "       lines_matched_exact, lines_matched_semantic, "
                    "       lines_matched_price_only, lines_unmatched, "
                    "       review_required, created_at_utc, updated_at_utc "
                    "FROM albaran_valuations "
                    "WHERE document_id = :doc_id "
                    "LIMIT 1"
                ),
                {"doc_id": document_id},
            ).mappings().first()
        except Exception:
            # Tabla puede no existir (BBDD antigua).
            session.rollback()
            return None

        if header_row is None:
            return None

        valuation_id = str(header_row["id"])

        try:
            line_rows = session.execute(
                text(
                    "SELECT id, "
                    "       merge_line_id, matched_contrato_line_id, "
                    "       derived_contrato_line_id, "
                    "       precio_unitario_contrato_db, "
                    "       precio_unitario_pdf_inferido, "
                    "       precio_unitario_final, precio_unitario_source, "
                    "       precio_unitario_agreement, "
                    "       unidad_albaran, unidad_contrato, "
                    "       unidad_categoria, unidad_category_match, "
                    "       cantidad_albaran, cantidad_convertida, "
                    "       factor_conversion, "
                    "       importe_calculado, importe_albaran_declarado, "
                    "       importe_source, "
                    "       codigo_partida_albaran, codigo_partida_final, "
                    "       partida_action, "
                    "       match_confidence_pct, match_method, "
                    "       review_required, "
                    # ----- sub-tanda 2D: campos de líneas sintéticas -----
                    "       line_kind, parent_merge_line_id, "
                    "       modifier_source, modifier_reason, "
                    "       descripcion_linea "
                    "FROM albaran_line_valuations "
                    "WHERE valuation_id = :vid "
                    # from_albaran primero (para poder iterar base, luego
                    # sintéticas agrupadas por parent en la UI futura).
                    "ORDER BY "
                    "  CASE WHEN line_kind = 'synthetic_modifier' "
                    "       THEN 1 ELSE 0 END, "
                    "  merge_line_id NULLS LAST, "
                    "  id"
                ),
                {"vid": valuation_id},
            ).mappings().all()
        except Exception:
            session.rollback()
            line_rows = []

        # Sub-tanda 2D: las líneas pueden ser 'from_albaran' (con
        # merge_line_id real, van al dict de siempre para que la UI
        # pinte cada línea del albarán con su valoración) o
        # 'synthetic_modifier' (merge_line_id NULL, se acumulan en una
        # lista aparte para que la UI las pinte como bloque adicional).
        #
        # Compatibilidad retroactiva: para valoraciones anteriores a
        # 2D (sin columna line_kind), tratamos NULL/'' como
        # 'from_albaran'. Así los albaranes valorados con la versión
        # anterior siguen funcionando.
        lines_by_id: dict[int, LineValuationPayload] = {}
        synthetic_lines: list[LineValuationPayload] = []
        for row in line_rows:
            raw_merge_line_id = row.get("merge_line_id") if hasattr(
                row, "get"
            ) else row["merge_line_id"]

            raw_line_kind = None
            try:
                raw_line_kind = row["line_kind"]
            except KeyError:
                # Columna no existe (BBDD anterior a 2D) — default.
                raw_line_kind = None
            line_kind = (raw_line_kind or "from_albaran").strip() or "from_albaran"

            # Columnas nuevas de 2D — opcionales, toleramos ausencia.
            try:
                parent_merge_line_id = row["parent_merge_line_id"]
            except KeyError:
                parent_merge_line_id = None
            try:
                modifier_source = row["modifier_source"]
            except KeyError:
                modifier_source = None
            try:
                modifier_reason = row["modifier_reason"]
            except KeyError:
                modifier_reason = None
            try:
                descripcion_linea = row["descripcion_linea"]
            except KeyError:
                descripcion_linea = None

            merge_line_id_typed: int | None
            if raw_merge_line_id is None:
                merge_line_id_typed = None
            else:
                try:
                    merge_line_id_typed = int(raw_merge_line_id)
                except (TypeError, ValueError):
                    merge_line_id_typed = None

            try:
                valuation_line_id = int(row["id"])
            except (TypeError, ValueError, KeyError):
                valuation_line_id = None

            payload = LineValuationPayload(
                valuation_line_id=valuation_line_id,
                merge_line_id=merge_line_id_typed,
                matched_contrato_line_id=row["matched_contrato_line_id"],
                derived_contrato_line_id=row["derived_contrato_line_id"],
                precio_unitario_contrato_db=row["precio_unitario_contrato_db"],
                precio_unitario_pdf_inferido=row["precio_unitario_pdf_inferido"],
                precio_unitario_final=row["precio_unitario_final"],
                precio_unitario_source=row["precio_unitario_source"],
                precio_unitario_agreement=row["precio_unitario_agreement"],
                unidad_albaran=row["unidad_albaran"],
                unidad_contrato=row["unidad_contrato"],
                unidad_categoria=row["unidad_categoria"],
                unidad_category_match=row["unidad_category_match"],
                cantidad_albaran=row["cantidad_albaran"],
                cantidad_convertida=row["cantidad_convertida"],
                factor_conversion=row["factor_conversion"],
                importe_calculado=row["importe_calculado"],
                importe_albaran_declarado=row["importe_albaran_declarado"],
                importe_source=row["importe_source"],
                codigo_partida_albaran=row["codigo_partida_albaran"],
                codigo_partida_final=row["codigo_partida_final"],
                partida_action=row["partida_action"],
                match_confidence_pct=row["match_confidence_pct"],
                match_method=row["match_method"],
                review_required=row["review_required"],
                # --- sub-tanda 2D ---
                line_kind=line_kind,
                parent_merge_line_id=parent_merge_line_id,
                modifier_source=modifier_source,
                modifier_reason=modifier_reason,
                descripcion_linea=descripcion_linea,
            )

            if line_kind == "synthetic_modifier":
                synthetic_lines.append(payload)
            else:
                # from_albaran. Si por la razón que sea merge_line_id es
                # None (BBDD corrupta, dato antiguo raro), no lo metemos
                # en el dict — se añade también a synthetic_lines para
                # que al menos no desaparezca.
                if merge_line_id_typed is None:
                    synthetic_lines.append(payload)
                else:
                    lines_by_id[merge_line_id_typed] = payload

        return ValuationPayload(
            valuation_id=valuation_id,
            contrato_codigo=header_row["contrato_codigo"],
            status=str(header_row["status"]),
            provider_ia=header_row["provider_ia"],
            model_name=header_row["model_name"],
            total_valorado=float(header_row["total_valorado"] or 0.0),
            total_lines=int(header_row["total_lines"] or 0),
            lines_matched_exact=int(header_row["lines_matched_exact"] or 0),
            lines_matched_semantic=int(header_row["lines_matched_semantic"] or 0),
            lines_matched_price_only=int(
                header_row["lines_matched_price_only"] or 0
            ),
            lines_unmatched=int(header_row["lines_unmatched"] or 0),
            review_required=bool(header_row["review_required"]),
            created_at_utc=header_row["created_at_utc"],
            updated_at_utc=header_row["updated_at_utc"],
            lines_by_merge_line_id=lines_by_id,
            synthetic_lines=synthetic_lines,
        )

    # ----------------------------------------------------------------- #
    # Carga de los datos de conciliación que se muestran inline en sv4
    # bajo cada línea del albarán.
    #
    # Para cada línea valorada (LineValuationPayload from_albaran), se
    # busca el bloque de detalle a mostrar:
    #
    #   - Si tiene matched_contrato_line_id (línea cacheada de Sigrid),
    #     leemos albaran_contrato_lines_merge.
    #   - Si tiene derived_contrato_line_id (línea creada por el
    #     valorador), leemos contrato_lines_derived.
    #
    # Las dos consultas son batch (un solo SELECT con IN) para no
    # disparar N+1.
    #
    # Devuelve un dict {merge_line_id -> ConciliacionDisplay}. Líneas
    # del albarán sin valoración (o cuya valoración no tiene match ni
    # derived) no aparecen en el dict; el front no muestra el bloque.
    # ----------------------------------------------------------------- #
    def _load_conciliation_map_in_session(
        self,
        *,
        session: Any,
        valuation: ValuationPayload | None,
    ) -> dict[int, ConciliacionDisplay]:
        """Devuelve ``{valuation_line_id -> ConciliacionDisplay}``.

        Bloque 2 (may 2026): antes se indexaba por ``merge_line_id``, lo
        que DEJABA FUERA a las líneas sintéticas (``merge_line_id`` NULL)
        y por eso solo conciliaba la línea base en el front. Ahora se
        indexa por ``valuation_line_id``, que existe para TODAS las
        líneas valoradas (``from_albaran`` y ``synthetic_modifier``), de
        modo que las sintéticas también muestran su conciliación contra
        Sigrid.

        El indicador verde/rojo (``price_agreement``) se calcula aquí
        comparando el precio de la línea de Sigrid con el precio
        valorado de la línea, para que el revisor vea de un vistazo si
        cuadran.
        """
        if valuation is None:
            return {}

        # Todas las líneas valoradas: base/complementarias + sintéticas.
        all_lines: list[LineValuationPayload] = list(
            valuation.lines_by_merge_line_id.values()
        ) + list(valuation.synthetic_lines)

        # Paso 1: agrupar por tipo (matched / derived) con la salida
        # indexada por valuation_line_id.
        #
        # IMPORTANTE (fix jun 2026 — líneas que se quedaban sin conciliar):
        # la relación línea_de_contrato → líneas_de_valoración es UNO-A-VARIOS,
        # no uno-a-uno. Varias líneas del albarán pueden casar con la MISMA
        # línea de contrato (p.ej. dos niveles láser idénticos con distinto
        # nº de serie casan ambos contra la misma línea de Sigrid; o varias
        # piezas de un mismo sanitario apuntan a la misma partida). Antes
        # indexábamos ``matched_to_vline[contrato_line_id] = vid`` y la
        # segunda línea SOBREESCRIBÍA a la primera: solo la última conservaba
        # su bloque de conciliación y las demás aparecían vacías con el botón
        # "+". Ahora acumulamos TODOS los valuation_line_id que casan con cada
        # línea de contrato para pintarles el bloque a todos.
        matched_to_vlines: dict[int, list[int]] = {}
        derived_to_vlines: dict[int, list[int]] = {}
        precio_final_by_vline: dict[int, float | None] = {}
        vline_by_vid: dict[int, LineValuationPayload] = {}
        for vline in all_lines:
            vid = vline.valuation_line_id
            if vid is None:
                continue
            precio_final_by_vline[vid] = vline.precio_unitario_final
            vline_by_vid[vid] = vline
            if vline.matched_contrato_line_id is not None:
                matched_to_vlines.setdefault(
                    vline.matched_contrato_line_id, []
                ).append(vid)
            elif vline.derived_contrato_line_id is not None:
                derived_to_vlines.setdefault(
                    vline.derived_contrato_line_id, []
                ).append(vid)

        out: dict[int, ConciliacionDisplay] = {}

        # Paso 2: líneas matched (de Sigrid cacheadas) en bloque.
        for row in self._fetch_contrato_lines_in_session(
            session, list(matched_to_vlines.keys())
        ):
            # Todas las líneas de valoración que casaron con ESTA línea de
            # contrato reciben su propio bloque de conciliación (uno-a-varios).
            for vid in matched_to_vlines.get(row["id"], []):
                unitario = row.get("precio_unitario")
                precio_final = precio_final_by_vline.get(vid)
                out[vid] = ConciliacionDisplay(
                    kind="assigned",
                    descripcion=row.get("descripcion_linea"),
                    unitario=unitario,
                    medicion_total=row.get("uds"),
                    medicion_pendiente=row.get("pendiente_servir"),
                    unidad=row.get("unidad_medida"),
                    codigo_partida=row.get("codigo_partida"),
                    descripcion_partida=row.get("descripcion_partida"),
                    price_agreement=self._price_agreement(unitario, precio_final),
                    precio_unitario_final=precio_final,
                    sibling=None,
                    match_method=getattr(vline_by_vid.get(vid), "match_method", None),
                    match_confidence_pct=getattr(
                        vline_by_vid.get(vid), "match_confidence_pct", None
                    ),
                    precio_unitario_source=getattr(
                        vline_by_vid.get(vid), "precio_unitario_source", None
                    ),
                )

        # Paso 3: líneas derived (creadas por el valorador) en bloque.
        for row in self._fetch_derived_lines_in_session(
            session, list(derived_to_vlines.keys())
        ):
            for vid in derived_to_vlines.get(row["id"], []):
                unitario = row.get("precio_unitario")
                precio_final = precio_final_by_vline.get(vid)
                out[vid] = ConciliacionDisplay(
                    kind="derived",
                    descripcion=row.get("descripcion_linea"),
                    unitario=unitario,
                    medicion_total=row.get("uds"),
                    medicion_pendiente=None,  # derived: no hay pendiente
                    unidad=row.get("unidad_medida"),
                    codigo_partida=row.get("codigo_partida"),
                    descripcion_partida=row.get("descripcion_partida"),
                    price_agreement=self._price_agreement(unitario, precio_final),
                    precio_unitario_final=precio_final,
                    sibling=None,
                    match_method=getattr(vline_by_vid.get(vid), "match_method", None),
                    match_confidence_pct=getattr(
                        vline_by_vid.get(vid), "match_confidence_pct", None
                    ),
                    precio_unitario_source=getattr(
                        vline_by_vid.get(vid), "precio_unitario_source", None
                    ),
                    derived_origen=row.get("origen"),
                )

        return out

    @staticmethod
    def _fetch_contrato_lines_in_session(session: Any, ids: list[int]):
        """Carga en bloque líneas de ``albaran_contrato_lines_merge`` por
        id, con fallback a ``IN (...)`` si la BBDD no soporta ``ANY``.
        """
        if not ids:
            return []
        base_sql = (
            "SELECT id, descripcion_linea, unidad_medida, precio_unitario, "
            "       uds, pendiente_servir, codigo_partida, descripcion_partida "
            "FROM albaran_contrato_lines_merge "
        )
        try:
            return session.execute(
                text(base_sql + "WHERE id = ANY(:ids)"), {"ids": ids}
            ).mappings().all()
        except Exception:
            session.rollback()
            placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
            params = {f"id_{i}": v for i, v in enumerate(ids)}
            return session.execute(
                text(base_sql + f"WHERE id IN ({placeholders})"), params
            ).mappings().all()

    @staticmethod
    def _fetch_derived_lines_in_session(session: Any, ids: list[int]):
        """Carga en bloque líneas de ``contrato_lines_derived`` por id.
        Tolera que la tabla no exista (BBDD anterior a sv6 con derived).

        OJO: el esquema real de ``contrato_lines_derived`` (ver sv6
        ``schema_contribution``) NO tiene ``uds`` ni ``descripcion_partida``;
        solo ``codigo_partida`` y ``unidad_medida``. Seleccionar columnas
        inexistentes hacía petar la query en silencio y la conciliación de
        las líneas derivadas no aparecía.
        """
        if not ids:
            return []
        base_sql = (
            "SELECT id, descripcion_linea, unidad_medida, precio_unitario, "
            "       codigo_partida, origen "
            "FROM contrato_lines_derived "
        )
        try:
            return session.execute(
                text(base_sql + "WHERE id = ANY(:ids)"), {"ids": ids}
            ).mappings().all()
        except Exception:
            session.rollback()
            placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
            params = {f"id_{i}": v for i, v in enumerate(ids)}
            try:
                return session.execute(
                    text(base_sql + f"WHERE id IN ({placeholders})"), params
                ).mappings().all()
            except Exception:
                session.rollback()
                return []

    @staticmethod
    def _price_agreement(
        unitario: float | None,
        precio_final: float | None,
    ) -> str | None:
        """'agree' si el precio de Sigrid y el valorado cuadran (±1%),
        'disagree' si difieren, None si falta alguno.
        """
        if unitario is None or precio_final is None:
            return None
        try:
            a = float(unitario)
            b = float(precio_final)
        except (TypeError, ValueError):
            return None
        tol = max(0.01, 0.01 * max(abs(a), abs(b)))
        return "agree" if abs(a - b) <= tol else "disagree"

    @staticmethod
    def _fetch_contrato_lines_for_codigo_in_session(
        session: Any, codigo_contrato: str | None
    ):
        """Todas las lineas de albaran_contrato_lines_merge de un contrato,
        para poblar el desplegable de conciliacion editable. Tolerante a
        que la tabla no exista (BBDD antigua)."""
        if not codigo_contrato:
            return []
        sql = (
            "SELECT id, codigo_contrato, codigo_partida, descripcion_linea, "
            "       precio_unitario, unidad_medida, codigo_producto "
            "FROM albaran_contrato_lines_merge "
            "WHERE codigo_contrato = :cod "
            "ORDER BY codigo_partida NULLS LAST, descripcion_linea"
        )
        try:
            return session.execute(
                text(sql), {"cod": codigo_contrato}
            ).mappings().all()
        except Exception:
            session.rollback()
            return []

    def set_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
        mode: str,
        matched_contrato_line_id: int | None = None,
        descripcion: str | None = None,
        precio_unitario: float | None = None,
    ) -> bool:
        """Override manual de la conciliacion de UNA linea de valoracion.

        mode='contract_line': apunta la linea a matched_contrato_line_id
          (linea de albaran_contrato_lines_merge), trae su precio y
          recalcula el importe. Limpia la derivada previa.
        mode='nueva': crea una linea en contrato_lines_derived en la
          partida de la linea (descripcion/precio por defecto los de la
          propia linea), la enlaza y recalcula el importe. Limpia el match.

        NO revalora: solo toca esta linea y el total de la cabecera. La
        correccion se pierde si luego se pulsa "Valorar ahora" (reemplazo
        completo). Devuelve True si actualizo algo.
        """
        self.initialize()
        with self._session_factory.create_session() as session:
            row = session.execute(
                text(
                    "SELECT lv.id, lv.merge_line_id, lv.valuation_id, "
                    "       lv.cantidad_albaran, lv.cantidad_convertida, "
                    "       lv.codigo_partida_final, lv.descripcion_linea, "
                    "       lv.precio_unitario_final, v.document_id, "
                    "       v.contrato_codigo "
                    "FROM albaran_line_valuations lv "
                    "JOIN albaran_valuations v ON v.id = lv.valuation_id "
                    "WHERE lv.id = :vid"
                ),
                {"vid": valuation_line_id},
            ).mappings().first()
            if row is None or row["document_id"] != document_id:
                return False

            valuation_id = row["valuation_id"]
            cantidad = (
                row["cantidad_convertida"]
                if row["cantidad_convertida"] is not None
                else row["cantidad_albaran"]
            )

            def _importe(precio):
                if precio is None or cantidad is None:
                    return None
                return round(float(precio) * float(cantidad), 2)

            if mode == "contract_line":
                if matched_contrato_line_id is None:
                    return False
                cl = session.execute(
                    text(
                        "SELECT precio_unitario "
                        "FROM albaran_contrato_lines_merge WHERE id = :id"
                    ),
                    {"id": matched_contrato_line_id},
                ).mappings().first()
                if cl is None:
                    return False
                precio = (
                    precio_unitario
                    if precio_unitario is not None
                    else cl["precio_unitario"]
                )
                session.execute(
                    text(
                        "UPDATE albaran_line_valuations SET "
                        "  matched_contrato_line_id = :mid, "
                        "  derived_contrato_line_id = NULL, "
                        "  precio_unitario_final = :pu, "
                        "  precio_unitario_source = 'manual_contract', "
                        "  precio_unitario_agreement = 'manual', "
                        "  importe_calculado = :imp, "
                        "  importe_source = 'calculated', "
                        "  review_required = FALSE "
                        "WHERE id = :vid"
                    ),
                    {
                        "mid": matched_contrato_line_id,
                        "pu": precio,
                        "imp": _importe(precio),
                        "vid": valuation_line_id,
                    },
                )
            elif mode == "nueva":
                # "Nueva" COPIA de la linea blanca (merge), no de la
                # valoracion: concepto y precio declarados del albaran como
                # defaults. (Para el total cuenta esta linea salmon.)
                ml_nueva = None
                if row["merge_line_id"] is not None:
                    ml_nueva = session.execute(
                        text(
                            "SELECT concepto, precio "
                            "FROM albaran_lines_merge WHERE id = :mid"
                        ),
                        {"mid": row["merge_line_id"]},
                    ).mappings().first()
                precio = (
                    precio_unitario
                    if precio_unitario is not None
                    else (
                        ml_nueva["precio"]
                        if ml_nueva and ml_nueva["precio"] is not None
                        else row["precio_unitario_final"]
                    )
                )
                desc = descripcion or (
                    ml_nueva["concepto"]
                    if ml_nueva and ml_nueva["concepto"]
                    else row["descripcion_linea"]
                )
                new_id = session.execute(
                    text(
                        "INSERT INTO contrato_lines_derived ("
                        "  created_by_valuation_id, source_document_id, "
                        "  codigo_contrato, codigo_producto, descripcion_linea, "
                        "  unidad_medida, precio_unitario, codigo_partida, "
                        "  origen, created_at_utc) "
                        "VALUES (:vid, :doc, :cod, NULL, :desc, NULL, :pu, "
                        "  :part, 'manual_override', :now) "
                        "RETURNING id"
                    ),
                    {
                        "vid": valuation_id,
                        "doc": document_id,
                        "cod": row["contrato_codigo"] or "",
                        "desc": desc,
                        "pu": precio,
                        "part": row["codigo_partida_final"],
                        "now": self._utc_iso(),
                    },
                ).scalar_one()
                session.execute(
                    text(
                        "UPDATE albaran_line_valuations SET "
                        "  matched_contrato_line_id = NULL, "
                        "  derived_contrato_line_id = :did, "
                        "  precio_unitario_final = :pu, "
                        "  precio_unitario_source = 'manual_derived', "
                        "  precio_unitario_agreement = 'manual', "
                        "  importe_calculado = :imp, "
                        "  importe_source = 'calculated', "
                        "  review_required = FALSE "
                        "WHERE id = :vid"
                    ),
                    {
                        "did": new_id,
                        "pu": precio,
                        "imp": _importe(precio),
                        "vid": valuation_line_id,
                    },
                )
            else:
                return False

            # Recalcular total de la cabecera.
            session.execute(
                text(
                    "UPDATE albaran_valuations SET "
                    "  total_valorado = COALESCE((SELECT SUM(importe_calculado) "
                    "    FROM albaran_line_valuations WHERE valuation_id = :vid), 0), "
                    "  updated_at_utc = :now "
                    "WHERE id = :vid"
                ),
                {"vid": valuation_id, "now": self._utc_iso()},
            )
            session.commit()
            return True

    def remove_line_valuation(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
    ) -> bool:
        """Borra DEFINITIVAMENTE la linea de valoracion (la fila salmon).

        La linea blanca (merge) NO se toca: sigue mostrandose y reaparece
        el boton '+' para volver a traer una conciliacion si se quiere.
        La linea borrada deja de contar en el total. Si tenia una linea
        derivada ('nueva'), tambien se limpia. Devuelve True si borro.
        """
        self.initialize()
        with self._session_factory.create_session() as session:
            row = session.execute(
                text(
                    "SELECT lv.id, lv.valuation_id, "
                    "       lv.derived_contrato_line_id, v.document_id "
                    "FROM albaran_line_valuations lv "
                    "JOIN albaran_valuations v ON v.id = lv.valuation_id "
                    "WHERE lv.id = :vid"
                ),
                {"vid": valuation_line_id},
            ).mappings().first()
            if row is None or row["document_id"] != document_id:
                return False

            valuation_id = row["valuation_id"]
            derived_id = row["derived_contrato_line_id"]

            session.execute(
                text("DELETE FROM albaran_line_valuations WHERE id = :vid"),
                {"vid": valuation_line_id},
            )
            if derived_id is not None:
                session.execute(
                    text("DELETE FROM contrato_lines_derived WHERE id = :did"),
                    {"did": derived_id},
                )

            # Recalcular total de la cabecera (la linea borrada ya no suma).
            session.execute(
                text(
                    "UPDATE albaran_valuations SET "
                    "  total_valorado = COALESCE((SELECT SUM(importe_calculado) "
                    "    FROM albaran_line_valuations WHERE valuation_id = :vid), 0), "
                    "  updated_at_utc = :now "
                    "WHERE id = :vid"
                ),
                {"vid": valuation_id, "now": self._utc_iso()},
            )
            session.commit()
            return True

    def add_conciliacion_for_merge_line(
        self,
        *,
        document_id: str,
        merge_line_id: int,
        mode: str = "contract_line",
        matched_contrato_line_id: int | None = None,
        precio_unitario: float | None = None,
        descripcion: str | None = None,
    ) -> bool:
        """Trae una conciliacion a una linea de albaran SIN valoracion.

        mode='contract_line': apunta la linea a ``matched_contrato_line_id``
          (linea de albaran_contrato_lines_merge), trae su precio. Salmon
          "Sigrid".
        mode='nueva': crea una linea DERIVADA copiando la BLANCA del albaran
          (concepto/precio/partida declarados) -> salmon "Nueva", igual que
          el modo "nueva" del badge de edicion.

        Crea la cabecera de valoracion si el documento aun no la tiene. NO
        revalora: la correccion se pierde si luego se pulsa "Valorar ahora".
        Devuelve True si ok.
        """
        import uuid

        self.initialize()
        with self._session_factory.create_session() as session:
            ml = session.execute(
                text(
                    "SELECT id, document_id, cantidad, concepto, "
                    "       codigo_imputacion, precio "
                    "FROM albaran_lines_merge WHERE id = :id"
                ),
                {"id": merge_line_id},
            ).mappings().first()
            if ml is None or ml["document_id"] != document_id:
                return False

            now = self._utc_iso()

            # Cabecera de valoracion (crear si no existe). contrato_codigo
            # se usa como codigo_contrato de la derivada en modo "nueva".
            hdr = session.execute(
                text(
                    "SELECT id, contrato_codigo FROM albaran_valuations "
                    "WHERE document_id = :doc"
                ),
                {"doc": document_id},
            ).mappings().first()
            if hdr is None:
                valuation_id = str(uuid.uuid4())
                contrato_codigo = None
                session.execute(
                    text(
                        "INSERT INTO albaran_valuations "
                        "  (id, document_id, status, created_at_utc) "
                        "VALUES (:id, :doc, 'manual', :now)"
                    ),
                    {"id": valuation_id, "doc": document_id, "now": now},
                )
            else:
                valuation_id = hdr["id"]
                contrato_codigo = hdr["contrato_codigo"]

            cantidad = ml["cantidad"]

            # ----------------------------------------------------------------
            # Resolver la conciliacion segun el modo.
            # ----------------------------------------------------------------
            mid_val: int | None = None
            did_val: int | None = None

            if mode == "nueva":
                precio = (
                    precio_unitario
                    if precio_unitario is not None
                    else ml["precio"]
                )
                desc = descripcion or ml["concepto"]
                partida = ml["codigo_imputacion"]
                unidad_contrato = None
                source = "manual_derived"
                did_val = session.execute(
                    text(
                        "INSERT INTO contrato_lines_derived ("
                        "  created_by_valuation_id, source_document_id, "
                        "  codigo_contrato, codigo_producto, descripcion_linea, "
                        "  unidad_medida, precio_unitario, codigo_partida, "
                        "  origen, created_at_utc) "
                        "VALUES (:vid, :doc, :cod, NULL, :desc, NULL, :pu, "
                        "  :part, 'manual_override', :now) "
                        "RETURNING id"
                    ),
                    {
                        "vid": valuation_id,
                        "doc": document_id,
                        "cod": contrato_codigo or "",
                        "desc": desc,
                        "pu": precio,
                        "part": partida,
                        "now": now,
                    },
                ).scalar_one()
            else:
                if matched_contrato_line_id is None:
                    return False
                cl = session.execute(
                    text(
                        "SELECT precio_unitario, unidad_medida, codigo_partida, "
                        "       descripcion_linea "
                        "FROM albaran_contrato_lines_merge WHERE id = :id"
                    ),
                    {"id": matched_contrato_line_id},
                ).mappings().first()
                if cl is None:
                    return False
                precio = (
                    precio_unitario
                    if precio_unitario is not None
                    else cl["precio_unitario"]
                )
                desc = ml["concepto"]
                partida = ml["codigo_imputacion"] or cl["codigo_partida"]
                unidad_contrato = cl["unidad_medida"]
                source = "manual_contract"
                mid_val = matched_contrato_line_id

            importe = (
                round(float(precio) * float(cantidad), 2)
                if (precio is not None and cantidad is not None)
                else None
            )

            existing = session.execute(
                text(
                    "SELECT id FROM albaran_line_valuations "
                    "WHERE valuation_id = :v AND merge_line_id = :m"
                ),
                {"v": valuation_id, "m": merge_line_id},
            ).mappings().first()

            if existing is None:
                # Crear la linea de valoracion completa (todas las columnas
                # NOT NULL cubiertas). line_kind=from_albaran.
                session.execute(
                    text(
                        "INSERT INTO albaran_line_valuations ("
                        "  valuation_id, merge_line_id, matched_contrato_line_id, "
                        "  derived_contrato_line_id, "
                        "  precio_unitario_contrato_db, precio_unitario_final, "
                        "  precio_unitario_source, precio_unitario_agreement, "
                        "  unidad_albaran, unidad_contrato, unidad_categoria, "
                        "  unidad_category_match, cantidad_albaran, "
                        "  cantidad_convertida, factor_conversion, importe_calculado, "
                        "  importe_source, codigo_partida_albaran, codigo_partida_final, "
                        "  partida_action, descripcion_linea, line_kind, "
                        "  match_confidence_pct, match_method, review_required, "
                        "  created_at_utc) "
                        "VALUES ("
                        "  :v, :m, :mid, :did, :pu, :pu, :src, 'manual', "
                        "  NULL, :uc, 'manual', TRUE, :can, :can, 1.0, :imp, "
                        "  'calculated', :part, :part, 'manual', :desc, "
                        "  'from_albaran', 100.0, 'manual', FALSE, :now)"
                    ),
                    {
                        "v": valuation_id,
                        "m": merge_line_id,
                        "mid": mid_val,
                        "did": did_val,
                        "pu": precio,
                        "src": source,
                        "uc": unidad_contrato,
                        "can": cantidad,
                        "imp": importe,
                        "part": partida,
                        "desc": desc,
                        "now": now,
                    },
                )
            else:
                # Ya habia valoracion para esa linea: la re-apuntamos
                # (al contrato o a la derivada nueva) como en
                # set_line_conciliacion.
                session.execute(
                    text(
                        "UPDATE albaran_line_valuations SET "
                        "  matched_contrato_line_id = :mid, "
                        "  derived_contrato_line_id = :did, "
                        "  precio_unitario_final = :pu, "
                        "  precio_unitario_source = :src, "
                        "  precio_unitario_agreement = 'manual', "
                        "  importe_calculado = :imp, "
                        "  importe_source = 'calculated', "
                        "  review_required = FALSE "
                        "WHERE id = :id"
                    ),
                    {
                        "mid": mid_val,
                        "did": did_val,
                        "pu": precio,
                        "src": source,
                        "imp": importe,
                        "id": existing["id"],
                    },
                )

            # Recalcular total de la cabecera.
            session.execute(
                text(
                    "UPDATE albaran_valuations SET "
                    "  total_valorado = COALESCE((SELECT SUM(importe_calculado) "
                    "    FROM albaran_line_valuations WHERE valuation_id = :v), 0), "
                    "  updated_at_utc = :now "
                    "WHERE id = :v"
                ),
                {"v": valuation_id, "now": now},
            )
            session.commit()
            return True


    def _build_merge_detail(
        self,
        *,
        merge_doc: AlbaranDocumentMergeOrm,
        available_views: list[str],
        provider_snapshots: list[ProviderSnapshot],
        contratos: list[ContratoPayload],
        selected_contrato_codigo: str | None,
        valuation: ValuationPayload | None = None,
        conciliation_by_valuation_line_id: dict[int, ConciliacionDisplay] | None = None,
        contrato_lines: list[ContratoLinePayload] | None = None,
        proveedores: list[ProveedorOption] | None = None,
        obras: list[ObraOption] | None = None,
    ) -> DocumentDetailPayload:
        merge_lines_payload = [
            self._merge_line_to_payload(line) for line in merge_doc.lines
        ]
        display_lines = self._build_display_lines(
            merge_lines=merge_lines_payload,
            valuation=valuation,
            conciliation_by_valuation_line_id=conciliation_by_valuation_line_id or {},
        )
        return DocumentDetailPayload(
            id=merge_doc.id,
            view_mode=VIEW_MODE_MERGE,
            available_views=available_views,
            is_editable=True,
            provider_document_id=None,
            source_document_id=merge_doc.source_document_id,
            document_storage_ref=merge_doc.document_storage_ref,
            source_filename=merge_doc.source_filename,
            provider_origin=merge_doc.provider_origin,
            model_name=merge_doc.model_name,
            proveedor_nombre=merge_doc.proveedor_nombre,
            proveedor_cif=merge_doc.proveedor_cif,
            fecha=merge_doc.fecha,
            numero_albaran=merge_doc.numero_albaran,
            forma_pago=merge_doc.forma_pago,
            obra_codigo=merge_doc.obra_codigo,
            obra_nombre=merge_doc.obra_nombre,
            obra_direccion=merge_doc.obra_direccion,
            document_url=self._document_url(merge_doc),
            confidence_pct_calc=merge_doc.confidence_pct_calc,
            review_required=merge_doc.review_required,
            review_reasons_json=merge_doc.review_reasons_json,
            comparison_summary_json=merge_doc.comparison_summary_json,
            raw_extraction_json=merge_doc.raw_extraction_json,
            ia_output_json=None,
            approved=bool(merge_doc.approved),
            approved_at_utc=merge_doc.approved_at_utc,
            approved_by=merge_doc.approved_by,
            reviewed_at_utc=merge_doc.reviewed_at_utc,
            review_notes=merge_doc.review_notes,
            created_at_utc=merge_doc.created_at_utc,
            lines=merge_lines_payload,
            display_lines=display_lines,
            provider_snapshots=provider_snapshots,
            contratos=contratos,
            contrato_lines=contrato_lines or [],
            proveedores_disponibles=proveedores or [],
            obras_disponibles=obras or [],
            selected_contrato_codigo=selected_contrato_codigo,
            valuation=valuation,
        )

    def _build_provider_detail(
        self,
        *,
        merge_doc: AlbaranDocumentMergeOrm,
        provider_doc: AlbaranDocumentBaseOrm,
        provider_lines: list[AlbaranLineBaseOrm],
        available_views: list[str],
        provider_snapshots: list[ProviderSnapshot],
        view_mode: str,
        contratos: list[ContratoPayload],
        selected_contrato_codigo: str | None,
        valuation: ValuationPayload | None = None,
    ) -> DocumentDetailPayload:
        return DocumentDetailPayload(
            id=merge_doc.id,
            view_mode=view_mode,
            available_views=available_views,
            is_editable=False,
            provider_document_id=provider_doc.id,
            source_document_id=provider_doc.source_document_id
            or merge_doc.source_document_id,
            document_storage_ref=merge_doc.document_storage_ref,
            source_filename=provider_doc.source_filename or merge_doc.source_filename,
            provider_origin=provider_doc.provider_origin,
            model_name=provider_doc.model_name,
            proveedor_nombre=provider_doc.proveedor_nombre,
            proveedor_cif=None,
            fecha=provider_doc.fecha,
            numero_albaran=provider_doc.numero_albaran,
            forma_pago=None,
            obra_codigo=provider_doc.obra_codigo,
            obra_nombre=None,
            obra_direccion=None,
            document_url=self._document_url(merge_doc),
            confidence_pct_calc=None,
            review_required=None,
            review_reasons_json=None,
            comparison_summary_json=None,
            raw_extraction_json=provider_doc.raw_extraction_json,
            ia_output_json=provider_doc.ia_output_json,
            approved=bool(merge_doc.approved),
            approved_at_utc=merge_doc.approved_at_utc,
            approved_by=merge_doc.approved_by,
            reviewed_at_utc=merge_doc.reviewed_at_utc,
            review_notes=merge_doc.review_notes,
            created_at_utc=provider_doc.created_at_utc,
            lines=[self._base_line_to_payload(line) for line in provider_lines],
            provider_snapshots=provider_snapshots,
            contratos=contratos,
            selected_contrato_codigo=selected_contrato_codigo,
            valuation=valuation,
        )

    def get_merge_cif_and_obra(
        self,
        *,
        document_id: str,
    ) -> tuple[str | None, str | None]:
        self.initialize()
        with self._session_factory.create_session() as session:
            document = session.get(AlbaranDocumentMergeOrm, document_id)
            if document is None:
                return None, None
            return document.proveedor_cif, document.obra_codigo

    # ---------------------------------------------------------------- #
    # NOTA REFACTOR (mayo 2026): se eliminaron de aquí 3 métodos
    # obsoletos que pertenecían al wiring antiguo (cuando el sv4
    # llamaba a Sigrid directamente):
    #
    #   * get_existing_pdf_paths
    #   * replace_contratos_and_select
    #   * update_contrato_pdf_paths
    #
    # Esa responsabilidad ahora vive ÍNTEGRAMENTE en el sv3, que
    # es el dueño de las tablas albaran_contratos_merge y
    # albaran_contrato_lines_merge (con UPSERT por sigrid_ide).
    # El sv4 solo LEE esas tablas para pintar el portal.
    # ---------------------------------------------------------------- #

    def update_document(
        self,
        *,
        document_id: str,
        payload: MergeDocumentUpdatePayload,
    ) -> DocumentDetailPayload:
        self.initialize()
        with self._session_factory.create_session() as session:
            document = session.get(AlbaranDocumentMergeOrm, document_id)
            if document is None:
                raise KeyError(f"Documento no encontrado: {document_id}")

            document.proveedor_nombre = self._clean_text(payload.proveedor_nombre)
            document.proveedor_cif = self._clean_text(payload.proveedor_cif)
            document.fecha = self._clean_text(payload.fecha)
            document.numero_albaran = self._clean_text(payload.numero_albaran)
            document.forma_pago = self._clean_text(payload.forma_pago)
            document.obra_codigo = self._clean_text(payload.obra_codigo)
            document.obra_nombre = self._clean_text(payload.obra_nombre)
            document.obra_direccion = self._clean_text(payload.obra_direccion)
            document.review_notes = self._clean_text(payload.review_notes)
            document.reviewed_at_utc = self._utc_iso()
            document.last_modified_at_utc = self._utc_iso()

            proposed_codigo = self._clean_text(payload.selected_contrato_codigo)
            if proposed_codigo is not None:
                existing_codes = set(
                    session.scalars(
                        select(AlbaranContratoMergeOrm.codigo_contrato).where(
                            AlbaranContratoMergeOrm.document_id == document.id
                        )
                    ).all()
                )
                if proposed_codigo not in existing_codes:
                    # Contrato COMPARTIDO: la cabecera de contrato es unica
                    # por sigrid_ide y su document_id se sobrescribe con el
                    # del ULTIMO albaran enriquecido (UPSERT en sv3). Un
                    # albaran que comparte el mismo contrato no es "dueno" de
                    # la fila, asi que el filtro por document_id no lo
                    # encuentra y antes borrabamos la seleccion a None -> el
                    # boton Valorar daba 409 "no tiene contrato seleccionado".
                    # Aceptamos el codigo si existe como contrato en
                    # albaran_contratos_merge (misma recuperacion por codigo
                    # que el detalle).
                    shared = session.scalar(
                        select(AlbaranContratoMergeOrm.codigo_contrato)
                        .where(
                            AlbaranContratoMergeOrm.codigo_contrato
                            == proposed_codigo
                        )
                        .limit(1)
                    )
                    if shared is None:
                        proposed_codigo = None
            document.selected_contrato_codigo = proposed_codigo

            if payload.approved:
                document.approved = True
                document.approved_at_utc = self._utc_iso()
                document.approved_by = self._clean_text(payload.approved_by)
            else:
                document.approved = False
                document.approved_at_utc = None
                document.approved_by = None

            # -------------------------------------------------------- #
            # Persistencia de líneas — UPDATE IN-PLACE (no delete+insert).
            #
            # Importante: las valoraciones del servicio 6 (tabla
            # albaran_line_valuations) tienen FK
            #   merge_line_id -> albaran_lines_merge.id ON DELETE CASCADE
            # Si borráramos y reinsertáramos las líneas merge, la
            # valoración entera se perdería en cada save. Por eso:
            #   - Las líneas con id conocido se UPDATE en su sitio.
            #   - Las líneas nuevas (sin id) se INSERT.
            #   - Las líneas que estaban y el revisor eliminó (ya no
            #     vienen en el payload) se DELETE explícitamente.
            # Así los ids sobreviven y la valoración asociada también.
            # -------------------------------------------------------- #
            incoming_ids: set[int] = {
                int(line.id)
                for line in payload.lines
                if line.id is not None
            }

            # (a) borrar sólo las líneas que desaparecieron
            existing_rows = session.scalars(
                select(AlbaranLineMergeOrm).where(
                    AlbaranLineMergeOrm.document_id == document.id
                )
            ).all()
            for row in existing_rows:
                if row.id not in incoming_ids:
                    session.delete(row)
            session.flush()

            # (b) update in-place + insert de nuevas
            for index, line in enumerate(payload.lines, start=1):
                if line.id is not None:
                    existing = session.get(AlbaranLineMergeOrm, int(line.id))
                    if existing is not None and existing.document_id == document.id:
                        existing.line_index = index
                        existing.external_line_id = self._clean_text(line.external_line_id)
                        existing.cabecera_id = self._clean_text(line.cabecera_id)
                        existing.codigo = self._clean_text(line.codigo)
                        existing.cantidad = line.cantidad
                        existing.concepto = self._clean_text(line.concepto)
                        existing.precio = line.precio
                        existing.descuento = line.descuento
                        existing.precio_neto = line.precio_neto
                        existing.codigo_imputacion = self._clean_text(line.codigo_imputacion)
                        existing.confianza_pct = line.confianza_pct
                        existing.confidence_pct_calc = line.confidence_pct_calc
                        existing.line_match_score = line.line_match_score
                        existing.comparison_status_json = line.comparison_status_json
                        existing.field_scores_json = line.field_scores_json
                        continue
                # línea nueva o id no válido -> insert
                session.add(
                    AlbaranLineMergeOrm(
                        document_id=document.id,
                        provider_origin=document.provider_origin,
                        line_index=index,
                        external_line_id=self._clean_text(line.external_line_id),
                        cabecera_id=self._clean_text(line.cabecera_id),
                        codigo=self._clean_text(line.codigo),
                        cantidad=line.cantidad,
                        concepto=self._clean_text(line.concepto),
                        precio=line.precio,
                        descuento=line.descuento,
                        precio_neto=line.precio_neto,
                        codigo_imputacion=self._clean_text(line.codigo_imputacion),
                        confianza_pct=line.confianza_pct,
                        confidence_pct_calc=line.confidence_pct_calc,
                        line_match_score=line.line_match_score,
                        comparison_status_json=line.comparison_status_json,
                        field_scores_json=line.field_scores_json,
                    )
                )
            session.flush()

            # -------------------------------------------------------- #
            # Sub-tanda 2D — ediciones del revisor sobre líneas
            # sintéticas (line_kind='synthetic_modifier').
            #
            # Estas líneas NO viven en albaran_lines_merge; viven en
            # albaran_line_valuations con merge_line_id=NULL. El
            # revisor las ve en la tabla del detalle como una fila
            # más y puede editar sus campos visibles: concepto,
            # cantidad, unidad, precio unitario, importe, partida.
            # Aquí aplicamos esas ediciones por valuation_line_id.
            #
            # No afecta a las from_albaran (tienen merge_line_id real
            # y ya se han procesado arriba). Tampoco se crean ni se
            # borran sintéticas desde la UI — eso lo hace el svc6 al
            # re-valorar. Si el usuario borró visualmente una fila
            # sintética, simplemente no llega en el payload y queda
            # sin modificar; si quiere quitarla de la valoración,
            # tiene que re-valorar.
            # -------------------------------------------------------- #
            if payload.valuation_line_updates:
                self._apply_valuation_line_updates_in_session(
                    session=session,
                    document_id=document.id,
                    updates=payload.valuation_line_updates,
                )
                session.flush()

            # -------------------------------------------------------- #
            # Recalcular importe_calculado en las líneas valoradas que
            # sobreviven. Fórmula: precio_unitario_final * cantidad
            # efectiva, donde cantidad efectiva =
            #   cantidad_convertida (si hay factor_conversion)
            #   ELSE cantidad_albaran nueva (la que acaba de editar el
            #        revisor).
            # También actualizamos total_valorado en la cabecera.
            # -------------------------------------------------------- #
            self._recalc_valuation_importes(
                session=session,
                document_id=document.id,
                new_line_quantities={
                    int(line.id): line.cantidad
                    for line in payload.lines
                    if line.id is not None and line.cantidad is not None
                },
            )

            session.commit()

        detail = self.get_document_detail(document_id)
        if detail is None:
            raise KeyError(f"Documento no encontrado tras guardar: {document_id}")
        return detail

    # ------------------------------------------------------------------ #
    # Recálculo de importes en la valoración tras guardar cambios del
    # revisor. Tolerante a ausencia de la valoración (no pasa nada si
    # todavía no existe para este documento).
    # ------------------------------------------------------------------ #
    def _recalc_valuation_importes(
        self,
        *,
        session: Any,
        document_id: str,
        new_line_quantities: dict[int, float],
    ) -> None:
        try:
            val_row = session.execute(
                text(
                    "SELECT id FROM albaran_valuations "
                    "WHERE document_id = :doc_id"
                ),
                {"doc_id": document_id},
            ).mappings().first()
        except Exception:
            # Tabla puede no existir en BBDD antiguas.
            session.rollback()
            return
        if val_row is None:
            return
        valuation_id = str(val_row["id"])

        # Cargar líneas de la valoración (pu_final + factor_conversion
        # + cantidad actual). Sólo procesamos las que tengan pu_final.
        try:
            line_rows = session.execute(
                text(
                    "SELECT id, merge_line_id, precio_unitario_final, "
                    "       factor_conversion, cantidad_albaran, "
                    "       cantidad_convertida, importe_source "
                    "FROM albaran_line_valuations "
                    "WHERE valuation_id = :vid"
                ),
                {"vid": valuation_id},
            ).mappings().all()
        except Exception:
            session.rollback()
            return

        for row in line_rows:
            pu = row["precio_unitario_final"]
            if pu is None:
                continue

            # Sub-tanda 2D: para las sintéticas merge_line_id es NULL.
            # Esta función recalcula importes a partir de cantidades
            # editadas por el revisor en líneas from_albaran; las
            # sintéticas NO reciben cantidad del formulario por esa
            # vía (su cantidad ya se actualizó, si el revisor la
            # tocó, en _apply_valuation_line_updates_in_session).
            # Aquí las dejamos con el importe_calculado que ya tenían.
            raw_merge_line_id = row["merge_line_id"]
            if raw_merge_line_id is None:
                continue
            merge_line_id = int(raw_merge_line_id)

            # Cantidad a usar: la nueva editada por el revisor (si
            # tenemos en el payload), si no la actual de la fila.
            if merge_line_id in new_line_quantities:
                nueva_cant_albaran: float | None = float(
                    new_line_quantities[merge_line_id]
                )
            else:
                existing_ca = row["cantidad_albaran"]
                nueva_cant_albaran = (
                    float(existing_ca) if existing_ca is not None else None
                )

            factor = row["factor_conversion"]
            if factor is not None and nueva_cant_albaran is not None:
                nueva_cant_conv: float | None = float(factor) * nueva_cant_albaran
            else:
                nueva_cant_conv = None

            cantidad_efectiva = (
                nueva_cant_conv
                if nueva_cant_conv is not None
                else nueva_cant_albaran
            )
            if cantidad_efectiva is None:
                continue

            nuevo_importe = round(float(pu) * float(cantidad_efectiva), 2)

            # Si antes era 'declared_albaran' respetamos esa semántica
            # (el albarán lo traía explícito), pero igualmente sobre-
            # escribimos con el nuevo calculado si el revisor cambió
            # cantidad — tiene más autoridad. importe_source queda
            # como 'calculated' cuando el revisor ha intervenido.
            new_src = "calculated"

            session.execute(
                text(
                    "UPDATE albaran_line_valuations "
                    "SET cantidad_albaran = :ca, "
                    "    cantidad_convertida = :cc, "
                    "    importe_calculado = :imp, "
                    "    importe_source = :src "
                    "WHERE id = :vid"
                ),
                {
                    "ca": nueva_cant_albaran,
                    "cc": nueva_cant_conv,
                    "imp": nuevo_importe,
                    "src": new_src,
                    "vid": int(row["id"]),
                },
            )

        # Total de la cabecera.
        session.execute(
            text(
                "UPDATE albaran_valuations SET "
                "total_valorado = COALESCE(("
                "  SELECT SUM(importe_calculado) "
                "  FROM albaran_line_valuations "
                "  WHERE valuation_id = albaran_valuations.id"
                "), 0), "
                "updated_at_utc = :now "
                "WHERE id = :vid"
            ),
            {"now": self._utc_iso(), "vid": valuation_id},
        )

    # ------------------------------------------------------------------ #
    # Sub-tanda 2D
    #
    # Aplica ediciones manuales del revisor sobre líneas sintéticas de
    # valoración (line_kind='synthetic_modifier'). Estas filas viven
    # en albaran_line_valuations con merge_line_id=NULL y se editan
    # directamente en la tabla del detalle.
    #
    # Campos editables (ver ValuationLineUpdate en review_models):
    #   - codigo_partida_final
    #   - descripcion_linea
    #   - cantidad_albaran (se replica en cantidad_convertida,
    #     factor 1.0 porque no hay conversión de unidad para sintéticas)
    #   - unidad_contrato (unidad visible en la fila)
    #   - precio_unitario_final
    #   - importe_calculado (recalculado si cantidad o precio cambian)
    #
    # Por seguridad:
    #   - Filtramos por valuation_line_id en albaran_line_valuations
    #     que pertenezca a un valuation del document_id dado
    #     (evita que un payload manipule filas de otro documento).
    #   - Solo actualizamos filas con line_kind='synthetic_modifier'.
    #     Las from_albaran no pueden editarse por esta vía.
    # ------------------------------------------------------------------ #
    def _apply_valuation_line_updates_in_session(
        self,
        *,
        session: Any,
        document_id: str,
        updates: list[Any],  # list[ValuationLineUpdate]; tipado suelto
                             # para no importar el modelo en el header.
    ) -> None:
        if not updates:
            return

        # Recogemos los id solicitados para validarlos en una sola
        # query y evitar que un payload toque filas ajenas.
        requested_ids = [int(u.valuation_line_id) for u in updates]

        try:
            allowed_rows = session.execute(
                text(
                    "SELECT lv.id AS lv_id "
                    "FROM albaran_line_valuations lv "
                    "JOIN albaran_valuations v ON v.id = lv.valuation_id "
                    "WHERE v.document_id = :doc_id "
                    "  AND lv.id = ANY(:ids)"
                ),
                {"doc_id": document_id, "ids": requested_ids},
            ).mappings().all()
        except Exception:
            # Si la columna line_kind aún no existiera (BBDD pre-2D),
            # salimos silenciosamente. No tiene sentido aplicar updates
            # a sintéticas si no existe el concepto.
            session.rollback()
            return

        allowed_ids = {int(row["lv_id"]) for row in allowed_rows}
        if not allowed_ids:
            return

        for upd in updates:
            lv_id = int(upd.valuation_line_id)
            if lv_id not in allowed_ids:
                # Silencioso: la fila no pertenece a este documento,
                # no es sintética o no existe. Puede pasar si el
                # revisor recarga tras una re-valoración que eliminó
                # la fila.
                continue

            # Derivamos el nuevo importe calculado: precio * cantidad.
            # Si alguno falta, dejamos importe_calculado en null y que
            # review_required se conserve como estaba.
            nuevo_importe: float | None = None
            if (
                upd.precio_unitario_final is not None
                and upd.cantidad_albaran is not None
            ):
                try:
                    nuevo_importe = round(
                        float(upd.precio_unitario_final)
                        * float(upd.cantidad_albaran),
                        2,
                    )
                except (TypeError, ValueError):
                    nuevo_importe = None

            # Si el revisor manda explícitamente un importe_calculado
            # distinto del calculado, respetamos el del revisor (tiene
            # autoridad). Esto cubre casos de precios por escalones o
            # redondeos propios.
            if upd.importe_calculado is not None:
                try:
                    nuevo_importe = round(float(upd.importe_calculado), 2)
                except (TypeError, ValueError):
                    pass

            # Para sintéticas no hay conversión de unidad: cantidad
            # convertida = cantidad albarán, factor = 1.0.
            cantidad_val = (
                float(upd.cantidad_albaran)
                if upd.cantidad_albaran is not None
                else None
            )

            # Las fuentes se calculan en Python (NO con CASE WHEN sobre el
            # parámetro): usar `CASE WHEN :pu IS NOT NULL ...` con :pu NULL
            # hace que Postgres no pueda deducir el tipo del parámetro
            # ("AmbiguousParameter: no se pudo determinar el tipo del
            # parámetro"). Pasándolas ya resueltas evitamos el problema y
            # queda más claro.
            precio_source_val = (
                "manual"
                if upd.precio_unitario_final is not None
                else "none"
            )
            importe_source_val = (
                "calculated" if nuevo_importe is not None else "none"
            )

            session.execute(
                text(
                    "UPDATE albaran_line_valuations SET "
                    "    codigo_partida_final = COALESCE(:codpart, codigo_partida_final), "
                    "    descripcion_linea = COALESCE(:desc, descripcion_linea), "
                    "    cantidad_albaran = :ca, "
                    "    cantidad_convertida = :cc, "
                    "    factor_conversion = :fc, "
                    "    unidad_contrato = COALESCE(:uc, unidad_contrato), "
                    "    precio_unitario_final = :pu, "
                    "    precio_unitario_source = :pu_src, "
                    "    importe_calculado = :imp, "
                    "    importe_source = :imp_src "
                    "WHERE id = :lv_id"
                ),
                {
                    "codpart": self._clean_text(upd.codigo_partida_final),
                    "desc": self._clean_text(upd.descripcion_linea),
                    "ca": cantidad_val,
                    "cc": cantidad_val,
                    "fc": 1.0 if cantidad_val is not None else None,
                    "uc": self._clean_text(upd.unidad_contrato),
                    "pu": upd.precio_unitario_final,
                    "pu_src": precio_source_val,
                    "imp": nuevo_importe,
                    "imp_src": importe_source_val,
                    "lv_id": lv_id,
                },
            )

    def set_approved(
        self,
        *,
        document_id: str,
        approved: bool,
        approved_by: str | None,
    ) -> None:
        self.initialize()
        with self._session_factory.create_session() as session:
            document = session.get(AlbaranDocumentMergeOrm, document_id)
            if document is None:
                raise KeyError(f"Documento no encontrado: {document_id}")
            document.approved = approved
            document.reviewed_at_utc = document.reviewed_at_utc or self._utc_iso()
            document.last_modified_at_utc = self._utc_iso()
            if approved:
                document.approved_at_utc = self._utc_iso()
                document.approved_by = self._clean_text(approved_by)
            else:
                document.approved_at_utc = None
                document.approved_by = None
            session.commit()

    def build_query_string(
        self,
        *,
        filters: DocumentListFilters,
        overrides: dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = filters.model_dump()
        if overrides:
            payload.update(overrides)
        return urlencode(
            {
                key: value
                for key, value in payload.items()
                if value not in (None, "")
            }
        )

    @staticmethod
    def _clean_text(value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def _utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _document_url(document: AlbaranDocumentMergeOrm) -> str | None:
        return (
            document.sharepoint_share_url
            or document.sharepoint_web_url
            or document.document_storage_ref
            or document.sharepoint_relative_path
        )

    # ------------------------------------------------------------------ #
    # Sub-tanda 2D — construcción de display_lines
    #
    # Mezcla líneas del merge (from_albaran) y líneas sintéticas de
    # valoración en una sola lista ordenada:
    #
    #   [base_1, sint_a_de_base_1, sint_b_de_base_1,
    #    base_2, sint_c_de_base_2,
    #    base_3, ...]
    #
    # Criterio: por cada línea del merge (respetando su line_index),
    # emitimos su DisplayLine y justo después las sintéticas cuyo
    # parent_merge_line_id apunte a ella. Las sintéticas huérfanas
    # (parent no encontrado) se emiten al final.
    #
    # Los campos se rellenan priorizando valoración sobre albarán:
    #   - codigo_imputacion   ← codigo_partida_final ?? codigo_imputacion
    #   - cantidad            ← cantidad_convertida ?? cantidad_albaran
    #   - unidad              ← unidad_contrato    ?? unidad_albaran
    #   - precio_unitario     ← precio_unitario_final
    #   - importe             ← importe_calculado  ?? precio_neto
    #
    # Para sintéticas, todos los campos vienen del payload de
    # valoración (no hay merge equivalente).
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_display_lines(
        *,
        merge_lines: list[MergeLinePayload],
        valuation: ValuationPayload | None,
        conciliation_by_valuation_line_id: dict[int, ConciliacionDisplay] | None = None,
    ) -> list[DisplayLine]:
        val_lines_by_merge = (
            valuation.lines_by_merge_line_id if valuation else {}
        )
        synthetic_all = (
            list(valuation.synthetic_lines) if valuation else []
        )
        conc_map: dict[int, ConciliacionDisplay] = (
            conciliation_by_valuation_line_id or {}
        )

        # Agrupar sintéticas por parent_merge_line_id (int | None).
        synth_by_parent: dict[int | None, list[LineValuationPayload]] = {}
        for syn in synthetic_all:
            key = syn.parent_merge_line_id
            synth_by_parent.setdefault(key, []).append(syn)

        display: list[DisplayLine] = []

        # Avisos tri-estado de la fila salmon: comparan lo DECLARADO en la
        # linea blanca contra la referencia de contrato. None = no hay dato
        # declarado que comparar (sin icono); True = coincide (✓);
        # False = difiere (⚠).
        def _agree_num(declared, expected):
            if declared is None or expected is None:
                return None
            try:
                return abs(float(declared) - float(expected)) < 0.01
            except (TypeError, ValueError):
                return None

        def _agree_txt(declared, expected):
            if declared is None or str(declared).strip() == "":
                return None
            if expected is None or str(expected).strip() == "":
                return None
            return str(declared).strip() == str(expected).strip()

        for idx, line in enumerate(merge_lines, start=1):
            v = val_lines_by_merge.get(line.id) if line.id is not None else None

            # MODELO "la fila salmon nunca modifica la blanca": la linea
            # blanca (from_albaran) muestra SOLO lo leido del albaran (merge).
            # La valoracion/conciliacion (precio del contrato, importe
            # calculado, partida casada) vive en la fila salmon (concilia),
            # NO aqui. Asi conciliar o cambiar la linea de contrato nunca
            # pisa lo leido/escrito por el usuario.
            eff_codimp = line.codigo_imputacion
            eff_cantidad = line.cantidad
            eff_unidad = None
            eff_precio_unit = line.precio
            eff_importe = line.precio_neto

            _conc = (
                conc_map.get(v.valuation_line_id)
                if v is not None and v.valuation_line_id is not None
                else None
            )
            if _conc is not None:
                _exp_imp = (
                    line.cantidad * _conc.unitario
                    if line.cantidad is not None and _conc.unitario is not None
                    else None
                )
                # partida: lo imputado en la blanca vs la partida del contrato
                _conc.agree_partida = _agree_txt(
                    line.codigo_imputacion, _conc.codigo_partida
                )
                # cantidad: la salmon copia la de la blanca (coincide si hay)
                _conc.agree_cantidad = _agree_num(line.cantidad, line.cantidad)
                # unitario: precio declarado del albaran vs precio de contrato
                _conc.agree_unitario = _agree_num(line.precio, _conc.unitario)
                # importe: importe declarado vs cantidad x unitario de contrato
                _conc.agree_importe = _agree_num(line.precio_neto, _exp_imp)

            display.append(
                DisplayLine(
                    line_kind="from_albaran",
                    merge_line_id=line.id,
                    valuation_line_id=(v.valuation_line_id if v else None),
                    line_index=idx,
                    codigo_imputacion=eff_codimp,
                    concepto=line.concepto,
                    cantidad=eff_cantidad,
                    unidad=eff_unidad,
                    precio_unitario=eff_precio_unit,
                    importe=eff_importe,
                    descuento=line.descuento,
                    codigo=line.codigo,
                    confianza_pct=line.confianza_pct,
                    is_valued=v is not None,
                    parent_merge_line_id=None,
                    concilia=_conc,
                )
            )

            # Sintéticas que cuelgan de esta base.
            for syn in synth_by_parent.get(line.id, []):
                display.append(
                    AlbaranReviewRepository._synthetic_to_display(
                        syn=syn,
                        line_index=idx,
                        conc_map=conc_map,
                    )
                )

        # Sintéticas huérfanas (parent no encontrado). Raro, pero no
        # podemos silenciarlas sin avisar: las colocamos al final.
        orphans: list[LineValuationPayload] = []
        known_parents = {line.id for line in merge_lines if line.id is not None}
        for parent_key, syns in synth_by_parent.items():
            if parent_key is None or parent_key not in known_parents:
                orphans.extend(syns)
        for orphan_idx, syn in enumerate(orphans, start=len(merge_lines) + 1):
            display.append(
                AlbaranReviewRepository._synthetic_to_display(
                    syn=syn,
                    line_index=orphan_idx,
                    conc_map=conc_map,
                )
            )

        return display

    @staticmethod
    def _synthetic_to_display(
        *,
        syn: LineValuationPayload,
        line_index: int,
        conc_map: dict[int, ConciliacionDisplay] | None = None,
    ) -> DisplayLine:
        conc = (
            (conc_map or {}).get(syn.valuation_line_id)
            if syn.valuation_line_id is not None
            else None
        )

        # Avisos tri-estado tambien para las sinteticas (antes solo se
        # calculaban para from_albaran -> en hormigon no salian los ✓/⚠
        # de partida/precio/importe). Para una sintetica la "linea blanca"
        # es su propio valor (no hay merge): se compara contra el contrato
        # de la salmon (conc). None = nada que comparar (sin icono).
        if conc is not None:
            _cant = (
                syn.cantidad_convertida
                if syn.cantidad_convertida is not None
                else syn.cantidad_albaran
            )
            _unit = conc.unitario
            _exp_imp = (
                _cant * _unit
                if _cant is not None and _unit is not None
                else None
            )

            def _num(d, e):
                if d is None or e is None:
                    return None
                try:
                    return abs(float(d) - float(e)) < 0.01
                except (TypeError, ValueError):
                    return None

            def _txt(d, e):
                if d is None or str(d).strip() == "":
                    return None
                if e is None or str(e).strip() == "":
                    return None
                return str(d).strip() == str(e).strip()

            conc.agree_partida = _txt(syn.codigo_partida_final, conc.codigo_partida)
            conc.agree_cantidad = _num(_cant, _cant)
            conc.agree_unitario = _num(syn.precio_unitario_final, _unit)
            conc.agree_importe = _num(syn.importe_calculado, _exp_imp)

        return DisplayLine(
            line_kind="synthetic_modifier",
            merge_line_id=None,
            valuation_line_id=syn.valuation_line_id,
            line_index=line_index,
            codigo_imputacion=syn.codigo_partida_final,
            concepto=syn.descripcion_linea,
            cantidad=(
                syn.cantidad_convertida
                if syn.cantidad_convertida is not None
                else syn.cantidad_albaran
            ),
            unidad=(syn.unidad_contrato or syn.unidad_albaran),
            precio_unitario=syn.precio_unitario_final,
            importe=syn.importe_calculado,
            descuento=None,
            codigo=None,
            confianza_pct=None,
            is_valued=True,
            parent_merge_line_id=syn.parent_merge_line_id,
            concilia=conc,
        )

    @staticmethod
    def _contrato_orm_to_payload(item: AlbaranContratoMergeOrm) -> ContratoPayload:
        return ContratoPayload(
            id=item.id,
            codigo_contrato=item.codigo_contrato,
            nombre_contrato=item.nombre_contrato,
            fecha_alta_contrato=item.fecha_alta_contrato,
            fecha_contrato=item.fecha_contrato,
            vigencia_desde=item.vigencia_desde,
            vigencia_hasta=item.vigencia_hasta,
            importe_total=item.importe_total,
            cif_proveedor=item.cif_proveedor,
            nombre_proveedor=item.nombre_proveedor,
            codigo_obra=item.codigo_obra,
            nombre_obra=item.nombre_obra,
            pdf_sharepoint_relative_path=getattr(
                item, "pdf_sharepoint_relative_path", None
            ),
            pdf_sharepoint_web_url=getattr(
                item, "pdf_sharepoint_web_url", None
            ),
        )

    @staticmethod
    def _merge_line_to_payload(line: AlbaranLineMergeOrm) -> MergeLinePayload:
        return MergeLinePayload(
            id=line.id,
            line_index=line.line_index,
            external_line_id=line.external_line_id,
            cabecera_id=line.cabecera_id,
            codigo=line.codigo,
            cantidad=line.cantidad,
            concepto=line.concepto,
            precio=line.precio,
            descuento=line.descuento,
            precio_neto=line.precio_neto,
            codigo_imputacion=line.codigo_imputacion,
            confianza_pct=line.confianza_pct,
            confidence_pct_calc=line.confidence_pct_calc,
            line_match_score=line.line_match_score,
            comparison_status_json=line.comparison_status_json,
            field_scores_json=line.field_scores_json,
        )

    @staticmethod
    def _base_line_to_payload(line: AlbaranLineBaseOrm) -> MergeLinePayload:
        return MergeLinePayload(
            id=line.id,
            line_index=line.line_index,
            external_line_id=line.external_line_id,
            cabecera_id=line.cabecera_id,
            codigo=line.codigo,
            cantidad=line.cantidad,
            concepto=line.concepto,
            precio=line.precio,
            descuento=line.descuento,
            precio_neto=line.precio_neto,
            codigo_imputacion=line.codigo_imputacion,
            confianza_pct=line.confianza_pct,
            confidence_pct_calc=None,
            line_match_score=None,
            comparison_status_json=None,
            field_scores_json=None,
        )

    @staticmethod
    def _to_list_item(
        row: AlbaranDocumentMergeOrm,
        total_valorado: float | None = None,
        contrato_codigo: str | None = None,
        contrato_nombre: str | None = None,
    ) -> DocumentListItem:
        return DocumentListItem(
            id=row.id,
            source_document_id=row.source_document_id,
            source_filename=row.source_filename,
            proveedor_nombre=row.proveedor_nombre,
            fecha=row.fecha,
            obra_codigo=row.obra_codigo,
            obra_nombre=row.obra_nombre,
            numero_albaran=row.numero_albaran,
            confidence_pct_calc=row.confidence_pct_calc,
            review_required=row.review_required,
            approved=bool(row.approved),
            total_valorado=total_valorado,
            contrato_codigo=contrato_codigo,
            contrato_nombre=contrato_nombre,
            provider_origin=row.provider_origin,
            created_at_utc=row.created_at_utc,
            document_url=AlbaranReviewRepository._document_url(row),
        )

    @staticmethod
    def _apply_filters(*, stmt: Any, filters: DocumentListFilters) -> Any:
        if filters.search:
            term = f"%{filters.search.strip()}%"
            stmt = stmt.where(
                or_(
                    AlbaranDocumentMergeOrm.proveedor_nombre.ilike(term),
                    AlbaranDocumentMergeOrm.numero_albaran.ilike(term),
                    AlbaranDocumentMergeOrm.obra_codigo.ilike(term),
                    AlbaranDocumentMergeOrm.obra_nombre.ilike(term),
                    AlbaranDocumentMergeOrm.source_filename.ilike(term),
                    AlbaranDocumentMergeOrm.source_document_id.ilike(term),
                )
            )
        if filters.approved == "approved":
            stmt = stmt.where(AlbaranDocumentMergeOrm.approved.is_(True))
        elif filters.approved == "pending":
            stmt = stmt.where(AlbaranDocumentMergeOrm.approved.is_(False))
        if filters.review_required == "yes":
            stmt = stmt.where(AlbaranDocumentMergeOrm.review_required.is_(True))
        elif filters.review_required == "no":
            stmt = stmt.where(AlbaranDocumentMergeOrm.review_required.is_(False))
        if filters.min_confidence is not None:
            stmt = stmt.where(
                AlbaranDocumentMergeOrm.confidence_pct_calc >= filters.min_confidence
            )
        if filters.max_confidence is not None:
            stmt = stmt.where(
                AlbaranDocumentMergeOrm.confidence_pct_calc <= filters.max_confidence
            )
        return stmt

    @staticmethod
    def _apply_sort(*, stmt: Any, filters: DocumentListFilters) -> Any:
        sort_map = {
            "created_at_utc": AlbaranDocumentMergeOrm.created_at_utc,
            "fecha": AlbaranDocumentMergeOrm.fecha,
            "proveedor_nombre": AlbaranDocumentMergeOrm.proveedor_nombre,
            "obra_codigo": AlbaranDocumentMergeOrm.obra_codigo,
            "numero_albaran": AlbaranDocumentMergeOrm.numero_albaran,
            "confidence_pct_calc": AlbaranDocumentMergeOrm.confidence_pct_calc,
            "approved": AlbaranDocumentMergeOrm.approved,
        }
        column = sort_map.get(filters.sort_by, AlbaranDocumentMergeOrm.confidence_pct_calc)
        if filters.sort_dir == "desc":
            stmt = stmt.order_by(nullslast(column.desc()))
        else:
            stmt = stmt.order_by(nullslast(column.asc()))
        stmt = stmt.order_by(AlbaranDocumentMergeOrm.created_at_utc.desc())
        return stmt

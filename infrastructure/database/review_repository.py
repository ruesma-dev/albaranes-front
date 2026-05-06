# infrastructure/database/review_repository.py
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import delete, func, inspect, nullslast, or_, select, text

from domain.models.contrato_sigrid_models import ContratoFromSigrid
from domain.models.review_models import (
    ConciliacionDisplay,
    ConciliacionSibling,
    ContratoPayload,
    DisplayLine,
    DocumentDetailPayload,
    DocumentListFilters,
    DocumentListItem,
    KNOWN_PROVIDER_VIEWS,
    LineValuationPayload,
    MergeDocumentUpdatePayload,
    MergeLinePayload,
    PaginatedDocuments,
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

        items = [self._to_list_item(row) for row in rows]
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
            contratos_payload = [
                ContratoPayload(
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
                for item in contratos_orm
            ]
            selected_contrato_codigo = getattr(
                merge_doc, "selected_contrato_codigo", None
            )

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

            # NUEVO: leer valoración (si existe) en la misma sesión.
            valuation_payload = self._load_valuation_in_session(
                session=session,
                document_id=merge_doc.id,
            )

            # NUEVO: cargar el mapa de conciliación (qué línea de
            # contrato — sigrid o derivada — casó con cada línea del
            # albarán) para mostrar inline en sv4. None si no hay
            # valoración o ninguna línea tiene matched/derived.
            conciliation_by_merge_line_id = (
                self._load_conciliation_map_in_session(
                    session=session,
                    valuation=valuation_payload,
                )
                if valuation_payload is not None
                else {}
            )

            if normalized_view == VIEW_MODE_MERGE:
                return self._build_merge_detail(
                    merge_doc=merge_doc,
                    available_views=available_views,
                    provider_snapshots=provider_snapshots_payload,
                    contratos=contratos_payload,
                    selected_contrato_codigo=selected_contrato_codigo,
                    valuation=valuation_payload,
                    conciliation_by_merge_line_id=conciliation_by_merge_line_id,
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
                    selected_contrato_codigo=selected_contrato_codigo,
                    valuation=valuation_payload,
                    conciliation_by_merge_line_id=conciliation_by_merge_line_id,
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
        if valuation is None:
            return {}

        # Paso 1: agrupar por tipo (matched / derived).
        matched_to_merge: dict[int, int] = {}
        derived_to_merge: dict[int, int] = {}
        agreement_by_merge: dict[int, str | None] = {}
        precio_final_by_merge: dict[int, float | None] = {}
        for merge_line_id, vline in valuation.lines_by_merge_line_id.items():
            if merge_line_id is None:
                continue
            if vline.matched_contrato_line_id is not None:
                matched_to_merge[vline.matched_contrato_line_id] = merge_line_id
            elif vline.derived_contrato_line_id is not None:
                derived_to_merge[vline.derived_contrato_line_id] = merge_line_id
            agreement_by_merge[merge_line_id] = vline.precio_unitario_agreement
            precio_final_by_merge[merge_line_id] = vline.precio_unitario_final

        out: dict[int, ConciliacionDisplay] = {}

        # Paso 2: cargar líneas matched (de Sigrid cacheadas) en bloque.
        if matched_to_merge:
            try:
                rows = session.execute(
                    text(
                        "SELECT id, descripcion_linea, unidad_medida, "
                        "       precio_unitario, uds, pendiente_servir, "
                        "       codigo_partida, descripcion_partida "
                        "FROM albaran_contrato_lines_merge "
                        "WHERE id = ANY(:ids)"
                    ),
                    {"ids": list(matched_to_merge.keys())},
                ).mappings().all()
            except Exception:
                # Postgres con sqlalchemy soporta ANY(:ids) con lista.
                # Si la BBDD no fuese postgres y fallara, usamos IN clásico.
                session.rollback()
                ids = list(matched_to_merge.keys())
                placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
                params = {f"id_{i}": v for i, v in enumerate(ids)}
                rows = session.execute(
                    text(
                        f"SELECT id, descripcion_linea, unidad_medida, "
                        f"       precio_unitario, uds, pendiente_servir, "
                        f"       codigo_partida, descripcion_partida "
                        f"FROM albaran_contrato_lines_merge "
                        f"WHERE id IN ({placeholders})"
                    ),
                    params,
                ).mappings().all()

            for row in rows:
                merge_line_id = matched_to_merge.get(row["id"])
                if merge_line_id is None:
                    continue
                out[merge_line_id] = ConciliacionDisplay(
                    kind="assigned",
                    descripcion=row.get("descripcion_linea"),
                    unitario=row.get("precio_unitario"),
                    medicion_total=row.get("uds"),
                    medicion_pendiente=row.get("pendiente_servir"),
                    unidad=row.get("unidad_medida"),
                    codigo_partida=row.get("codigo_partida"),
                    descripcion_partida=row.get("descripcion_partida"),
                    price_agreement=agreement_by_merge.get(merge_line_id),
                    precio_unitario_final=precio_final_by_merge.get(merge_line_id),
                    sibling=None,
                )

        # Paso 3: cargar líneas derived (creadas por el valorador) en bloque.
        # Esquema esperado de contrato_lines_derived (ver sv6):
        #   id, descripcion, unidad_medida, precio_unitario, uds,
        #   codigo_partida, descripcion_partida, origen.
        if derived_to_merge:
            try:
                rows = session.execute(
                    text(
                        "SELECT id, descripcion_linea, unidad_medida, "
                        "       precio_unitario, uds, "
                        "       codigo_partida, descripcion_partida, "
                        "       origen "
                        "FROM contrato_lines_derived "
                        "WHERE id = ANY(:ids)"
                    ),
                    {"ids": list(derived_to_merge.keys())},
                ).mappings().all()
            except Exception:
                session.rollback()
                ids = list(derived_to_merge.keys())
                placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
                params = {f"id_{i}": v for i, v in enumerate(ids)}
                try:
                    rows = session.execute(
                        text(
                            f"SELECT id, descripcion_linea, unidad_medida, "
                            f"       precio_unitario, uds, "
                            f"       codigo_partida, descripcion_partida, "
                            f"       origen "
                            f"FROM contrato_lines_derived "
                            f"WHERE id IN ({placeholders})"
                        ),
                        params,
                    ).mappings().all()
                except Exception:
                    # Tabla no existe (BBDD anterior a sv6 con derived).
                    session.rollback()
                    rows = []

            for row in rows:
                merge_line_id = derived_to_merge.get(row["id"])
                if merge_line_id is None:
                    continue
                out[merge_line_id] = ConciliacionDisplay(
                    kind="derived",
                    descripcion=row.get("descripcion_linea"),
                    unitario=row.get("precio_unitario"),
                    medicion_total=row.get("uds"),
                    medicion_pendiente=None,  # derived: no hay pendiente
                    unidad=row.get("unidad_medida"),
                    codigo_partida=row.get("codigo_partida"),
                    descripcion_partida=row.get("descripcion_partida"),
                    price_agreement=agreement_by_merge.get(merge_line_id),
                    precio_unitario_final=precio_final_by_merge.get(merge_line_id),
                    # sibling se rellena en una iteración futura cuando
                    # contrato_lines_derived tenga el FK a la línea
                    # hermana de Sigrid en caso de discrepancia de
                    # precio. Hoy queda None.
                    sibling=None,
                )

        return out

    def _build_merge_detail(
        self,
        *,
        merge_doc: AlbaranDocumentMergeOrm,
        available_views: list[str],
        provider_snapshots: list[ProviderSnapshot],
        contratos: list[ContratoPayload],
        selected_contrato_codigo: str | None,
        valuation: ValuationPayload | None = None,
        conciliation_by_merge_line_id: dict[int, ConciliacionDisplay] | None = None,
    ) -> DocumentDetailPayload:
        merge_lines_payload = [
            self._merge_line_to_payload(line) for line in merge_doc.lines
        ]
        display_lines = self._build_display_lines(
            merge_lines=merge_lines_payload,
            valuation=valuation,
            conciliation_by_merge_line_id=conciliation_by_merge_line_id or {},
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

    def get_existing_pdf_paths(
        self,
        *,
        document_id: str,
    ) -> dict[str, tuple[int | None, str | None, str | None]]:
        self.initialize()
        result: dict[str, tuple[int | None, str | None, str | None]] = {}
        with self._session_factory.create_session() as session:
            rows = session.execute(
                select(
                    AlbaranContratoMergeOrm.codigo_contrato,
                    AlbaranContratoMergeOrm.gra_rep_ide,
                    AlbaranContratoMergeOrm.pdf_sharepoint_relative_path,
                    AlbaranContratoMergeOrm.pdf_sharepoint_web_url,
                ).where(AlbaranContratoMergeOrm.document_id == document_id)
            ).all()
            for codigo, ide, rel_path, web_url in rows:
                result[codigo] = (ide, rel_path, web_url)
        return result

    def replace_contratos_and_select(
        self,
        *,
        document_id: str,
        contratos: list[ContratoFromSigrid],
        selected_codigo: str | None,
    ) -> None:
        self.initialize()
        now = datetime.now(timezone.utc).isoformat()
        with self._session_factory.create_session() as session:
            merge_doc = session.get(AlbaranDocumentMergeOrm, document_id)
            if merge_doc is None:
                raise KeyError(f"Documento merge no encontrado: {document_id}")

            session.execute(
                delete(AlbaranContratoMergeOrm).where(
                    AlbaranContratoMergeOrm.document_id == document_id
                )
            )
            session.flush()

            for contrato in contratos:
                header_orm = AlbaranContratoMergeOrm(
                    document_id=document_id,
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
                    gra_rep_ide=contrato.gra_rep_ide,
                    pdf_sharepoint_relative_path=contrato.pdf_sharepoint_relative_path,
                    pdf_sharepoint_web_url=contrato.pdf_sharepoint_web_url,
                    fetched_at_utc=now,
                )
                session.add(header_orm)
                session.flush()

                for line in (contrato.lines or []):
                    session.add(
                        AlbaranContratoLineMergeOrm(
                            contrato_id=header_orm.id,
                            codigo_contrato=contrato.codigo_contrato,
                            linea=line.linea,
                            numero_linea=line.numero_linea,
                            codigo_producto=line.codigo_producto,
                            codigo_alternativo=line.codigo_alternativo,
                            unidad_medida=line.unidad_medida,
                            descripcion_linea=line.descripcion_linea,
                            uds=line.uds,
                            cantidad_servida=line.cantidad_servida,
                            cantidad_facturada=line.cantidad_facturada,
                            pendiente_servir=line.pendiente_servir,
                            precio_unitario=line.precio_unitario,
                            precio_bruto=line.precio_bruto,
                            descuentos=line.descuentos,
                            importe_linea=line.importe_linea,
                            cuota_iva=line.cuota_iva,
                            doc_origen=line.doc_origen,
                            codigo_partida=line.codigo_partida,
                            descripcion_partida=line.descripcion_partida,
                            fetched_at_utc=now,
                        )
                    )

            session.execute(
                text(
                    "UPDATE albaran_documents_merge "
                    "SET selected_contrato_codigo = :codigo "
                    "WHERE id = :doc_id"
                ),
                {"codigo": selected_codigo, "doc_id": document_id},
            )

            session.commit()

    def update_contrato_pdf_paths(
        self,
        *,
        document_id: str,
        codigo_contrato: str,
        relative_path: str | None,
        web_url: str | None,
    ) -> None:
        self.initialize()
        with self._session_factory.create_session() as session:
            session.execute(
                text(
                    "UPDATE albaran_contratos_merge "
                    "SET pdf_sharepoint_relative_path = :rel, "
                    "    pdf_sharepoint_web_url = :url "
                    "WHERE document_id = :doc_id "
                    "  AND codigo_contrato = :codigo"
                ),
                {
                    "rel": relative_path,
                    "url": web_url,
                    "doc_id": document_id,
                    "codigo": codigo_contrato,
                },
            )
            session.commit()

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
                    "  AND lv.line_kind = 'synthetic_modifier' "
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

            session.execute(
                text(
                    "UPDATE albaran_line_valuations SET "
                    "    codigo_partida_final = :codpart, "
                    "    descripcion_linea = :desc, "
                    "    cantidad_albaran = :ca, "
                    "    cantidad_convertida = :cc, "
                    "    factor_conversion = :fc, "
                    "    unidad_contrato = :uc, "
                    "    precio_unitario_final = :pu, "
                    "    precio_unitario_source = "
                    "        CASE WHEN :pu IS NOT NULL "
                    "             THEN 'pdf_inference' "
                    "             ELSE 'none' END, "
                    "    importe_calculado = :imp, "
                    "    importe_source = "
                    "        CASE WHEN :imp IS NOT NULL "
                    "             THEN 'calculated' "
                    "             ELSE 'none' END "
                    "WHERE id = :lv_id "
                    "  AND line_kind = 'synthetic_modifier'"
                ),
                {
                    "codpart": self._clean_text(upd.codigo_partida_final),
                    "desc": self._clean_text(upd.descripcion_linea),
                    "ca": cantidad_val,
                    "cc": cantidad_val,
                    "fc": 1.0 if cantidad_val is not None else None,
                    "uc": self._clean_text(upd.unidad_contrato),
                    "pu": upd.precio_unitario_final,
                    "imp": nuevo_importe,
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
        conciliation_by_merge_line_id: dict[int, ConciliacionDisplay] | None = None,
    ) -> list[DisplayLine]:
        val_lines_by_merge = (
            valuation.lines_by_merge_line_id if valuation else {}
        )
        synthetic_all = (
            list(valuation.synthetic_lines) if valuation else []
        )
        conc_map: dict[int, ConciliacionDisplay] = (
            conciliation_by_merge_line_id or {}
        )

        # Agrupar sintéticas por parent_merge_line_id (int | None).
        synth_by_parent: dict[int | None, list[LineValuationPayload]] = {}
        for syn in synthetic_all:
            key = syn.parent_merge_line_id
            synth_by_parent.setdefault(key, []).append(syn)

        display: list[DisplayLine] = []

        for idx, line in enumerate(merge_lines, start=1):
            v = val_lines_by_merge.get(line.id) if line.id is not None else None

            eff_codimp = (
                (v.codigo_partida_final if v and v.codigo_partida_final else None)
                or line.codigo_imputacion
            )
            eff_cantidad = (
                v.cantidad_convertida
                if v and v.cantidad_convertida is not None
                else line.cantidad
            )
            eff_unidad = (
                (v.unidad_contrato if v and v.unidad_contrato else None)
                or (v.unidad_albaran if v else None)
            )
            eff_precio_unit = v.precio_unitario_final if v else None
            eff_importe = (
                v.importe_calculado
                if v and v.importe_calculado is not None
                else line.precio_neto
            )

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
                    concilia=conc_map.get(line.id) if line.id is not None else None,
                )
            )

            # Sintéticas que cuelgan de esta base.
            for syn in synth_by_parent.get(line.id, []):
                display.append(
                    AlbaranReviewRepository._synthetic_to_display(
                        syn=syn,
                        line_index=idx,
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
                )
            )

        return display

    @staticmethod
    def _synthetic_to_display(
        *,
        syn: LineValuationPayload,
        line_index: int,
    ) -> DisplayLine:
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
    def _to_list_item(row: AlbaranDocumentMergeOrm) -> DocumentListItem:
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

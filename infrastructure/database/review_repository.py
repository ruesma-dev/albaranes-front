# infrastructure/database/review_repository.py
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import delete, func, inspect, nullslast, or_, select, text

from domain.models.contrato_sigrid_models import ContratoFromSigrid
from domain.models.review_models import (
    ContratoPayload,
    DocumentDetailPayload,
    DocumentListFilters,
    DocumentListItem,
    KNOWN_PROVIDER_VIEWS,
    LineValuationPayload,
    MergeDocumentUpdatePayload,
    MergeLinePayload,
    PaginatedDocuments,
    ProviderSnapshot,
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

            if normalized_view == VIEW_MODE_MERGE:
                return self._build_merge_detail(
                    merge_doc=merge_doc,
                    available_views=available_views,
                    provider_snapshots=provider_snapshots_payload,
                    contratos=contratos_payload,
                    selected_contrato_codigo=selected_contrato_codigo,
                    valuation=valuation_payload,
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
                    "SELECT merge_line_id, matched_contrato_line_id, "
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
                    "       review_required "
                    "FROM albaran_line_valuations "
                    "WHERE valuation_id = :vid"
                ),
                {"vid": valuation_id},
            ).mappings().all()
        except Exception:
            session.rollback()
            line_rows = []

        lines_by_id: dict[int, LineValuationPayload] = {}
        for row in line_rows:
            merge_line_id = int(row["merge_line_id"])
            lines_by_id[merge_line_id] = LineValuationPayload(
                merge_line_id=merge_line_id,
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
            )

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
        )

    def _build_merge_detail(
        self,
        *,
        merge_doc: AlbaranDocumentMergeOrm,
        available_views: list[str],
        provider_snapshots: list[ProviderSnapshot],
        contratos: list[ContratoPayload],
        selected_contrato_codigo: str | None,
        valuation: ValuationPayload | None = None,
    ) -> DocumentDetailPayload:
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
            lines=[self._merge_line_to_payload(line) for line in merge_doc.lines],
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

            merge_line_id = int(row["merge_line_id"])

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

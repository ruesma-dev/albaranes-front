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
    MergeDocumentUpdatePayload,
    MergeLinePayload,
    PaginatedDocuments,
    ProviderSnapshot,
    VIEW_MODE_MERGE,
    normalize_view_mode,
)
from infrastructure.database.orm_models import (
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
        if self._initialized:
            return self._tables_ready()

        self._rename_legacy_tables_if_needed()
        if not self._tables_ready():
            self._initialized = True
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
            # Nueva columna: contrato seleccionado (ref. soft, no FK).
            "ALTER TABLE albaran_documents_merge ADD COLUMN IF NOT EXISTS selected_contrato_codigo VARCHAR(64)",
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

            # Cargar contratos asociados (read-only). Siempre se cargan,
            # independientemente del view_mode, para que la cabecera
            # muestre la info del contrato también en vistas por proveedor
            # (ahí la sección es informativa, no permite cambiar selección).
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

            if normalized_view == VIEW_MODE_MERGE:
                return self._build_merge_detail(
                    merge_doc=merge_doc,
                    available_views=available_views,
                    provider_snapshots=provider_snapshots_payload,
                    contratos=contratos_payload,
                    selected_contrato_codigo=selected_contrato_codigo,
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
            )

    def _build_merge_detail(
        self,
        *,
        merge_doc: AlbaranDocumentMergeOrm,
        available_views: list[str],
        provider_snapshots: list[ProviderSnapshot],
        contratos: list[ContratoPayload],
        selected_contrato_codigo: str | None,
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
        )

    # ================================================================== #
    # Re-fetch manual de contratos desde el portal.
    # Usado por ContratoRefetchService para:
    #   1) Leer el (cif, obra) actuales del merge.
    #   2) Tras llamar a Sigrid, reemplazar atómicamente los contratos
    #      guardados y fijar selected_contrato_codigo en un solo paso.
    # Deliberadamente NO toca reviewed_at_utc ni last_modified_at_utc
    # porque esto es un enriquecimiento automático, no una edición
    # humana (auditoría intacta).
    # ================================================================== #
    def get_merge_cif_and_obra(
        self,
        *,
        document_id: str,
    ) -> tuple[str | None, str | None]:
        """Devuelve (proveedor_cif, obra_codigo) del merge doc.

        Devuelve (None, None) si el documento no existe. No lanza excepción
        porque el endpoint ya valida la existencia del doc antes; aquí solo
        leemos valores.
        """
        self.initialize()
        with self._session_factory.create_session() as session:
            document = session.get(AlbaranDocumentMergeOrm, document_id)
            if document is None:
                return None, None
            return document.proveedor_cif, document.obra_codigo

    def replace_contratos_and_select(
        self,
        *,
        document_id: str,
        contratos: list[ContratoFromSigrid],
        selected_codigo: str | None,
    ) -> None:
        """Reemplaza los contratos del documento e inserta la selección.

        Flujo transaccional:
          1) DELETE de todos los contratos previos del documento.
          2) INSERT de los recibidos de Sigrid.
          3) UPDATE del selected_contrato_codigo (0 ó 1 código).
        Si algo casca, el commit no se ejecuta y queda como estaba.
        """
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
                session.add(
                    AlbaranContratoMergeOrm(
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

            # Actualiza selected_contrato_codigo validando contra los
            # contratos realmente guardados del doc. Si el usuario manda
            # un código que no existe (p.e. JSON manipulado), dejamos NULL
            # en lugar de guardar una referencia rota.
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

            session.execute(
                delete(AlbaranLineMergeOrm).where(
                    AlbaranLineMergeOrm.document_id == document.id,
                )
            )
            session.flush()

            for index, line in enumerate(payload.lines, start=1):
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

            session.commit()

        detail = self.get_document_detail(document_id)
        if detail is None:
            raise KeyError(f"Documento no encontrado tras guardar: {document_id}")
        return detail

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

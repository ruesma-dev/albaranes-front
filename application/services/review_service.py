# application/services/review_service.py
from __future__ import annotations

from domain.models.review_models import (
    DocumentDetailPayload,
    DocumentListFilters,
    MergeDocumentUpdatePayload,
    ObraResumenItem,
    PaginatedDocuments,
    ProveedorResumenItem,
    VIEW_MODE_MERGE,
)
from infrastructure.database.review_repository import AlbaranReviewRepository


class ReviewService:
    def __init__(self, repository: AlbaranReviewRepository, default_reviewer: str | None) -> None:
        self._repository = repository
        self._default_reviewer = (default_reviewer or "").strip() or None

    def initialize(self) -> bool:
        return self._repository.initialize()

    def list_documents(self, filters: DocumentListFilters) -> PaginatedDocuments:
        return self._repository.list_documents(filters)

    def list_obras_resumen(
        self, *, search: str | None = None,
    ) -> list[ObraResumenItem]:
        """Vista de OBRAS (jul 2026): resumen agregado por obra."""
        return self._repository.list_obras_resumen(search=search)

    def list_proveedores_resumen(
        self, *, search: str | None = None,
    ) -> list[ProveedorResumenItem]:
        """Vista de PROVEEDORES (jul 2026): resumen agregado por CIF."""
        return self._repository.list_proveedores_resumen(search=search)

    def get_neighbor_ids(
        self,
        *,
        document_id: str,
        filters: DocumentListFilters,
    ) -> tuple[str | None, str | None]:
        """IDs del albaran anterior y siguiente segun el orden+filtros de
        la bandeja (para los botones de navegacion del detalle)."""
        return self._repository.get_neighbor_ids(
            document_id=document_id,
            filters=filters,
        )

    def get_document(
        self,
        document_id: str,
        *,
        view_mode: str = VIEW_MODE_MERGE,
    ) -> DocumentDetailPayload | None:
        return self._repository.get_document_detail(
            document_id,
            view_mode=view_mode,
        )

    def get_contrato_pdf_url(self, document_id: str) -> str | None:
        """URL del PDF del contrato en SharePoint para abrir desde el front.
        Resuelta por consulta directa en el repositorio (robusta)."""
        return self._repository.get_contrato_pdf_url(document_id)

    def save_document(
        self,
        *,
        document_id: str,
        payload: MergeDocumentUpdatePayload,
    ) -> DocumentDetailPayload:
        if payload.approved and not payload.approved_by:
            payload.approved_by = self._default_reviewer
        # Historial: estado previo de cabecera, valoración y las líneas
        # que este guardado va a tocar.
        snaps = [
            self._repository.snapshot_row(
                table="albaran_documents_merge", row_id=document_id,
            ),
            self._undo_snap_valuation(document_id),
        ]
        for upd in (payload.valuation_line_updates or []):
            snaps.append(self._undo_snap_line(upd.valuation_line_id))
        detail = self._repository.update_document(
            document_id=document_id, payload=payload,
        )
        n_lineas = len(payload.valuation_line_updates or [])
        que = "cabecera" if not n_lineas else (
            f"cabecera + {n_lineas} línea(s)"
        )
        self._repository.record_undo(
            action="guardar",
            description=(
                f"Guardar {que} "
                f"({self._repository.doc_label(document_id=document_id)})"
            ),
            restore=snaps,
        )
        return detail

    def approve_document(self, *, document_id: str, approved_by: str | None) -> None:
        self._repository.set_approved(
            document_id=document_id,
            approved=True,
            approved_by=approved_by or self._default_reviewer,
        )

    def delete_document(
        self, *, document_id: str, deleted_by: str | None
    ) -> None:
        """Soft-delete: mueve el albarán a la papelera (is_active=False)."""
        self._repository.soft_delete_document(
            document_id=document_id,
            deleted_by=deleted_by or self._default_reviewer,
        )

    def restore_document(self, *, document_id: str) -> None:
        """Restaura un albarán desde la papelera (is_active=True)."""
        self._repository.restore_document(document_id=document_id)

    def hard_delete_document(self, *, document_id: str) -> str | None:
        """Purga DEFINITIVA: borra físicamente el albarán y todo lo que
        cuelga de él (líneas, valoración, contratos asociados, cruda).
        Irreversible. Pensado para usarse solo desde la papelera.

        Devuelve el ``source_sha256`` del documento purgado (o None) —
        el endpoint del portal lo reenvía a sv7 en el evento
        ``document-purged`` para desbloquear el dedup por contenido."""
        return self._repository.hard_delete_document(document_id=document_id)

    def update_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
        codigo_partida: str | None = None,
        descripcion: str | None = None,
        cantidad: float | None = None,
        unidad: str | None = None,
        precio_unitario: float | None = None,
        descuento: float | None = None,
        codigo_externo: str | None = None,
    ) -> bool:
        """Edición de la fila salmon (ver repositorio para la regla
        SIGRID→NUEVA al cambiar imputación/unidad/precio)."""
        etiqueta = self._undo_line_desc(valuation_line_id)
        snap_line = self._undo_snap_line(valuation_line_id)
        snap_val = self._undo_snap_valuation(document_id)
        ok = self._repository.update_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            codigo_partida=codigo_partida,
            descripcion=descripcion,
            cantidad=cantidad,
            unidad=unidad,
            precio_unitario=precio_unitario,
            descuento=descuento,
            codigo_externo=codigo_externo,
        )
        if ok:
            self._repository.record_undo(
                action="linea_edit",
                description=(
                    f"Editar línea · {etiqueta} "
                    f"({self._repository.doc_label(document_id=document_id)})"
                ),
                restore=[snap_line, snap_val],
            )
        return ok

    def remove_line_conciliacion(
        self,
        *,
        document_id: str,
        valuation_line_id: int,
    ) -> bool:
        etiqueta = self._undo_line_desc(valuation_line_id)
        brief = self._repository.line_brief(
            valuation_line_id=valuation_line_id,
        )
        snap_line = self._undo_snap_line(valuation_line_id)
        snap_derived = None
        if brief and brief.get("derived_contrato_line_id") is not None:
            snap_derived = self._repository.snapshot_row(
                table="contrato_lines_derived",
                row_id=brief["derived_contrato_line_id"],
            )
        snap_val = self._undo_snap_valuation(document_id)
        ok = self._repository.remove_line_valuation(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
        )
        if ok:
            self._repository.record_undo(
                action="linea_borrar",
                description=(
                    f"Borrar línea · {etiqueta} "
                    f"({self._repository.doc_label(document_id=document_id)})"
                ),
                reinsert=[snap_derived, snap_line],
                restore=[snap_val],
            )
        return ok

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
        etiqueta = self._undo_line_desc(valuation_line_id)
        antes = self._repository.line_brief(
            valuation_line_id=valuation_line_id,
        )
        snap_line = self._undo_snap_line(valuation_line_id)
        snap_derived_previa = None
        if antes and antes.get("derived_contrato_line_id") is not None:
            # modo contract_line borra la derivada previa: hay que poder
            # reinsertarla al deshacer.
            snap_derived_previa = self._repository.snapshot_row(
                table="contrato_lines_derived",
                row_id=antes["derived_contrato_line_id"],
            )
        snap_val = self._undo_snap_valuation(document_id)
        ok = self._repository.set_line_conciliacion(
            document_id=document_id,
            valuation_line_id=valuation_line_id,
            mode=mode,
            matched_contrato_line_id=matched_contrato_line_id,
            descripcion=descripcion,
            precio_unitario=precio_unitario,
        )
        if ok:
            # modo 'nueva' crea una derivada NUEVA: al deshacer hay que
            # borrarla (se detecta releyendo la línea tras la mutación).
            delete_created = []
            despues = self._repository.line_brief(
                valuation_line_id=valuation_line_id,
            )
            derivada_antes = (
                antes.get("derived_contrato_line_id") if antes else None
            )
            derivada_despues = (
                despues.get("derived_contrato_line_id") if despues else None
            )
            if (
                derivada_despues is not None
                and derivada_despues != derivada_antes
            ):
                delete_created.append({
                    "table": "contrato_lines_derived",
                    "id": derivada_despues,
                })
            self._repository.record_undo(
                action="conciliar",
                description=(
                    f"Cambiar conciliación · {etiqueta} "
                    f"({self._repository.doc_label(document_id=document_id)})"
                ),
                restore=[snap_line, snap_val],
                reinsert=[snap_derived_previa],
                delete_created=delete_created,
            )
        return ok

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
        snap_val = self._undo_snap_valuation(document_id)
        ok = self._repository.add_conciliacion_for_merge_line(
            document_id=document_id,
            merge_line_id=merge_line_id,
            mode=mode,
            matched_contrato_line_id=matched_contrato_line_id,
            precio_unitario=precio_unitario,
            descripcion=descripcion,
        )
        if ok:
            creada = self._repository.latest_valuation_line_for_merge(
                merge_line_id=merge_line_id,
            )
            delete_created = []
            if creada:
                if creada.get("derived_contrato_line_id") is not None:
                    delete_created.append({
                        "table": "contrato_lines_derived",
                        "id": creada["derived_contrato_line_id"],
                    })
                delete_created.append({
                    "table": "albaran_line_valuations",
                    "id": creada["id"],
                })
            self._repository.record_undo(
                action="linea_add",
                description=(
                    ("Copiar → Nueva" if mode == "nueva" else "Casar línea")
                    + f" · línea albarán {merge_line_id} "
                    + f"({self._repository.doc_label(document_id=document_id)})"
                ),
                restore=[snap_val],
                delete_created=delete_created,
            )
        return ok

    def add_standalone_valuation_line(
        self,
        *,
        document_id: str,
    ) -> int | None:
        new_id = self._repository.add_standalone_valuation_line(
            document_id=document_id,
        )
        if new_id is not None:
            brief = self._repository.line_brief(valuation_line_id=new_id)
            delete_created = []
            if brief and brief.get("derived_contrato_line_id") is not None:
                delete_created.append({
                    "table": "contrato_lines_derived",
                    "id": brief["derived_contrato_line_id"],
                })
            delete_created.append({
                "table": "albaran_line_valuations", "id": new_id,
            })
            self._repository.record_undo(
                action="linea_add",
                description=(
                    "Añadir línea "
                    f"({self._repository.doc_label(document_id=document_id)})"
                ),
                delete_created=delete_created,
            )
        return new_id

    def add_valuation_lines_from_contrato(
        self,
        *,
        document_id: str,
        contrato_line_ids: list[int],
    ) -> int:
        """Trae líneas del contrato a la tabla salmón (jul 2026): crea
        una línea salmón ya casada por cada línea de contrato elegida.
        Devuelve cuántas se crearon."""
        creadas = self._repository.add_valuation_lines_from_contrato(
            document_id=document_id,
            contrato_line_ids=contrato_line_ids,
        )
        if creadas:
            self._repository.record_undo(
                action="linea_add",
                description=(
                    f"Traer {len(creadas)} línea(s) de contrato "
                    f"({self._repository.doc_label(document_id=document_id)})"
                ),
                delete_created=[
                    {"table": "albaran_line_valuations", "id": i}
                    for i in creadas
                ],
            )
        return len(creadas)

    def list_trash_document_ids(self) -> list[str]:
        """Ids de los albaranes en la papelera (para vaciarla)."""
        return self._repository.list_trash_document_ids()

    # ------------------------------------------------------------------ #
    # DESHACER (jul 2026): los ganchos viven AQUÍ (snapshot antes de
    # delegar en el repo, registro después solo si la mutación fue bien)
    # para no tocar la lógica interna de los métodos mutadores. El motor
    # (tabla undo_log, aplicar restore/reinsert/delete_created) está en
    # el repositorio. Ver nota de no-atomicidad en el repositorio.
    # ------------------------------------------------------------------ #
    def list_undo(self, *, limit: int = 15) -> list[dict]:
        return self._repository.list_undo(limit=limit)

    def undo_last(self) -> dict:
        return self._repository.undo_last()

    def _undo_snap_line(self, valuation_line_id: int) -> dict | None:
        return self._repository.snapshot_row(
            table="albaran_line_valuations", row_id=valuation_line_id,
        )

    def _undo_snap_valuation(self, document_id: str) -> dict | None:
        vid = self._repository.valuation_header_id(document_id=document_id)
        if vid is None:
            return None
        return self._repository.snapshot_row(
            table="albaran_valuations", row_id=vid,
        )

    def _undo_line_desc(self, valuation_line_id: int) -> str:
        b = self._repository.line_brief(valuation_line_id=valuation_line_id)
        if not b:
            return f"línea {valuation_line_id}"
        partida = b.get("codigo_partida_final") or ""
        descr = (b.get("descripcion_linea") or "").strip()
        if len(descr) > 40:
            descr = descr[:37] + "…"
        etiqueta = " ".join(x for x in (partida, descr) if x)
        return etiqueta or f"línea {valuation_line_id}"

    def unapprove_document(self, *, document_id: str) -> None:
        self._repository.set_approved(
            document_id=document_id,
            approved=False,
            approved_by=None,
        )

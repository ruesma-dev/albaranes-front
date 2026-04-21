# application/services/contrato_refetch_service.py
from __future__ import annotations

import logging

from application.services.obra_code_normalizer import normalize_obra_code
from domain.models.contrato_refetch_models import ContratoRefetchOutcome
from domain.models.contrato_sigrid_models import ContratoFromSigrid
from domain.ports.contrato_refetch_port import (
    ContratoLookupClient,
    ContratoPdfStorage,
)
from infrastructure.database.review_repository import AlbaranReviewRepository

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[contrato-refetch]"


class ContratoRefetchService:
    """Orquesta la re-búsqueda manual de contratos desde el portal.

    Flujo (espejo del servicio 3 pero lanzado por el usuario):
      1. Lee (cif, obra_codigo) CUALES ESTÉN ACTUALMENTE EN EL MERGE
         (ojo: si el usuario acaba de guardar, son los valores nuevos).
      2. Normaliza obra_codigo con la MISMA regla que servicio 3.
      3. Si falta CIF o no valida obra → devuelve 'skipped_missing_data'
         (no llama a Sigrid — ahorra una llamada tonta).
      4. Llama a Sigrid.
      5. ANTES del replace, lee el mapa de PDFs ya subidos para cada
         contrato; si el nuevo ``gra_rep_ide`` coincide con el previo,
         reutiliza los paths (inyectándolos en el DTO antes del replace).
      6. Reemplaza los contratos del documento (delete + insert).
      7. Si exactamente 1 → auto-selecciona. Si 0 o varios → NULL.
      8. Para los contratos con ``gra_rep_ide`` y SIN reutilización:
         descarga el PDF de Sigrid, lo sube a SharePoint y actualiza
         los paths en BBDD. Best-effort por contrato (un fallo no rompe
         los otros, ni afecta al outcome global del refetch).
      9. Devuelve el outcome para que el endpoint dé feedback al front.

    A diferencia del servicio 3 (best-effort silencioso), aquí sí
    propagamos 'sigrid_error' de la primera fase (SQL) al usuario.
    Los fallos de subida del PDF NO modifican el outcome — solo se
    loguean; el contrato queda persistido sin PDF y el próximo refetch
    reintenta.

    ``pdf_storage`` es OPCIONAL: si no se inyecta, toda la lógica de
    descarga/subida se omite (compatibilidad con despliegues donde no
    esté configurado SharePoint).
    """

    def __init__(
        self,
        *,
        client: ContratoLookupClient,
        repository: AlbaranReviewRepository,
        pdf_storage: ContratoPdfStorage | None = None,
        enabled: bool = True,
    ) -> None:
        self._client = client
        self._repository = repository
        self._pdf_storage = pdf_storage
        self._enabled = enabled
        logger.info(
            "%s ContratoRefetchService INSTANCIADO "
            "(enabled=%s pdf_storage=%s)",
            _LOG_PREFIX,
            enabled,
            type(pdf_storage).__name__ if pdf_storage is not None else "None",
        )

    def refetch(self, *, document_id: str) -> ContratoRefetchOutcome:
        logger.info("%s refetch() INVOCADO document_id=%s", _LOG_PREFIX, document_id)

        if not self._enabled:
            return ContratoRefetchOutcome(
                status="skipped_missing_data",
                count=0,
                selected_contrato_codigo=None,
                message="La búsqueda de contratos está deshabilitada en este servicio.",
                cif=None,
                obra_codigo=None,
            )

        cif, obra_raw = self._repository.get_merge_cif_and_obra(
            document_id=document_id,
        )
        cif_clean = (cif or "").strip().upper().replace(" ", "") or None
        obra_norm = normalize_obra_code(obra_raw)

        logger.info(
            "%s Leídos del merge: cif=%r obra_raw=%r → normalizados cif=%r obra=%r",
            _LOG_PREFIX,
            cif,
            obra_raw,
            cif_clean,
            obra_norm,
        )

        if not cif_clean or not obra_norm:
            message_parts = []
            if not cif_clean:
                message_parts.append("CIF del proveedor vacío")
            if not obra_norm:
                message_parts.append(
                    f"código de obra inválido ({obra_raw!r}); debe ser 1–4 dígitos"
                )
            return ContratoRefetchOutcome(
                status="skipped_missing_data",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se consultó el ERP: "
                    + " y ".join(message_parts)
                    + ". Corrige los datos del albarán y pulsa 'Guardar y volver a buscar'."
                ),
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        try:
            contratos = self._client.fetch_contratos(
                cif_proveedor=cif_clean,
                codigo_obra_normalizado=obra_norm,
            )
        except Exception as exc:
            logger.exception(
                "%s ERROR llamando a Sigrid. document_id=%s exc=%r",
                _LOG_PREFIX,
                document_id,
                exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=0,
                selected_contrato_codigo=None,
                message=(
                    "No se pudo contactar con el ERP para consultar contratos. "
                    "Inténtalo de nuevo en unos segundos. "
                    f"Detalle: {exc}"
                ),
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        logger.info("%s Sigrid devolvió %s contratos", _LOG_PREFIX, len(contratos))

        # ----------------------------------------------------------- #
        # Mapa de PDFs ya guardados ANTES del replace.
        # Si el repo fallase (tabla no existe, etc.), lo tratamos como
        # mapa vacío — no bloqueamos el refetch.
        # ----------------------------------------------------------- #
        existing_pdfs: dict[str, tuple[int | None, str | None, str | None]] = {}
        try:
            existing_pdfs = self._repository.get_existing_pdf_paths(
                document_id=document_id,
            )
        except Exception:
            logger.exception(
                "%s No se pudo leer mapa de PDFs existentes; se ignora.",
                _LOG_PREFIX,
            )

        # ----------------------------------------------------------- #
        # Reutilización: si el gra_rep_ide nuevo == previo, inyecta
        # paths existentes en el DTO. Marca los que SÍ necesitan subida
        # nueva para procesarlos en paso 8.
        # ----------------------------------------------------------- #
        reused_count = 0
        contratos_final: list[ContratoFromSigrid] = []
        pending_pdf_indices: list[int] = []
        for idx, contrato in enumerate(contratos):
            prev = existing_pdfs.get(contrato.codigo_contrato)
            if (
                prev is not None
                and prev[0] is not None
                and contrato.gra_rep_ide is not None
                and prev[0] == contrato.gra_rep_ide
                and prev[1] is not None
            ):
                contratos_final.append(
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
                        gra_rep_ide=contrato.gra_rep_ide,
                        pdf_sharepoint_relative_path=prev[1],
                        pdf_sharepoint_web_url=prev[2],
                        lines=contrato.lines,
                    )
                )
                reused_count += 1
            else:
                contratos_final.append(contrato)
                if contrato.gra_rep_ide is not None and self._pdf_storage is not None:
                    pending_pdf_indices.append(idx)

        logger.info(
            "%s PDFs reutilizados=%s pendientes_descargar=%s",
            _LOG_PREFIX,
            reused_count,
            len(pending_pdf_indices),
        )

        # ----------------------------------------------------------- #
        # Replace atómico + auto-selección si hay uno solo.
        # ----------------------------------------------------------- #
        selected = None
        if len(contratos_final) == 1:
            selected = contratos_final[0].codigo_contrato

        try:
            self._repository.replace_contratos_and_select(
                document_id=document_id,
                contratos=contratos_final,
                selected_codigo=selected,
            )
        except Exception as exc:
            logger.exception(
                "%s ERROR persistiendo contratos. document_id=%s exc=%r",
                _LOG_PREFIX,
                document_id,
                exc,
            )
            return ContratoRefetchOutcome(
                status="sigrid_error",
                count=len(contratos_final),
                selected_contrato_codigo=None,
                message=f"Error guardando los contratos en BBDD: {exc}",
                cif=cif_clean,
                obra_codigo=obra_norm,
            )

        # ----------------------------------------------------------- #
        # Descarga + subida de PDFs para los contratos NO reutilizados.
        # Best-effort: fallos no afectan al outcome.
        # ----------------------------------------------------------- #
        if self._pdf_storage is not None:
            for idx in pending_pdf_indices:
                self._download_and_store_pdf(
                    document_id=document_id,
                    contrato=contratos_final[idx],
                )
        elif any(c.gra_rep_ide is not None for c in contratos_final):
            logger.info(
                "%s Hay %s contrato(s) con PDF pero pdf_storage=None; "
                "se omite descarga/subida.",
                _LOG_PREFIX,
                sum(
                    1
                    for c in contratos_final
                    if c.gra_rep_ide is not None
                ),
            )

        # ----------------------------------------------------------- #
        # Outcome final.
        # ----------------------------------------------------------- #
        if len(contratos_final) == 0:
            status = "no_results"
            message = (
                f"Se consultó el ERP con CIF {cif_clean} y obra {obra_norm}, "
                "pero no existe ningún contrato para esa combinación."
            )
        elif len(contratos_final) == 1:
            status = "found_single"
            message = (
                f"Se encontró 1 contrato y se ha asignado automáticamente "
                f"({contratos_final[0].nombre_contrato or contratos_final[0].codigo_contrato})."
            )
        else:
            status = "found_multiple"
            message = (
                f"Se encontraron {len(contratos_final)} contratos. "
                "Selecciona el que corresponda en el desplegable."
            )

        return ContratoRefetchOutcome(
            status=status,
            count=len(contratos_final),
            selected_contrato_codigo=selected,
            message=message,
            cif=cif_clean,
            obra_codigo=obra_norm,
        )

    def _download_and_store_pdf(
        self,
        *,
        document_id: str,
        contrato: ContratoFromSigrid,
    ) -> None:
        """Descarga + sube + actualiza BBDD para un contrato concreto.

        Silencia todas las excepciones: el objetivo es que un PDF con
        problema no impida procesar los demás, ni afecte al outcome del
        refetch visto por el usuario.
        """
        if self._pdf_storage is None or contrato.gra_rep_ide is None:
            return

        try:
            payload = self._client.download_contrato_pdf(
                gra_rep_ide=contrato.gra_rep_ide,
            )
        except Exception:
            logger.exception(
                "%s FALLO descarga PDF codigo=%s gra_rep_ide=%s",
                _LOG_PREFIX,
                contrato.codigo_contrato,
                contrato.gra_rep_ide,
            )
            return

        if payload is None:
            logger.warning(
                "%s Descarga PDF devolvió vacío codigo=%s gra_rep_ide=%s",
                _LOG_PREFIX,
                contrato.codigo_contrato,
                contrato.gra_rep_ide,
            )
            return

        try:
            stored = self._pdf_storage.upload_contrato_pdf(
                filename=payload.filename,
                file_bytes=payload.content,
                codigo_contrato=contrato.codigo_contrato,
                gra_rep_ide=contrato.gra_rep_ide,
            )
        except Exception:
            logger.exception(
                "%s FALLO subida PDF codigo=%s gra_rep_ide=%s",
                _LOG_PREFIX,
                contrato.codigo_contrato,
                contrato.gra_rep_ide,
            )
            return

        try:
            self._repository.update_contrato_pdf_paths(
                document_id=document_id,
                codigo_contrato=contrato.codigo_contrato,
                relative_path=stored.relative_path,
                web_url=stored.web_url,
            )
            logger.info(
                "%s PDF OK codigo=%s -> %s",
                _LOG_PREFIX,
                contrato.codigo_contrato,
                stored.relative_path,
            )
        except Exception:
            logger.exception(
                "%s FALLO persistiendo paths del PDF codigo=%s",
                _LOG_PREFIX,
                contrato.codigo_contrato,
            )

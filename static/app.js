// static/app.js
//
// Portal de revisión — interacciones del detalle editable:
//   - Selector de contrato cuando hay varios (y sincronía con el botón
//     "Abrir contrato en SharePoint" de la cabecera).
//   - Edición inline de líneas: el <tbody> viene PRE-RENDERIZADO desde
//     el servidor para mezclar datos extraídos y valorados. El JS solo
//     añade/elimina filas y recolecta para guardar.
//   - Guardar/Aprobar contra PUT /api/documents/{id}.
//   - Botones "Guardar y volver a buscar" / "Solo volver a buscar"
//     dentro del alert amarillo cuando hay 0 contratos.
//
// Los inputs con data-field terminado en '_display' son read-only y
// reflejan valores valorados (unidad, precio_unitario del contrato).
// NO se envían al backend: collectLines() los ignora.

(function () {
    "use strict";

    // --------------------------------------------------------------- //
    // Mapa de PDFs por contrato (solo si hay varios)
    // --------------------------------------------------------------- //
    let contratosPdfMap = {};
    const pdfMapTag = document.getElementById("contratos-pdf-map");
    if (pdfMapTag) {
        try {
            const pairs = JSON.parse(pdfMapTag.textContent) || [];
            if (Array.isArray(pairs)) {
                contratosPdfMap = Object.fromEntries(pairs);
            } else if (pairs && typeof pairs === "object") {
                contratosPdfMap = pairs;
            }
        } catch (exc) {
            console.warn("No se pudo parsear contratos-pdf-map:", exc);
            contratosPdfMap = {};
        }
    }

    function findHeaderContratoBtn() {
        const anchors = document.querySelectorAll(".header-actions a");
        for (let i = 0; i < anchors.length; i++) {
            const text = (anchors[i].textContent || "").trim();
            if (text === "Abrir contrato en SharePoint") {
                return anchors[i];
            }
        }
        return null;
    }

    function wireContratoSelector() {
        const selector = document.getElementById("selected_contrato_codigo");
        if (!selector || selector.tagName !== "SELECT") return;
        const cards = document.querySelectorAll(".js-contrato-card");
        const headerBtn = findHeaderContratoBtn();

        function syncHeaderBtn(codigo) {
            if (!headerBtn) return;
            const url = codigo ? contratosPdfMap[codigo] : null;
            if (url) {
                headerBtn.href = url;
                headerBtn.title = "Abrir el PDF del contrato " + codigo;
                headerBtn.style.display = "";
            } else {
                headerBtn.style.display = "none";
            }
        }

        selector.addEventListener("change", function () {
            const codigo = selector.value;
            cards.forEach(function (card) {
                card.style.display = card.dataset.codigo === codigo ? "" : "none";
            });
            syncHeaderBtn(codigo);
        });
        syncHeaderBtn(selector.value);
    }

    wireContratoSelector();

    // --------------------------------------------------------------- //
    // Edición de líneas del albarán (solo en vista merge)
    // --------------------------------------------------------------- //
    const dataTag = document.getElementById("document-data");
    if (!dataTag) {
        return; // vista read-only de proveedor
    }

    const linesTable = document.getElementById("lines-table");
    const linesBody = linesTable ? linesTable.querySelector("tbody") : null;
    if (!linesBody) return;

    const documentId =
        (dataTag.dataset && dataTag.dataset.documentId) ||
        window.reviewDocumentId ||
        (function () {
            try { return JSON.parse(dataTag.textContent).id; } catch (_) { return null; }
        })();

    const addLineBtn = document.getElementById("add-line-btn");
    const saveBtn = document.getElementById("save-btn");
    const approveBtn = document.getElementById("approve-btn");
    const saveAndRefetchBtn = document.getElementById("save-and-refetch-btn");
    const refetchOnlyBtn = document.getElementById("refetch-only-btn");
    const refetchStatus = document.getElementById("refetch-status");

    // --------------------------------------------------------------- //
    // Renumerar la columna '#' tras añadir/eliminar filas.
    // --------------------------------------------------------------- //
    function reindexRows() {
        const rows = linesBody.querySelectorAll("tr");
        rows.forEach(function (row, idx) {
            const first = row.querySelector("td");
            if (first) first.textContent = String(idx + 1);
        });
    }

    // --------------------------------------------------------------- //
    // Crear una fila nueva (usada al pulsar "Añadir línea"). Los campos
    // de valoración ('unidad_display', 'precio_unitario_display') se
    // crean readonly y vacíos: nunca habrá valoración para filas que
    // acaba de añadir el usuario hasta que el servicio 6 vuelva a
    // pasar (fuera del alcance de esta pasada).
    //
    // Orden de columnas:
    //   # | Cód imput. | Concepto | Cantidad | Unidad |
    //     Precio unit. | Importe | Descuento | Código | [Acciones]
    // Más un input HIDDEN para 'precio' (extraído del albarán) que
    // viaja en el payload al guardar pero no ocupa celda visual.
    // --------------------------------------------------------------- //
    function buildEmptyRow(index) {
        const tr = document.createElement("tr");
        tr.dataset.lineId = "";
        // Sub-tanda 2D: filas nuevas son siempre del albarán-merge.
        // Las sintéticas solo las crea el valorador.
        tr.dataset.lineKind = "from_albaran";
        tr.dataset.valuationLineId = "";

        function cell(inner) {
            const td = document.createElement("td");
            td.appendChild(inner);
            tr.appendChild(td);
        }

        function txt(field) {
            const el = document.createElement("input");
            el.type = "text";
            el.dataset.field = field;
            return el;
        }
        function num(field) {
            const el = document.createElement("input");
            el.type = "number";
            el.step = "any";
            el.dataset.field = field;
            return el;
        }
        function area(field) {
            const el = document.createElement("textarea");
            el.dataset.field = field;
            return el;
        }
        function ro(field) {
            const el = document.createElement("input");
            el.type = "text";
            el.dataset.field = field;
            el.readOnly = true;
            el.className = "readonly-cell";
            return el;
        }

        const idxCell = document.createElement("td");
        idxCell.textContent = String(index + 1);
        tr.appendChild(idxCell);

        cell(txt("codigo_imputacion"));
        cell(area("concepto"));
        cell(num("cantidad"));
        cell(ro("unidad_display"));
        cell(ro("precio_unitario_display"));
        // Importe (el template V3 usa data-field=importe; al enviar al
        // backend collectLinesAndValuationUpdates lo mapea a precio_neto).
        cell(num("importe"));
        cell(num("descuento"));
        cell(txt("codigo"));

        // 'precio' (del albarán) sobrevive oculto, como input hidden
        // pegado a la primera celda para que siga en el form.
        const hiddenPrecio = document.createElement("input");
        hiddenPrecio.type = "hidden";
        hiddenPrecio.dataset.field = "precio";
        tr.firstChild.appendChild(hiddenPrecio);

        const actionsTd = document.createElement("td");
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn danger small js-remove-line";
        btn.textContent = "Eliminar";
        actionsTd.appendChild(btn);
        tr.appendChild(actionsTd);

        return tr;
    }

    // Delegación: clicks en botones "Eliminar" (pintados por el servidor
    // o por buildEmptyRow).
    linesBody.addEventListener("click", function (evt) {
        const target = evt.target;
        if (target && target.classList && target.classList.contains("js-remove-line")) {
            const tr = target.closest("tr");
            if (tr) {
                tr.remove();
                reindexRows();
            }
        }
    });

    if (addLineBtn) {
        addLineBtn.addEventListener("click", function () {
            const row = buildEmptyRow(linesBody.querySelectorAll("tr").length);
            linesBody.appendChild(row);
            reindexRows();
        });
    }

    function readNumericOrNull(input) {
        if (!input) return null;
        const value = (input.value || "").trim();
        if (!value) return null;
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    }

    function readTextOrNull(input) {
        if (!input) return null;
        const value = (input.value || "").trim();
        return value || null;
    }

    // --------------------------------------------------------------- //
    // Recolectar líneas para enviar al backend.
    //
    // Sub-tanda 2D: diferenciamos dos tipos de fila por data-line-kind:
    //
    //   - "from_albaran": líneas reales del albarán-merge. Se envían en
    //     payload.lines tal como antes. data-line-id lleva el id en
    //     albaran_lines_merge.
    //
    //   - "synthetic_modifier": líneas sintéticas de valoración
    //     (incrementos por año, consistencia, árido, aditivo, residuos,
    //     tiempo). No viven en el merge sino en albaran_line_valuations.
    //     Se envían en payload.valuation_line_updates identificadas por
    //     data-valuation-line-id.
    //
    // Si una fila no tiene data-line-kind (caso raro: código antiguo)
    // la tratamos como from_albaran por compatibilidad.
    //
    // Los campos 'unidad_display' y 'precio_unitario_display' siguen
    // siendo puramente informativos y no se envían.
    //
    // Nota sobre nombres: el backend ``MergeLinePayload`` usa
    // 'precio_neto' como nombre de campo para el importe de la línea
    // (herencia del modelo extraído). El template V3 usa data-field=
    // "importe" como nombre más claro para el revisor; al recolectar
    // lo mapeamos: importe → precio_neto.
    // --------------------------------------------------------------- //
    function collectLinesAndValuationUpdates() {
        const merge_lines = [];
        const valuation_updates = [];
        const rows = linesBody.querySelectorAll("tr");
        let merge_index = 0;

        rows.forEach(function (row) {
            const byField = {};
            row.querySelectorAll("[data-field]").forEach(function (el) {
                byField[el.dataset.field] = el;
            });

            const kind = row.dataset.lineKind || "from_albaran";

            if (kind === "synthetic_modifier") {
                const vlid = row.dataset.valuationLineId;
                if (!vlid) {
                    // Sin valuation_line_id no podemos UPDATE-arla;
                    // silencioso: no se envía.
                    return;
                }
                valuation_updates.push({
                    valuation_line_id: Number(vlid),
                    codigo_partida_final: readTextOrNull(byField.codigo_imputacion),
                    descripcion_linea: readTextOrNull(byField.concepto),
                    cantidad_albaran: readNumericOrNull(byField.cantidad),
                    unidad_contrato: readTextOrNull(byField.unidad),
                    precio_unitario_final: readNumericOrNull(byField.precio_unitario),
                    importe_calculado: readNumericOrNull(byField.importe),
                });
                return;
            }

            // from_albaran (o fila nueva añadida por el usuario).
            merge_index += 1;
            merge_lines.push({
                id: row.dataset.lineId ? Number(row.dataset.lineId) : null,
                line_index: merge_index,
                codigo_imputacion: readTextOrNull(byField.codigo_imputacion),
                concepto: readTextOrNull(byField.concepto),
                cantidad: readNumericOrNull(byField.cantidad),
                precio: readNumericOrNull(byField.precio),
                descuento: readNumericOrNull(byField.descuento),
                // 'importe' en V3 == 'precio_neto' en el schema del backend.
                precio_neto: readNumericOrNull(byField.importe),
                codigo: readTextOrNull(byField.codigo),
                // 'unidad_display' y 'precio_unitario_display' NO se envían.
            });
        });

        return { merge_lines: merge_lines, valuation_updates: valuation_updates };
    }

    // Compatibilidad hacia atrás: algún código externo podría llamar
    // collectLines(). Exponemos una versión reducida que devuelve solo
    // las líneas del merge.
    function collectLines() {
        return collectLinesAndValuationUpdates().merge_lines;
    }

    function collectSelectedContratoCodigo() {
        const el = document.getElementById("selected_contrato_codigo");
        if (!el) return null;
        const raw = (el.value || "").trim();
        return raw || null;
    }

    function collectPayload(markApproved) {
        const getInput = function (id) {
            const el = document.getElementById(id);
            if (!el) return null;
            const value = (el.value || "").trim();
            return value || null;
        };
        // Sub-tanda 2D: una sola lectura del DOM para obtener tanto
        // las líneas del merge como las ediciones de sintéticas.
        const collected = collectLinesAndValuationUpdates();
        return {
            proveedor_nombre: getInput("proveedor_nombre"),
            proveedor_cif: getInput("proveedor_cif"),
            fecha: getInput("fecha"),
            numero_albaran: getInput("numero_albaran"),
            forma_pago: getInput("forma_pago"),
            obra_codigo: getInput("obra_codigo"),
            obra_nombre: getInput("obra_nombre"),
            obra_direccion: getInput("obra_direccion"),
            review_notes: getInput("review_notes"),
            approved_by: getInput("approved_by"),
            approved: Boolean(markApproved),
            selected_contrato_codigo: collectSelectedContratoCodigo(),
            lines: collected.merge_lines,
            valuation_line_updates: collected.valuation_updates,
        };
    }

    async function sendSave(markApproved) {
        const payload = collectPayload(markApproved);
        const response = await fetch(`/api/documents/${documentId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        if (!response.ok) {
            let detail = response.statusText;
            try {
                const body = await response.json();
                detail = body.detail || detail;
            } catch (_) {}
            alert("Error al guardar: " + detail);
            return false;
        }
        const body = await response.json();
        window.location.href = body.redirect_url;
        return true;
    }

    if (saveBtn) saveBtn.addEventListener("click", function () { sendSave(false); });
    if (approveBtn) approveBtn.addEventListener("click", function () { sendSave(true); });

    // --------------------------------------------------------------- //
    // Re-búsqueda manual de contratos (alert amarillo de "0 contratos")
    // --------------------------------------------------------------- //
    function paintStatus(kind, text) {
        if (!refetchStatus) return;
        refetchStatus.hidden = false;
        refetchStatus.className = "refetch-status refetch-" + kind;
        refetchStatus.textContent = text;
    }

    function setButtonsDisabled(disabled) {
        [saveAndRefetchBtn, refetchOnlyBtn].forEach(function (btn) {
            if (btn) btn.disabled = disabled;
        });
    }

    async function refetchContratos() {
        const response = await fetch(
            `/api/documents/${documentId}/re-fetch-contratos`,
            { method: "POST", headers: { "Content-Type": "application/json" } }
        );
        if (!response.ok) {
            let detail = response.statusText;
            try {
                const body = await response.json();
                detail = body.detail || detail;
            } catch (_) {}
            throw new Error(detail);
        }
        return response.json();
    }

    async function handleSaveAndRefetch() {
        setButtonsDisabled(true);
        paintStatus("info", "Guardando cambios…");
        try {
            const savePayload = collectPayload(false);
            const saveResp = await fetch(`/api/documents/${documentId}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(savePayload),
            });
            if (!saveResp.ok) {
                let detail = saveResp.statusText;
                try {
                    const body = await saveResp.json();
                    detail = body.detail || detail;
                } catch (_) {}
                paintStatus("error", "Error al guardar: " + detail);
                setButtonsDisabled(false);
                return;
            }
            paintStatus("info", "Consultando ERP…");
            const outcome = await refetchContratos();
            handleRefetchOutcome(outcome);
        } catch (exc) {
            paintStatus("error", "Error: " + (exc.message || exc));
            setButtonsDisabled(false);
        }
    }

    async function handleRefetchOnly() {
        setButtonsDisabled(true);
        paintStatus("info", "Consultando ERP…");
        try {
            const outcome = await refetchContratos();
            handleRefetchOutcome(outcome);
        } catch (exc) {
            paintStatus("error", "Error: " + (exc.message || exc));
            setButtonsDisabled(false);
        }
    }

    function handleRefetchOutcome(outcome) {
        const kind = {
            found_single: "success",
            found_multiple: "success",
            no_results: "warning",
            skipped_missing_data: "warning",
            sigrid_error: "error",
        }[outcome.status] || "info";

        paintStatus(kind, outcome.message || "Sin mensaje.");
        if (outcome.count > 0) {
            setTimeout(function () { window.location.reload(); }, 700);
            return;
        }
        setButtonsDisabled(false);
    }

    if (saveAndRefetchBtn) saveAndRefetchBtn.addEventListener("click", handleSaveAndRefetch);
    if (refetchOnlyBtn) refetchOnlyBtn.addEventListener("click", handleRefetchOnly);
})();

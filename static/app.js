// static/app.js
//
// Portal de revisión — interacciones del detalle editable:
//   - Selector de contrato cuando hay varios.
//   - Edición inline de líneas.
//   - Guardar/Aprobar contra PUT /api/documents/{id}.
//   - Botones "Guardar y volver a buscar" / "Solo volver a buscar"
//     dentro del alert amarillo cuando hay 0 contratos. Llaman a
//     POST /api/documents/{id}/re-fetch-contratos.
//
// Formateo de fechas e importes: se hace server-side con filtros
// Jinja2 (fecha_int_iso, importe_eur). El JS no formatea presentación.

(function () {
    "use strict";

    // --------------------------------------------------------------- //
    // Selector de contrato cuando hay varios (muestra la card elegida).
    // --------------------------------------------------------------- //
    function wireContratoSelector() {
        const selector = document.getElementById("selected_contrato_codigo");
        if (!selector || selector.tagName !== "SELECT") return;
        const cards = document.querySelectorAll(".js-contrato-card");
        selector.addEventListener("change", function () {
            const codigo = selector.value;
            cards.forEach(function (card) {
                card.style.display = card.dataset.codigo === codigo ? "" : "none";
            });
        });
    }

    wireContratoSelector();

    // --------------------------------------------------------------- //
    // Edición de líneas del albarán (solo en vista merge)
    // --------------------------------------------------------------- //
    const dataTag = document.getElementById("document-data");
    if (!dataTag) {
        // Vista por proveedor (read-only) → no cableamos nada más.
        return;
    }

    let documentData = null;
    try {
        documentData = JSON.parse(dataTag.textContent);
    } catch (exc) {
        console.error("No se pudo parsear document-data JSON:", exc);
        return;
    }

    const documentId =
        (dataTag.dataset && dataTag.dataset.documentId) ||
        window.reviewDocumentId ||
        documentData.id;

    const linesBody = document.querySelector("#lines-table tbody");
    const addLineBtn = document.getElementById("add-line-btn");
    const saveBtn = document.getElementById("save-btn");
    const approveBtn = document.getElementById("approve-btn");
    const saveAndRefetchBtn = document.getElementById("save-and-refetch-btn");
    const refetchOnlyBtn = document.getElementById("refetch-only-btn");
    const refetchStatus = document.getElementById("refetch-status");

    function lineToRow(line, index) {
        const tr = document.createElement("tr");
        tr.dataset.lineId = line.id !== null && line.id !== undefined ? String(line.id) : "";

        function addCell(type, value, field) {
            const td = document.createElement("td");
            const input = document.createElement(type === "textarea" ? "textarea" : "input");
            if (type !== "textarea") input.type = type;
            input.dataset.field = field;
            if (value !== null && value !== undefined) input.value = value;
            td.appendChild(input);
            tr.appendChild(td);
        }

        const idxCell = document.createElement("td");
        idxCell.textContent = String(index + 1);
        tr.appendChild(idxCell);

        addCell("text", line.codigo_imputacion, "codigo_imputacion");
        addCell("textarea", line.concepto, "concepto");
        addCell("number", line.cantidad, "cantidad");
        addCell("number", line.precio, "precio");
        addCell("number", line.descuento, "descuento");
        addCell("number", line.precio_neto, "precio_neto");
        addCell("text", line.codigo, "codigo");

        const actionsCell = document.createElement("td");
        const rm = document.createElement("button");
        rm.type = "button";
        rm.className = "btn danger small";
        rm.textContent = "Eliminar";
        rm.addEventListener("click", function () {
            tr.remove();
            reindexRows();
        });
        actionsCell.appendChild(rm);
        tr.appendChild(actionsCell);

        return tr;
    }

    function reindexRows() {
        const rows = linesBody.querySelectorAll("tr");
        rows.forEach(function (row, idx) {
            const first = row.querySelector("td");
            if (first) first.textContent = String(idx + 1);
        });
    }

    function renderLines(lines) {
        linesBody.innerHTML = "";
        (lines || []).forEach(function (line, idx) {
            linesBody.appendChild(lineToRow(line, idx));
        });
    }

    renderLines(documentData.lines);

    if (addLineBtn) {
        addLineBtn.addEventListener("click", function () {
            const row = lineToRow(
                {
                    id: null,
                    codigo_imputacion: "",
                    concepto: "",
                    cantidad: null,
                    precio: null,
                    descuento: null,
                    precio_neto: null,
                    codigo: "",
                },
                linesBody.querySelectorAll("tr").length
            );
            linesBody.appendChild(row);
            reindexRows();
        });
    }

    function readNumericOrNull(input) {
        const value = (input.value || "").trim();
        if (!value) return null;
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    }

    function readTextOrNull(input) {
        const value = (input.value || "").trim();
        return value || null;
    }

    function collectLines() {
        const out = [];
        const rows = linesBody.querySelectorAll("tr");
        rows.forEach(function (row, idx) {
            const byField = {};
            row.querySelectorAll("[data-field]").forEach(function (el) {
                byField[el.dataset.field] = el;
            });
            out.push({
                id: row.dataset.lineId ? Number(row.dataset.lineId) : null,
                line_index: idx + 1,
                codigo_imputacion: readTextOrNull(byField.codigo_imputacion),
                concepto: readTextOrNull(byField.concepto),
                cantidad: readNumericOrNull(byField.cantidad),
                precio: readNumericOrNull(byField.precio),
                descuento: readNumericOrNull(byField.descuento),
                precio_neto: readNumericOrNull(byField.precio_neto),
                codigo: readTextOrNull(byField.codigo),
            });
        });
        return out;
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
            lines: collectLines(),
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
        if (markApproved) {
            // Solo redirigimos en el flujo de aprobar — en Guardar puro
            // también, mantenemos el comportamiento original.
            const body = await response.json();
            window.location.href = body.redirect_url;
            return true;
        }
        // Guardar sin aprobar: redirigir o recargar. Mantenemos el
        // redirect como antes.
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
            // 1) Guardamos los campos SIN aprobar. En vez de redirigir,
            //    hacemos la llamada directa al API de guardar.
            const savePayload = collectPayload(false);
            // forzamos que el flag de "approved" no cambie nada
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

            // 2) Re-búsqueda.
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

        // Si hemos encontrado contratos, recargamos para pintar la card
        // (o el desplegable) en lugar del alert amarillo.
        if (outcome.count > 0) {
            setTimeout(function () {
                window.location.reload();
            }, 700);
            return;
        }
        // Si seguimos sin resultados o ha habido error, mantenemos al
        // usuario en la página para que pueda editar/volver a intentar.
        setButtonsDisabled(false);
    }

    if (saveAndRefetchBtn) {
        saveAndRefetchBtn.addEventListener("click", handleSaveAndRefetch);
    }
    if (refetchOnlyBtn) {
        refetchOnlyBtn.addEventListener("click", handleRefetchOnly);
    }
})();

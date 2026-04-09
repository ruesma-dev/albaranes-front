// static/app.js
(function () {
    const dataTag = document.getElementById("document-data");
    if (!dataTag) {
        return;
    }

    const documentId = resolveDocumentId(dataTag);
    if (!documentId) {
        console.error("No se pudo resolver el documentId para guardar la revisión.");
        return;
    }

    const documentData = JSON.parse(dataTag.textContent || "{}");
    const tbody = document.querySelector("#lines-table tbody");
    const addLineBtn = document.getElementById("add-line-btn");
    const saveBtn = document.getElementById("save-btn");
    const approveBtn = document.getElementById("approve-btn");

    const renderRows = (lines) => {
        tbody.innerHTML = "";
        lines.forEach((line, index) => {
            const tr = document.createElement("tr");
            tr.dataset.rowIndex = String(index + 1);
            tr.innerHTML = `
                <td class="line-number">${index + 1}</td>
                <td><input type="text" data-field="codigo_imputacion" value="${escapeHtml(line.codigo_imputacion || "")}"></td>
                <td><textarea data-field="concepto">${escapeHtml(line.concepto || "")}</textarea></td>
                <td><input type="number" step="0.0001" data-field="cantidad" value="${nullableNumber(line.cantidad)}"></td>
                <td><input type="number" step="0.0001" data-field="precio" value="${nullableNumber(line.precio)}"></td>
                <td><input type="number" step="0.0001" data-field="descuento" value="${nullableNumber(line.descuento)}"></td>
                <td><input type="number" step="0.0001" data-field="precio_neto" value="${nullableNumber(line.precio_neto)}"></td>
                <td><input type="text" data-field="codigo" value="${escapeHtml(line.codigo || "")}"></td>
                <td>
                    <button type="button" class="btn danger small remove-line-btn">Eliminar</button>
                    <input type="hidden" data-field="id" value="${line.id || ""}">
                    <input type="hidden" data-field="external_line_id" value="${escapeHtml(line.external_line_id || "")}">
                    <input type="hidden" data-field="cabecera_id" value="${escapeHtml(line.cabecera_id || "")}">
                    <input type="hidden" data-field="confianza_pct" value="${nullableNumber(line.confianza_pct)}">
                    <input type="hidden" data-field="confidence_pct_calc" value="${nullableNumber(line.confidence_pct_calc)}">
                    <input type="hidden" data-field="line_match_score" value="${nullableNumber(line.line_match_score)}">
                    <input type="hidden" data-field="comparison_status_json" value='${escapeAttribute(line.comparison_status_json || "")}'>
                    <input type="hidden" data-field="field_scores_json" value='${escapeAttribute(line.field_scores_json || "")}'>
                </td>
            `;
            tbody.appendChild(tr);
        });
        bindRowButtons();
    };

    const bindRowButtons = () => {
        document.querySelectorAll(".remove-line-btn").forEach((button) => {
            button.onclick = () => {
                button.closest("tr")?.remove();
                resequence();
            };
        });
    };

    const resequence = () => {
        document.querySelectorAll("#lines-table tbody tr").forEach((row, index) => {
            row.dataset.rowIndex = String(index + 1);
            const numberCell = row.querySelector(".line-number");
            if (numberCell) {
                numberCell.textContent = String(index + 1);
            }
        });
    };

    const addEmptyRow = () => {
        const newLine = {
            id: null,
            external_line_id: null,
            cabecera_id: null,
            codigo: "",
            cantidad: null,
            concepto: "",
            precio: null,
            descuento: null,
            precio_neto: null,
            codigo_imputacion: "",
            confianza_pct: null,
            confidence_pct_calc: null,
            line_match_score: null,
            comparison_status_json: null,
            field_scores_json: null,
        };
        const currentLines = collectLines();
        currentLines.push(newLine);
        renderRows(currentLines);
    };

    const collectLines = () => {
        return Array.from(document.querySelectorAll("#lines-table tbody tr")).map((row, index) => {
            const getValue = (field) => row.querySelector(`[data-field="${field}"]`)?.value ?? "";
            return {
                id: toNullableInt(getValue("id")),
                line_index: index + 1,
                external_line_id: toNullableString(getValue("external_line_id")),
                cabecera_id: toNullableString(getValue("cabecera_id")),
                codigo: toNullableString(getValue("codigo")),
                cantidad: toNullableFloat(getValue("cantidad")),
                concepto: toNullableString(getValue("concepto")),
                precio: toNullableFloat(getValue("precio")),
                descuento: toNullableFloat(getValue("descuento")),
                precio_neto: toNullableFloat(getValue("precio_neto")),
                codigo_imputacion: toNullableString(getValue("codigo_imputacion")),
                confianza_pct: toNullableFloat(getValue("confianza_pct")),
                confidence_pct_calc: toNullableFloat(getValue("confidence_pct_calc")),
                line_match_score: toNullableFloat(getValue("line_match_score")),
                comparison_status_json: toNullableString(getValue("comparison_status_json")),
                field_scores_json: toNullableString(getValue("field_scores_json")),
            };
        });
    };

    const buildPayload = (approved) => ({
        proveedor_nombre: toNullableString(document.getElementById("proveedor_nombre")?.value),
        proveedor_cif: toNullableString(document.getElementById("proveedor_cif")?.value),
        fecha: toNullableString(document.getElementById("fecha")?.value),
        numero_albaran: toNullableString(document.getElementById("numero_albaran")?.value),
        forma_pago: toNullableString(document.getElementById("forma_pago")?.value),
        obra_codigo: toNullableString(document.getElementById("obra_codigo")?.value),
        obra_nombre: toNullableString(document.getElementById("obra_nombre")?.value),
        obra_direccion: toNullableString(document.getElementById("obra_direccion")?.value),
        review_notes: toNullableString(document.getElementById("review_notes")?.value),
        approved,
        approved_by: toNullableString(document.getElementById("approved_by")?.value),
        lines: collectLines(),
    });

    const submitDocument = async (approved) => {
        try {
            const response = await fetch(`/api/documents/${documentId}`, {
                method: "PUT",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(buildPayload(approved)),
            });

            const payload = await response.json();
            if (!response.ok) {
                throw new Error(payload.detail || payload.message || "No se pudo guardar el documento");
            }
            if (payload.redirect_url) {
                window.location.href = payload.redirect_url;
                return;
            }
            window.location.reload();
        } catch (error) {
            window.alert(error.message || String(error));
        }
    };

    addLineBtn?.addEventListener("click", addEmptyRow);
    saveBtn?.addEventListener("click", () => submitDocument(false));
    approveBtn?.addEventListener("click", () => submitDocument(true));

    renderRows(documentData.lines || []);
})();

function resolveDocumentId(dataTag) {
    const fromData = dataTag?.dataset?.documentId;
    if (fromData && String(fromData).trim() !== "") {
        return String(fromData).trim();
    }

    if (window.reviewDocumentId && String(window.reviewDocumentId).trim() !== "") {
        return String(window.reviewDocumentId).trim();
    }

    const match = window.location.pathname.match(/^\/documents\/([^/?#]+)/i);
    if (match && match[1]) {
        return decodeURIComponent(match[1]);
    }

    return null;
}

function nullableNumber(value) {
    return value === null || value === undefined ? "" : String(value);
}

function toNullableString(value) {
    if (value === null || value === undefined) {
        return null;
    }
    const normalized = String(value).trim();
    return normalized === "" ? null : normalized;
}

function toNullableFloat(value) {
    if (value === null || value === undefined || String(value).trim() === "") {
        return null;
    }
    const parsed = Number(String(value).replace(",", "."));
    return Number.isFinite(parsed) ? parsed : null;
}

function toNullableInt(value) {
    if (value === null || value === undefined || String(value).trim() === "") {
        return null;
    }
    const parsed = Number.parseInt(String(value), 10);
    return Number.isFinite(parsed) ? parsed : null;
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
    return escapeHtml(value).replaceAll("`", "&#96;");
}

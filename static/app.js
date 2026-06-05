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
    const undoBtn = document.getElementById("undo-btn");
    const saveBtn = document.getElementById("save-btn");
    const approveBtn = document.getElementById("approve-btn");
    const valuateBtn = document.getElementById("valuate-btn");
    const valuateStatus = document.getElementById("valuate-status");
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
                if (typeof pushUndoAction === "function") pushUndoAction();
            }
        }
    });

    // --------------------------------------------------------------- //
    // Conciliacion editable.
    //
    // El badge Sigrid/Nueva de la fila de conciliacion es un boton que
    // revela un <select> con las lineas del contrato (+ opcion "Nueva").
    // Al elegir una opcion se hace PATCH al endpoint de override y se
    // recarga la pagina para reflejar precio/importe/badge actualizados.
    // (La correccion vive hasta el proximo "Valorar ahora").
    // --------------------------------------------------------------- //
    linesBody.addEventListener("click", function (evt) {
        const btn = evt.target.closest
            ? evt.target.closest(".js-concilia-edit")
            : null;
        if (!btn) return;
        const row = btn.closest("tr.conciliacion-row");
        if (!row) return;
        const sel = row.querySelector(".js-concilia-select");
        if (!sel) return;
        sel.hidden = !sel.hidden;
        if (!sel.hidden) sel.focus();
    });

    linesBody.addEventListener("change", async function (evt) {
        const sel = evt.target.closest
            ? evt.target.closest(".js-concilia-select")
            : null;
        if (!sel) return;
        const vid = sel.dataset.vid;
        const value = sel.value;
        if (!vid || !value) return;
        const body = (value === "nueva")
            ? { mode: "nueva" }
            : { mode: "contract_line", matched_contrato_line_id: Number(value) };
        sel.disabled = true;
        try {
            const resp = await fetch(
                `/api/documents/${documentId}/lines/${vid}/conciliacion`,
                {
                    method: "PATCH",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(body),
                }
            );
            if (!resp.ok) {
                let msg = "No se pudo aplicar la conciliacion.";
                try {
                    const j = await resp.json();
                    if (j && j.detail) msg = j.detail;
                } catch (e) { /* sin cuerpo JSON */ }
                window.alert(msg);
                sel.disabled = false;
                return;
            }
            window.location.reload();
        } catch (e) {
            window.alert("Error de red aplicando la conciliacion.");
            sel.disabled = false;
        }
    });

    if (addLineBtn) {
        addLineBtn.addEventListener("click", function () {
            const row = buildEmptyRow(linesBody.querySelectorAll("tr").length);
            row.dataset.added = "1";
            linesBody.appendChild(row);
            reindexRows();
            pushUndoAction();
        });
    }

    // --------------------------------------------------------------- //
    // Edicion de lineas (#4/#5/#6): unidad y unitario editables,
    // recalculo importe<->unitario, dirty-tracking y deshacer.
    //
    //   - cantidad o unitario cambian -> importe = cantidad * unitario.
    //   - importe cambia            -> unitario = importe / cantidad.
    //   - Toda edicion marca la fila "dirty". Solo las filas dirty con
    //     valuation_line_id envian valuation_line_update al guardar (asi
    //     no se corrompen conversiones de lineas no tocadas).
    //   - "Deshacer" restaura los valores tal cual se cargo la pagina y
    //     elimina las filas nuevas no guardadas.
    // --------------------------------------------------------------- //
    function _rowField(row, field) {
        return row.querySelector('[data-field="' + field + '"]');
    }
    function _num(el) {
        if (!el) return null;
        const v = (el.value || "").trim();
        if (!v) return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }
    function _round2(n) { return Math.round(n * 100) / 100; }
    function _markDirty(row) {
        if (!row || (row.classList && row.classList.contains("conciliacion-row"))) return;
        row.dataset.dirty = "1";
        if (undoBtn) undoBtn.hidden = false;
    }

    // Recalculo + dirty al teclear (no es una "accion" de undo todavia;
    // la accion se registra al CONFIRMAR el campo, evento change).
    linesBody.addEventListener("input", function (evt) {
        const el = evt.target;
        if (!el || !el.dataset || !el.dataset.field) return;
        const row = el.closest("tr");
        if (!row || (row.classList && row.classList.contains("conciliacion-row"))) return;
        _markDirty(row);

        const field = el.dataset.field;
        const cantEl = _rowField(row, "cantidad");
        const puEl = _rowField(row, "precio_unitario");
        const impEl = _rowField(row, "importe");

        if (field === "cantidad" || field === "precio_unitario") {
            const c = _num(cantEl), pu = _num(puEl);
            if (impEl && c !== null && pu !== null) impEl.value = String(_round2(c * pu));
        } else if (field === "importe") {
            const c = _num(cantEl), imp = _num(impEl);
            if (puEl && c !== null && c !== 0 && imp !== null) puEl.value = String(_round2(imp / c));
        }
    });

    // --------------------------------------------------------------- //
    // PILA DE DESHACER (#6): deshace la ULTIMA accion, con suelo en lo
    // ultimo guardado. Cada guardado navega a redirect_url (recarga), asi
    // que la pila arranca vacia en cada carga = ultimo guardado. Una
    // "accion" es: confirmar la edicion de un campo (change), añadir linea
    // o eliminar linea. Capturamos el estado serializando el <tbody>; las
    // escuchas estan DELEGADAS en linesBody, asi que reemplazar innerHTML
    // no las pierde.
    // --------------------------------------------------------------- //
    function _syncDomForSnapshot() {
        linesBody.querySelectorAll("input").forEach(function (el) {
            if (el.type === "checkbox" || el.type === "radio") {
                if (el.checked) el.setAttribute("checked", "checked");
                else el.removeAttribute("checked");
            } else {
                el.setAttribute("value", el.value);
            }
        });
        linesBody.querySelectorAll("textarea").forEach(function (el) {
            el.textContent = el.value;
        });
        linesBody.querySelectorAll("select").forEach(function (sel) {
            Array.prototype.forEach.call(sel.options, function (opt) {
                if (opt.selected) opt.setAttribute("selected", "selected");
                else opt.removeAttribute("selected");
            });
        });
    }
    function _captureState() {
        _syncDomForSnapshot();
        return linesBody.innerHTML;
    }
    let _undoStack = [];
    let _lastState = _captureState();   // baseline = ultimo guardado
    function _refreshUndoBtn() {
        if (undoBtn) undoBtn.hidden = (_undoStack.length === 0);
    }
    // Registra una accion: empuja el estado PREVIO y fija el actual.
    function pushUndoAction() {
        _undoStack.push(_lastState);
        _lastState = _captureState();
        _refreshUndoBtn();
    }
    function _doUndo() {
        if (_undoStack.length === 0) return;
        const prev = _undoStack.pop();
        linesBody.innerHTML = prev;     // listeners delegados -> sobreviven
        _lastState = prev;
        reindexRows();
        _refreshUndoBtn();
    }

    // Confirmar la edicion de un campo (blur/Enter) = una accion.
    linesBody.addEventListener("change", function (evt) {
        const el = evt.target;
        if (!el || !el.dataset || !el.dataset.field) return;
        const row = el.closest("tr");
        if (!row || (row.classList && row.classList.contains("conciliacion-row"))) return;
        pushUndoAction();
    });

    if (undoBtn) {
        undoBtn.addEventListener("click", _doUndo);
    }

    // --------------------------------------------------------------- //
    // Desplegables de cabecera: al elegir un proveedor o una obra de la
    // lista (proveedores/obras con contrato en el proyecto) rellenamos los
    // inputs existentes (que son los que se guardan por id). "Escribir
    // manualmente" (value vacio) no toca nada.
    // --------------------------------------------------------------- //
    function _setVal(id, value) {
        const el = document.getElementById(id);
        if (el) el.value = value;
    }
    function _labelFor(kind, val, nombre) {
        if (kind === "obra") {
            return (val || "s/codigo") + (nombre ? " — " + nombre : "");
        }
        return (nombre || "Sin nombre") + (val ? " — " + val : "");
    }
    function _selectedNombre(select) {
        const opt = (select.selectedIndex >= 0) ? select.options[select.selectedIndex] : null;
        return opt ? (opt.getAttribute("data-nombre") || "") : "";
    }
    // Reconstruye opciones: [actual (preseleccionada)] + items + manual + [error].
    function _rebuildOptions(select, items, opts) {
        opts = opts || {};
        const kind = select.dataset.kind;
        const current = (opts.current != null) ? opts.current : select.value;
        const currentNombre = opts.currentNombre || "";
        const hasCurrent = !!(current && current.length);
        select.innerHTML = "";
        const frag = document.createDocumentFragment();
        if (hasCurrent) {
            const o = document.createElement("option");
            o.value = current;
            o.setAttribute("data-nombre", currentNombre);
            o.textContent = _labelFor(kind, current, currentNombre) + " (actual)";
            o.selected = true;
            frag.appendChild(o);
        }
        (items || []).forEach(function (it) {
            const val = (kind === "obra") ? (it.codigo || "") : (it.cif || "");
            const nombre = it.nombre || "";
            if (hasCurrent && val === current) return;
            const o = document.createElement("option");
            o.value = val;
            o.setAttribute("data-nombre", nombre);
            o.textContent = _labelFor(kind, val, nombre);
            frag.appendChild(o);
        });
        const man = document.createElement("option");
        man.value = "";
        man.textContent = "— escribir manualmente —";
        frag.appendChild(man);
        if (opts.error) {
            const e = document.createElement("option");
            e.value = ""; e.disabled = true;
            e.textContent = "⚠ " + opts.error;
            frag.appendChild(e);
        } else if (opts.empty) {
            const e = document.createElement("option");
            e.value = ""; e.disabled = true;
            e.textContent = "(sin resultados en Sigrid)";
            frag.appendChild(e);
        }
        select.appendChild(frag);
        select.value = hasCurrent ? current : "";
    }
    function _showLoading(select) {
        const current = select.value;
        const currentNombre = _selectedNombre(select);
        select.innerHTML = "";
        if (current) {
            const o = document.createElement("option");
            o.value = current; o.setAttribute("data-nombre", currentNombre);
            o.textContent = _labelFor(select.dataset.kind, current, currentNombre) + " (actual)";
            o.selected = true; select.appendChild(o);
        }
        const l = document.createElement("option");
        l.value = current || ""; l.disabled = true;
        l.textContent = "Cargando de Sigrid…";
        select.appendChild(l);
        select.value = current || "";
        return { current: current, currentNombre: currentNombre };
    }
    // Carga BAJO DEMANDA al enfocar el desplegable. Para proveedores
    // depende de la obra actual: si cambia, vuelve a consultar.
    async function _loadSigridOptions(select) {
        const endpoint = select.dataset.endpoint;
        if (!endpoint) return;
        let url = endpoint;
        let obraVal = "";
        if (select.dataset.obraInput) {
            const obraEl = document.getElementById(select.dataset.obraInput);
            obraVal = obraEl ? (obraEl.value || "").trim() : "";
            url = endpoint + "?obra=" + encodeURIComponent(obraVal);
        }
        const loadKey = obraVal || "_";
        if (select.dataset.loaded === "loading") return;
        if (select.dataset.loaded === "1" && select.dataset.loadedKey === loadKey) return;

        select.dataset.loaded = "loading";
        const snap = _showLoading(select);
        try {
            const resp = await fetch(url, { headers: { "Accept": "application/json" } });
            const data = await resp.json();
            if (!data || !data.ok) {
                _rebuildOptions(select, [], {
                    current: snap.current, currentNombre: snap.currentNombre,
                    error: (data && data.error) || "Sigrid no disponible",
                });
                select.dataset.loaded = "";   // permitir reintento
                return;
            }
            const items = data.items || [];
            _rebuildOptions(select, items, {
                current: snap.current, currentNombre: snap.currentNombre,
                empty: items.length === 0,
            });
            select.dataset.loaded = "1";
            select.dataset.loadedKey = loadKey;
        } catch (e) {
            _rebuildOptions(select, [], {
                current: snap.current, currentNombre: snap.currentNombre,
                error: "Error de red consultando Sigrid",
            });
            select.dataset.loaded = "";
        }
    }

    const proveedorSelect = document.getElementById("proveedor_select");
    if (proveedorSelect) {
        proveedorSelect.addEventListener("focus", function () { _loadSigridOptions(this); });
        proveedorSelect.addEventListener("change", function () {
            if (!this.value) return;   // "escribir manualmente" no toca nada
            _setVal("proveedor_cif", this.value);
            _setVal("proveedor_nombre", _selectedNombre(this));
        });
    }
    const obraSelect = document.getElementById("obra_select");
    if (obraSelect) {
        obraSelect.addEventListener("focus", function () { _loadSigridOptions(this); });
        obraSelect.addEventListener("change", function () {
            if (!this.value) return;
            _setVal("obra_codigo", this.value);
            _setVal("obra_nombre", _selectedNombre(this));
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
            // Las filas de conciliacion (NUEVA/SIGRID) NO son lineas
            // editables: no tienen data-line-kind ni inputs data-field.
            // No deben recolectarse; antes se enviaban como lineas vacias
            // y el backend las insertaba como blancos en cada guardado (#2).
            if (row.classList && row.classList.contains("conciliacion-row")) {
                return;
            }

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
            const _idRaw = row.dataset.lineId ? Number(row.dataset.lineId) : null;
            const _codimp = readTextOrNull(byField.codigo_imputacion);
            const _concepto = readTextOrNull(byField.concepto);
            const _cantidad = readNumericOrNull(byField.cantidad);
            const _precio = readNumericOrNull(byField.precio);
            const _descuento = readNumericOrNull(byField.descuento);
            const _importe = readNumericOrNull(byField.importe);
            const _codigo = readTextOrNull(byField.codigo);

            // Saltar filas VACIAS (#2): una linea recien añadida y no
            // rellenada, o una que quedo en blanco, no debe enviarse. Si no
            // tiene id, simplemente no se inserta. Si tiene id pero esta
            // totalmente vacia, al no enviarla el backend la elimina, lo que
            // limpia los blancos acumulados de guardados anteriores.
            const _isEmpty = (
                !_codimp && !_concepto && _cantidad === null &&
                _precio === null && _descuento === null &&
                _importe === null && !_codigo
            );
            if (_isEmpty) {
                return;
            }

            merge_index += 1;
            merge_lines.push({
                id: _idRaw,
                line_index: merge_index,
                codigo_imputacion: _codimp,
                concepto: _concepto,
                cantidad: _cantidad,
                precio: _precio,
                descuento: _descuento,
                // 'importe' en V3 == 'precio_neto' en el schema del backend.
                precio_neto: _importe,
                codigo: _codigo,
                // 'unidad_display' y 'precio_unitario_display' NO se envían.
            });

            // Si una linea BASE se edito (dirty) y tiene valoracion, ademas
            // del merge enviamos un valuation_line_update para que la
            // valoracion (albaran_line_valuations) refleje cantidad/unidad/
            // unitario/importe. Solo si dirty -> no toca lineas intactas.
            const _vlid = row.dataset.valuationLineId;
            if (row.dataset.dirty === "1" && _vlid) {
                valuation_updates.push({
                    valuation_line_id: Number(_vlid),
                    codigo_partida_final: _codimp,
                    descripcion_linea: null,   // preservar (COALESCE en backend)
                    cantidad_albaran: _cantidad,
                    unidad_contrato: readTextOrNull(byField.unidad),
                    precio_unitario_final: readNumericOrNull(byField.precio_unitario),
                    importe_calculado: _importe,
                });
            }
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
    // Botón "Valorar ahora" — dispara la valoración (sv7 → sv6 → sv5)
    // sin necesidad de aprobar ni reabrir el documento.
    //
    // El backend solo permite valorar si hay selected_contrato_codigo,
    // pero por defensa redundante deshabilitamos el botón visualmente
    // (atributo disabled en el HTML cuando no hay contrato).
    //
    // Tras el click:
    //   - Deshabilita el botón durante la petición.
    //   - POST /api/documents/{id}/valuate.
    //   - Muestra un breve mensaje informativo y deja el botón
    //     habilitado de nuevo (el revisor puede reintentar si quiere).
    // --------------------------------------------------------------- //
    function paintValuateStatus(kind, text) {
        if (!valuateStatus) return;
        valuateStatus.hidden = false;
        valuateStatus.className = "valuate-status valuate-" + kind;
        valuateStatus.textContent = text;
    }

    async function triggerValuate() {
        if (!valuateBtn) return;

        // El backend valora contra el contrato GUARDADO en BBDD. Para que
        // el revisor no tenga que pulsar "Guardar" aparte tras seleccionar
        // el contrato, este botón hace dos pasos:
        //   1) Guarda la selección actual (PUT, SIN redirigir).
        //   2) Dispara la valoración (POST /valuate → sv7 → sv6 → sv5).
        const codigo = collectSelectedContratoCodigo();
        if (!codigo) {
            paintValuateStatus(
                "error",
                "Selecciona un contrato antes de valorar."
            );
            return;
        }

        valuateBtn.disabled = true;
        paintValuateStatus(
            "loading",
            "Guardando selección y lanzando valoración…"
        );
        try {
            // Paso 1: persistir la selección (y los campos editados) sin
            // navegar fuera de la página.
            const saveResp = await fetch(`/api/documents/${documentId}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(collectPayload(false)),
            });
            if (!saveResp.ok) {
                let detail = saveResp.statusText;
                try {
                    const b = await saveResp.json();
                    detail = b.detail || detail;
                } catch (_) {}
                paintValuateStatus(
                    "error",
                    "Error al guardar la selección: " + detail
                );
                return;
            }

            // Paso 2: disparar la valoración.
            const response = await fetch(
                `/api/documents/${documentId}/valuate`,
                { method: "POST", headers: { "Content-Type": "application/json" } }
            );
            let body = null;
            try { body = await response.json(); } catch (_) {}
            if (!response.ok) {
                const detail = (body && body.detail) || response.statusText;
                paintValuateStatus("error", "Error: " + detail);
                return;
            }
            const msg = (body && body.message) ||
                "Valoración encolada. Refresca en unos segundos.";
            paintValuateStatus("ok", msg);
        } catch (exc) {
            paintValuateStatus("error", "Error de red: " + (exc && exc.message || exc));
        } finally {
            // Volvemos a habilitar el botón. El revisor puede pulsar
            // otra vez si quiere reintentar (cada click genera un
            // selected_at_utc distinto → sv7 lo trata como una nueva
            // revaluación legítima).
            valuateBtn.disabled = false;
        }
    }

    if (valuateBtn) {
        valuateBtn.addEventListener("click", function (event) {
            event.preventDefault();
            triggerValuate();
        });
    }

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

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

    // El antiguo wireContratoSelector (que manejaba el <select> del caso
    // "varios contratos") queda sustituido por wireContratoCombo(), montado
    // al final del bloque de edición: un combo único con autocompletar
    // (contratos detectados + «Sin contrato») que guarda y re-valora al
    // cambiar. Se inicializa allí porque necesita documentId, collectPayload
    // y triggerValuate, definidos dentro de ese bloque.

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
        const combo = row.querySelector(".js-concilia-combo");
        if (!combo) return;
        const inp = combo.querySelector(".combo-input");
        if (inp) { inp.focus(); }
    });

    // Boton "Borrar" de la fila salmon: elimina DEFINITIVAMENTE la
    // conciliacion (la salmon desaparece y reaparece el "+"). La linea
    // deja de contar en el total y no se llevara a Sigrid. DELETE + recarga.
    linesBody.addEventListener("click", async function (evt) {
        const btn = evt.target.closest
            ? evt.target.closest(".js-concilia-remove")
            : null;
        if (!btn) return;
        const row = btn.closest("tr.conciliacion-row");
        if (!row) return;
        const vid = row.dataset.forValuationLineId;
        if (!vid) return;
        if (!window.confirm(
            "¿Borrar esta línea? Dejará de contar en el total y no se " +
            "llevará a Sigrid. Reaparecerá el botón + para volver a traerla."
        )) return;
        btn.disabled = true;
        try {
            const resp = await fetch(
                `/api/documents/${documentId}/lines/${vid}/conciliacion`,
                { method: "DELETE" }
            );
            if (!resp.ok) {
                let msg = "No se pudo borrar la línea.";
                try {
                    const j = await resp.json();
                    if (j && j.detail) msg = j.detail;
                } catch (e) { /* sin cuerpo JSON */ }
                window.alert(msg);
                btn.disabled = false;
                return;
            }
            window.location.reload();
        } catch (e) {
            window.alert("Error de red borrando la línea.");
            btn.disabled = false;
        }
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

    // --------------------------------------------------------------- //
    // "Traer linea de contrato" a una linea NO casada (boton + en la
    // celda del numero). Inyecta una fila salmon con un desplegable de
    // lineas de contrato (construido del JSON embebido). Al elegir, hace
    // POST al endpoint by-merge que CREA la valoracion de esa linea con la
    // linea de contrato elegida (precio del contrato). Recarga al terminar.
    // --------------------------------------------------------------- //
    let CONTRATO_LINES = [];
    try {
        const _clEl = document.getElementById("contrato-lines-json");
        if (_clEl) CONTRATO_LINES = JSON.parse(_clEl.textContent || "[]");
    } catch (e) { CONTRATO_LINES = []; }

    // ----------------------------------------------------------------- //
    // Combo type-ahead LOCAL para elegir linea de contrato escribiendo
    // (filtra CONTRATO_LINES por parecido de string, sin red). Reutiliza
    // el CSS .combo/.combo-panel/.combo-item de los combos proveedor/obra.
    // No concilia el mismo: al elegir, fija el <select> oculto que le
    // pasamos y dispara su "change", de modo que los handlers PATCH/POST
    // existentes siguen siendo los unicos que tocan el backend.
    // ----------------------------------------------------------------- //
    function _normLine(s) {
        return (s == null ? "" : String(s))
            .toLowerCase()
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "");
    }

    function _scrollActive(panel) {
        const el = panel.querySelector(".combo-item.active");
        if (el && el.scrollIntoView) { el.scrollIntoView({ block: "nearest" }); }
    }

    function makeLineCombo(opts) {
        // Combo de UNA pieza: casilla que al enfocar/clicar abre un panel
        // con TODAS las lineas de contrato; al escribir deja solo las que
        // contienen el texto (sin acentos, por palabras); al elegir, fija
        // el <select> oculto y dispara su "change" (PATCH/POST intactos).
        // El panel va FIXED (posicionado por JS) porque la tabla de lineas
        // tiene scroll horizontal y un panel absoluto se recortaria.
        const wrap = document.createElement("div");
        wrap.className = "combo combo-lines";
        const input = document.createElement("input");
        input.type = "text";
        input.className = "combo-input";
        input.setAttribute("autocomplete", "off");
        input.placeholder = opts.placeholder ||
            "Escribe o despliega para elegir linea…";
        const panel = document.createElement("div");
        panel.className = "combo-panel";
        panel.hidden = true;
        wrap.appendChild(input);
        wrap.appendChild(panel);

        let shown = [];
        let activeIdx = -1;
        let repos = null;

        function place() {
            const r = input.getBoundingClientRect();
            panel.style.top = (r.bottom + 4) + "px";
            panel.style.left = r.left + "px";
            panel.style.minWidth = Math.max(r.width, 360) + "px";
            panel.style.maxWidth = "720px";
        }

        function close() {
            panel.hidden = true;
            activeIdx = -1;
            if (repos) {
                window.removeEventListener("scroll", repos, true);
                window.removeEventListener("resize", repos);
                repos = null;
            }
        }

        function pick(item) {
            input.value = item.label;
            close();
            opts.onPick(item.value);
        }

        function render() {
            panel.innerHTML = "";
            if (!shown.length) {
                const d = document.createElement("div");
                d.className = "combo-msg";
                d.textContent = "Sin coincidencias";
                panel.appendChild(d);
            } else {
                shown.forEach(function (it, i) {
                    const d = document.createElement("div");
                    d.className =
                        "combo-item" + (i === activeIdx ? " active" : "");
                    d.textContent = it.label;
                    d.addEventListener("mousedown", function (e) {
                        e.preventDefault();
                        pick(it);
                    });
                    panel.appendChild(d);
                });
            }
            panel.hidden = false;
            place();
            if (!repos) {
                repos = function () { close(); };
                window.addEventListener("scroll", repos, true);
                window.addEventListener("resize", repos);
            }
        }

        function open() {
            const tokens = _normLine(input.value).split(/\s+/).filter(Boolean);
            const matches = (opts.lines || []).filter(function (l) {
                if (!tokens.length) { return true; }
                const nl = _normLine(l.label);
                return tokens.every(function (t) { return nl.indexOf(t) !== -1; });
            }).map(function (l) {
                return { value: String(l.id), label: l.label };
            });
            shown = [];
            if (opts.nuevaValue) {
                shown.push({
                    value: opts.nuevaValue,
                    label: opts.nuevaLabel || "➕ Nueva",
                });
            }
            shown = shown.concat(matches);
            activeIdx = -1;
            render();
        }

        input.addEventListener("focus", open);
        input.addEventListener("click", open);
        input.addEventListener("input", open);
        input.addEventListener("keydown", function (e) {
            if (panel.hidden) {
                if (e.key === "ArrowDown" || e.key === "Enter") { open(); }
                return;
            }
            if (e.key === "ArrowDown") {
                e.preventDefault();
                activeIdx = Math.min(activeIdx + 1, shown.length - 1);
                render(); _scrollActive(panel);
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                activeIdx = Math.max(activeIdx - 1, 0);
                render(); _scrollActive(panel);
            } else if (e.key === "Enter") {
                e.preventDefault();
                if (activeIdx >= 0 && shown[activeIdx]) { pick(shown[activeIdx]); }
            } else if (e.key === "Escape") {
                close();
            }
        });
        document.addEventListener("click", function (e) {
            if (!wrap.contains(e.target)) { close(); }
        });
        return wrap;
    }

    // Monta el combo de busqueda en TODAS las filas salmon ya conciliadas
    // (sustituye el <select> de cambiar conciliacion, que queda oculto como
    // contenedor de valor). Asi cada linea salmon tiene su casilla.
    function initConciliaCombos() {
        const sels = document.querySelectorAll(".js-concilia-select");
        Array.prototype.forEach.call(sels, function (sel) {
            if (sel.dataset.comboReady) { return; }
            sel.dataset.comboReady = "1";
            sel.hidden = true;
            const combo = makeLineCombo({
                lines: CONTRATO_LINES,
                nuevaValue: "nueva",
                nuevaLabel: "➕ Nueva (derivar a la partida)",
                onPick: function (value) {
                    sel.value = value;
                    sel.dispatchEvent(new Event("change", { bubbles: true }));
                },
            });
            combo.classList.add("js-concilia-combo");
            sel.insertAdjacentElement("beforebegin", combo);
        });
    }
    initConciliaCombos();

    linesBody.addEventListener("click", function (evt) {
        const btn = evt.target.closest ? evt.target.closest(".js-add-concilia") : null;
        if (!btn) return;
        const tr = btn.closest("tr");
        if (!tr) return;
        const mergeId = tr.dataset.lineId;
        if (!mergeId) return;
        // Toggle: si ya hay una fila "add" justo debajo, la quitamos.
        const next = tr.nextElementSibling;
        if (next && next.classList.contains("concilia-add")) { next.remove(); return; }

        const row = document.createElement("tr");
        row.className = "conciliacion-row concilia-add";
        const td1 = document.createElement("td");
        td1.className = "concilia-td";
        const badge = document.createElement("span");
        badge.className = "concilia-badge concilia-badge-assigned";
        badge.textContent = "Traer";
        td1.appendChild(badge);
        const td2 = document.createElement("td");
        td2.className = "concilia-td";
        td2.colSpan = 9;
        const sel = document.createElement("select");
        sel.className = "concilia-select js-add-concilia-select";
        sel.dataset.mergeId = mergeId;
        const opt0 = document.createElement("option");
        opt0.value = ""; opt0.disabled = true; opt0.selected = true;
        opt0.textContent = "— elegir línea de contrato —";
        sel.appendChild(opt0);
        // Opcion "Nueva": copia la linea blanca del albaran (salmon Nueva,
        // como si la hubiera generado el valorador sin match de contrato).
        const optNueva = document.createElement("option");
        optNueva.value = "__nueva__";
        optNueva.textContent = "➕ Nueva (copiar línea del albarán)";
        sel.appendChild(optNueva);
        if (!CONTRATO_LINES.length) {
            const o = document.createElement("option");
            o.value = ""; o.disabled = true;
            o.textContent = "(sin lineas de contrato; selecciona un contrato primero)";
            sel.appendChild(o);
        }
        CONTRATO_LINES.forEach(function (cl) {
            const o = document.createElement("option");
            o.value = String(cl.id);
            o.textContent = cl.label;
            sel.appendChild(o);
        });
        sel.hidden = true;  // contenedor de valor
        const addCombo = makeLineCombo({
            lines: CONTRATO_LINES,
            nuevaValue: "__nueva__",
            nuevaLabel: "➕ Nueva (copiar línea del albarán)",
            onPick: function (value) {
                sel.value = value;
                sel.dispatchEvent(new Event("change", { bubbles: true }));
            },
        });
        td2.appendChild(addCombo);
        td2.appendChild(sel);
        row.appendChild(td1);
        row.appendChild(td2);
        tr.insertAdjacentElement("afterend", row);
        addCombo.querySelector(".combo-input").focus();
    });

    linesBody.addEventListener("change", async function (evt) {
        const sel = evt.target.closest ? evt.target.closest(".js-add-concilia-select") : null;
        if (!sel) return;
        const mergeId = sel.dataset.mergeId;
        const value = sel.value;
        if (!mergeId || !value) return;
        const body = (value === "__nueva__")
            ? { mode: "nueva" }
            : { mode: "contract_line", matched_contrato_line_id: Number(value) };
        sel.disabled = true;
        try {
            const resp = await fetch(
                `/api/documents/${documentId}/lines/by-merge/${mergeId}/conciliacion`,
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(body),
                }
            );
            if (!resp.ok) {
                let msg = "No se pudo traer la linea de contrato.";
                try { const j = await resp.json(); if (j && j.detail) msg = j.detail; } catch (e) {}
                window.alert(msg); sel.disabled = false; return;
            }
            window.location.reload();
        } catch (e) {
            window.alert("Error de red trayendo la linea de contrato.");
            sel.disabled = false;
        }
    });

    // --------------------------------------------------------------- //
    // Acciones MASIVAS sobre las conciliaciones (jun 2026).
    //
    // - "Borrar casadas": borra TODAS las líneas casadas (salmon Sigrid y
    //   Nueva) reutilizando el DELETE individual, en serie para no
    //   machacar el backend. Al acabar recarga.
    // - "Copiar sin casar → Nueva": para cada línea del albarán SIN
    //   conciliación (las que muestran el botón +), crea su salmon
    //   «Nueva» copiando la línea blanca (mismo POST {mode:"nueva"} que
    //   el + → Nueva individual).
    // Ambas piden confirmación con el nº de líneas afectadas e informan
    // del progreso en el propio botón.
    // --------------------------------------------------------------- //
    const bulkDeleteBtn = document.getElementById("bulk-delete-btn");
    const bulkCopyBtn = document.getElementById("bulk-copy-btn");

    function collectVidsCasadas() {
        // Filas salmon reales (excluye la fila temporal del "+", que es
        // .concilia-add y no lleva valuation_line_id).
        const out = [];
        linesBody.querySelectorAll(
            "tr.conciliacion-row[data-for-valuation-line-id]"
        ).forEach(function (row) {
            if (row.classList.contains("concilia-add")) return;
            const vid = (row.dataset.forValuationLineId || "").trim();
            if (vid) out.push(vid);
        });
        return out;
    }

    function collectMergeIdsSinCasar() {
        // Las líneas SIN conciliación son las que tienen el botón "+".
        const out = [];
        linesBody.querySelectorAll(".js-add-concilia").forEach(function (btn) {
            const tr = btn.closest("tr");
            const mergeId = tr ? (tr.dataset.lineId || "").trim() : "";
            if (mergeId) out.push(mergeId);
        });
        return out;
    }

    async function runBulk(btn, items, labelProgress, worker) {
        btn.disabled = true;
        const originalText = btn.textContent;
        let failures = 0;
        for (let i = 0; i < items.length; i++) {
            btn.textContent = labelProgress + " " + (i + 1) + "/" + items.length + "…";
            try {
                const ok = await worker(items[i]);
                if (!ok) failures++;
            } catch (e) {
                failures++;
            }
        }
        if (failures) {
            window.alert(
                "Terminado con " + failures + " error(es) de " + items.length +
                ". Se recargará para reflejar lo aplicado."
            );
        }
        window.location.reload();
        // Por si la recarga tarda: restaurar estado visual.
        btn.textContent = originalText;
    }

    if (bulkDeleteBtn) {
        bulkDeleteBtn.addEventListener("click", async function () {
            const vids = collectVidsCasadas();
            if (!vids.length) {
                window.alert("No hay líneas casadas que borrar.");
                return;
            }
            if (!window.confirm(
                "¿Borrar las " + vids.length + " línea(s) casada(s) (Sigrid y " +
                "Nueva)? Dejarán de contar en el total y reaparecerá el botón " +
                "+ en cada línea."
            )) return;
            await runBulk(bulkDeleteBtn, vids, "Borrando", async function (vid) {
                const resp = await fetch(
                    `/api/documents/${documentId}/lines/${vid}/conciliacion`,
                    { method: "DELETE" }
                );
                return resp.ok;
            });
        });
    }

    if (bulkCopyBtn) {
        bulkCopyBtn.addEventListener("click", async function () {
            const mergeIds = collectMergeIdsSinCasar();
            if (!mergeIds.length) {
                window.alert("No hay líneas sin casar: todas tienen ya su línea de contrato.");
                return;
            }
            if (!window.confirm(
                "¿Crear una línea «Nueva» para las " + mergeIds.length +
                " línea(s) sin casar, copiando la línea blanca leída por la IA?"
            )) return;
            await runBulk(bulkCopyBtn, mergeIds, "Copiando", async function (mergeId) {
                const resp = await fetch(
                    `/api/documents/${documentId}/lines/by-merge/${mergeId}/conciliacion`,
                    {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ mode: "nueva" }),
                    }
                );
                return resp.ok;
            });
        });
    }

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
    // --------------------------------------------------------------- //
    // Comboboxes de cabecera (elegir proveedor / obra) CON BUSQUEDA.
    // Sustituyen a los <select>: un input donde escribes y filtra por
    // SUBCADENA (sin distinguir mayusculas ni acentos) sobre la lista
    // que se trae de Sigrid bajo demanda (al enfocar). Al elegir, rellena
    // los inputs reales (proveedor_cif/nombre, obra_codigo/nombre) que son
    // los que se guardan. La caja de busqueda no se guarda.
    // --------------------------------------------------------------- //
    function _norm(s) {
        return (s || "").toString().toLowerCase()
            .normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    }
    function _comboLabel(kind, val, nombre) {
        if (kind === "obra") {
            return (val || "s/codigo") + (nombre ? " — " + nombre : "");
        }
        return (nombre || "Sin nombre") + (val ? " — " + val : "");
    }
    // Puntuacion de coincidencia texto extraido (IA) vs nombre de Sigrid.
    // 1.0 si uno contiene al otro; si no, fraccion de tokens (>=3 letras)
    // del texto que aparecen en el candidato. Ambos normalizados.
    function _matchScore(qNorm, candNorm) {
        if (!qNorm || !candNorm) return 0;
        if (candNorm.indexOf(qNorm) !== -1 || qNorm.indexOf(candNorm) !== -1) {
            return 1;
        }
        const toks = qNorm.split(/\s+/).filter(function (t) {
            return t.length >= 3;
        });
        if (!toks.length) return 0;
        let hits = 0;
        toks.forEach(function (t) { if (candNorm.indexOf(t) !== -1) hits++; });
        return hits / toks.length;
    }

    function initCombo(combo) {
        const input = combo.querySelector(".combo-input");
        const panel = combo.querySelector(".combo-panel");
        if (!input || !panel) return;
        const kind = combo.dataset.kind;
        const endpoint = combo.dataset.endpoint;
        const valInputId = combo.dataset.valInput;
        const nombreInputId = combo.dataset.nombreInput;
        const obraInputId = combo.dataset.obraInput || "";

        let items = [];        // [{val, nombre, label, norm}]
        let filtered = [];
        let loadedKey = null;  // clave (obra) para la que se cargo
        let loading = false;
        let activeIdx = -1;

        function currentObra() {
            if (!obraInputId) return "";
            const el = document.getElementById(obraInputId);
            return el ? (el.value || "").trim() : "";
        }
        function close() { panel.hidden = true; activeIdx = -1; }
        function msg(text, cls) {
            panel.innerHTML = "";
            const d = document.createElement("div");
            d.className = "combo-msg" + (cls ? " " + cls : "");
            d.textContent = text;
            panel.appendChild(d);
            panel.hidden = false;
        }
        function render(list) {
            filtered = list;
            if (loading) { msg("Cargando de Sigrid…"); return; }
            if (!list.length) { msg("Sin coincidencias"); return; }
            panel.innerHTML = "";
            list.forEach(function (it, i) {
                const d = document.createElement("div");
                d.className = "combo-item" + (i === activeIdx ? " active" : "");
                d.textContent = it.label;
                // mousedown (no click) para que dispare antes del blur del input
                d.addEventListener("mousedown", function (e) {
                    e.preventDefault();
                    choose(it);
                });
                panel.appendChild(d);
            });
            panel.hidden = false;
        }
        function applyFilter() {
            activeIdx = -1;
            const q = _norm(input.value);
            if (!q) { render(items); return; }
            render(items.filter(function (it) { return it.norm.indexOf(q) !== -1; }));
        }
        function choose(it) {
            const vEl = document.getElementById(valInputId);
            const nEl = document.getElementById(nombreInputId);
            if (vEl) vEl.value = it.val || "";
            if (nEl) nEl.value = it.nombre || "";
            input.value = it.label;
            combo.dataset.label = it.label;
            close();
        }
        async function ensureLoaded(silent) {
            const key = obraInputId ? (currentObra() || "_") : "_";
            if (loading) return;
            if (loadedKey === key && items.length) return;
            loading = true; loadedKey = key;
            if (!silent) msg("Cargando de Sigrid…");
            let url = endpoint;
            if (obraInputId) url += "?obra=" + encodeURIComponent(currentObra());
            try {
                const resp = await fetch(url, { headers: { "Accept": "application/json" } });
                const data = await resp.json();
                loading = false;
                if (!data || !data.ok) {
                    items = []; loadedKey = null;
                    if (!silent) msg("⚠ " + ((data && data.error) || "Sigrid no disponible"), "combo-error");
                    return;
                }
                items = (data.items || []).map(function (it) {
                    const val = (kind === "obra") ? (it.codigo || "") : (it.cif || "");
                    const nombre = it.nombre || "";
                    const label = _comboLabel(kind, val, nombre);
                    return { val: val, nombre: nombre, label: label, norm: _norm(label) };
                });
                if (!silent) applyFilter();
            } catch (e) {
                loading = false; items = []; loadedKey = null;
                if (!silent) msg("⚠ Error de red consultando Sigrid", "combo-error");
            }
        }

        input.addEventListener("focus", function () {
            input.select();
            ensureLoaded();
        });
        input.addEventListener("input", function () {
            if (combo.dataset.suggested) {
                combo.classList.remove("combo-suggested");
                delete combo.dataset.suggested;
                input.removeAttribute("title");
            }
            if (items.length || loading) applyFilter();
            else ensureLoaded();
        });
        input.addEventListener("keydown", function (e) {
            if (e.key === "ArrowDown") {
                if (panel.hidden) { applyFilter(); return; }
                activeIdx = Math.min(activeIdx + 1, filtered.length - 1);
                render(filtered); e.preventDefault();
            } else if (e.key === "ArrowUp") {
                activeIdx = Math.max(activeIdx - 1, 0);
                render(filtered); e.preventDefault();
            } else if (e.key === "Enter") {
                if (activeIdx >= 0 && filtered[activeIdx]) {
                    choose(filtered[activeIdx]); e.preventDefault();
                }
            } else if (e.key === "Escape") {
                close();
            }
        });
        input.addEventListener("blur", function () {
            // retardo para permitir el mousedown de un item
            setTimeout(function () {
                close();
                if (combo.dataset.label != null) input.value = combo.dataset.label;
            }, 150);
        });

        // Propuesta por coincidencia de texto: cuando la IA no localizo
        // bien el codigo de obra / CIF, usamos el nombre extraido para
        // proponer la mejor coincidencia de Sigrid (misma lista que el
        // desplegable), dejandola pre-seleccionada y marcada "sugerido".
        combo._proposeBest = async function (text) {
            const vEl2 = document.getElementById(valInputId);
            if (vEl2 && (vEl2.value || "").trim()) return false;  // no pisar
            const q = _norm(text || "");
            if (!q) return false;
            await ensureLoaded(true);   // carga silenciosa (sin abrir panel)
            if (!items.length) return false;
            let best = null, bestScore = 0;
            items.forEach(function (it) {
                const s = _matchScore(q, _norm(it.nombre || ""));
                if (s > bestScore) { bestScore = s; best = it; }
            });
            if (best && bestScore >= 0.5) {
                choose(best);
                combo.classList.add("combo-suggested");
                combo.dataset.suggested = "1";
                input.title = "Sugerido por coincidencia de texto — verificalo";
                return true;
            }
            return false;
        };
    }

    // Excluimos los combos de lineas de contrato (.combo-lines): son
    // LOCALES (filtran CONTRATO_LINES en memoria) y NO deben pasar por
    // initCombo, que hace fetch a Sigrid y mostraba "Sigrid no disponible".
    document.querySelectorAll(".combo:not(.combo-lines)").forEach(initCombo);

    // Al cargar: si la IA no fijo el codigo de obra / CIF del proveedor,
    // proponer por coincidencia de texto. El proveedor depende de la obra
    // (sus contratos), asi que se resuelve primero la obra.
    (async function proposeHeaderMatches() {
        const obraCombo = document.querySelector('.combo[data-kind="obra"]');
        const provCombo = document.querySelector('.combo[data-kind="proveedor"]');
        const obraCodigo = document.getElementById("obra_codigo");
        const obraNombre = document.getElementById("obra_nombre");
        const provCif = document.getElementById("proveedor_cif");
        const provNombre = document.getElementById("proveedor_nombre");

        if (obraCombo && obraCombo._proposeBest &&
            obraCodigo && !(obraCodigo.value || "").trim() &&
            obraNombre && (obraNombre.value || "").trim()) {
            try { await obraCombo._proposeBest(obraNombre.value); } catch (_) {}
        }
        if (provCombo && provCombo._proposeBest &&
            obraCodigo && (obraCodigo.value || "").trim() &&
            provCif && !(provCif.value || "").trim() &&
            provNombre && (provNombre.value || "").trim()) {
            try { await provCombo._proposeBest(provNombre.value); } catch (_) {}
        }
    })();

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
            // La blanca guarda lo leido/escrito por el usuario. El precio
            // editable de la fila (precio_unitario) ES el precio declarado
            // del albaran y se guarda en el merge.
            const _precio = readNumericOrNull(byField.precio_unitario);
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
            // NOTA: las ediciones de la linea blanca van SOLO al merge. Ya
            // NO se manda valuation_line_update para from_albaran: la
            // valoracion (fila salmon) la fija la conciliacion con la linea
            // de contrato, y la blanca nunca debe pisar/ser pisada por ella.
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

    // La valoracion es ASINCRONA (sv7 -> sv6 -> sv5 escriben en
    // albaran_valuations). Tras lanzarla sondeamos el documento hasta
    // que aparezca una valoracion NUEVA (distinto valuation_id o
    // created_at_utc que la que habia al cargar la pagina) y entonces
    // refrescamos. Si tarda demasiado, ofrecemos refresco manual.
    async function pollUntilValued(baselineId, baselineTs) {
        const maxTries = 40;       // ~2 min a 3s
        const intervalMs = 3000;
        for (let i = 0; i < maxTries; i++) {
            await new Promise(function (r) { setTimeout(r, intervalMs); });
            try {
                const resp = await fetch(
                    `/api/documents/${documentId}`,
                    { headers: { "Accept": "application/json" } }
                );
                if (!resp.ok) continue;
                const b = await resp.json();
                const v = b && b.valuation;
                if (v && (String(v.valuation_id || "") !== baselineId
                        || String(v.created_at_utc || "") !== baselineTs)) {
                    paintValuateStatus(
                        "ok", "Valoracion completada. Actualizando…"
                    );
                    window.location.reload();
                    return;
                }
            } catch (_) { /* reintenta en el siguiente ciclo */ }
        }
        paintValuateStatus(
            "ok", "La valoracion esta tardando mas de lo normal."
        );
        if (valuateStatus) {
            const a = document.createElement("a");
            a.href = "#";
            a.textContent = " Refrescar ahora";
            a.addEventListener("click", function (e) {
                e.preventDefault(); window.location.reload();
            });
            valuateStatus.appendChild(a);
        }
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
            // Lanzada OK: ahora esperamos (sondeo) a que la valoracion
            // asincrona termine y refrescamos sola la pagina.
            const baselineId = (valuateBtn.dataset.currentValId || "");
            const baselineTs = (valuateBtn.dataset.currentValTs || "");
            paintValuateStatus(
                "loading",
                "Valorando… (esto puede tardar unos segundos)"
            );
            await pollUntilValued(baselineId, baselineTs);
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
    // Combo de contrato (jun 2026)
    //
    // Un único combo con autocompletar (mismo makeLineCombo que las filas
    // de conciliación) que lista los contratos detectados por CIF+obra MÁS
    // la opción «Sin contrato». Sustituye al <select> del caso "varios" y
    // al estado fijo del caso "1 contrato". Al elegir:
    //   - contrato real → escribe el código en el hidden y reutiliza
    //     triggerValuate() (guarda con PUT y RE-VALORA con sondeo).
    //   - «Sin contrato» → vacía el hidden y solo GUARDA (PUT); no se valora
    //     porque no hay nada contra qué hacerlo (el backend ya persiste NULL
    //     y el botón "Valorar ahora" queda deshabilitado).
    // Cuando el proceso no detectó contrato, el combo arranca en «Sin
    // contrato» (hidden vacío) y solo ofrece esa opción hasta que se
    // re-busque corrigiendo CIF/obra.
    // --------------------------------------------------------------- //
    // --------------------------------------------------------------- //
    // Combo de contrato REMOTO (jun 2026)
    //
    // Igual que los desplegables de obra/proveedor: al desplegarlo consulta
    // Sigrid EN VIVO (GET /api/sigrid/contratos?obra=&cif=) con la obra y el
    // CIF actuales del formulario; al escribir filtra en local. Resuelve el
    // caso de cambiar de obra: ya no depende de lo cacheado en Postgres.
    //
    // Al elegir un contrato real, como valorar exige que las LÍNEAS del
    // contrato estén cacheadas (sv5 las lee de Postgres), el flujo es:
    //   1) fija el código en el hidden + guarda (PUT con la obra/cif),
    //   2) re-fetch en sv3 (consulta Sigrid y CACHEA contratos + líneas),
    //   3) triggerValuate (re-guarda selección + valora + recarga).
    // «Sin contrato» solo guarda (no valora).
    // --------------------------------------------------------------- //
    function initContratoComboRemoto() {
        const mount = document.getElementById("contrato-combo-mount");
        const hidden = document.getElementById("selected_contrato_codigo");
        if (!mount || !hidden) return;

        const headerBtn = findHeaderContratoBtn();
        const cards = document.querySelectorAll(".js-contrato-card");
        const statusEl = document.getElementById("contrato-combo-status");
        const SIN = "__sin_contrato__";
        const SIN_ITEM = { codigo: SIN, label: "\ud83d\udeab Sin contrato", norm: _norm("sin contrato") };

        // Etiquetas de los contratos YA cacheados (solo para el estado inicial).
        const labelByCodigo = {};
        const dataTag = document.getElementById("contratos-combo-data");
        if (dataTag) {
            try {
                (JSON.parse(dataTag.textContent) || []).forEach(function (c) {
                    labelByCodigo[c.codigo] = c.label;
                });
            } catch (_) {}
        }

        function obraActual() {
            const el = document.getElementById("obra_codigo");
            return el ? (el.value || "").trim() : "";
        }
        function cifActual() {
            const el = document.getElementById("proveedor_cif");
            return el ? (el.value || "").trim() : "";
        }
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
        function showCard(codigo) {
            cards.forEach(function (card) {
                card.style.display = card.dataset.codigo === codigo ? "" : "none";
            });
        }
        function status(kind, text) {
            if (!statusEl) return;
            statusEl.hidden = false;
            statusEl.className = "valuate-status valuate-" + kind;
            statusEl.textContent = text;
        }

        // ---- DOM del combo (mismas clases .combo que obra/proveedor) ----
        const combo = document.createElement("div");
        combo.className = "combo combo-lines js-contrato-combo";
        const input = document.createElement("input");
        input.type = "text";
        input.className = "combo-input";
        input.setAttribute("autocomplete", "off");
        input.placeholder = "Escribe para buscar el contrato en Sigrid\u2026";
        const panel = document.createElement("div");
        panel.className = "combo-panel";
        panel.hidden = true;
        combo.appendChild(input);
        combo.appendChild(panel);
        mount.appendChild(combo);

        let items = [];        // [{codigo,label,norm}] (contratos de Sigrid)
        let filtered = [];
        let activeIdx = -1;
        let loadedKey = null;
        let loading = false;

        function place() {
            const r = input.getBoundingClientRect();
            panel.style.position = "fixed";
            panel.style.top = (r.bottom + 4) + "px";
            panel.style.left = r.left + "px";
            panel.style.minWidth = Math.max(r.width, 360) + "px";
            panel.style.maxWidth = "720px";
        }
        function close() { panel.hidden = true; activeIdx = -1; }
        function msg(text, cls) {
            panel.innerHTML = "";
            const d = document.createElement("div");
            d.className = "combo-msg" + (cls ? " " + cls : "");
            d.textContent = text;
            panel.appendChild(d);
            panel.hidden = false;
            place();
        }
        function render(list) {
            filtered = list;
            if (!list.length) { msg("Sin coincidencias"); return; }
            panel.innerHTML = "";
            list.forEach(function (it, i) {
                const d = document.createElement("div");
                d.className = "combo-item" + (i === activeIdx ? " active" : "");
                d.textContent = it.label;
                d.addEventListener("mousedown", function (e) {
                    e.preventDefault();
                    onPick(it.codigo);
                });
                panel.appendChild(d);
            });
            panel.hidden = false;
            place();
        }
        function applyFilter() {
            activeIdx = -1;
            const q = _norm(input.value);
            const matches = !q ? items.slice()
                : items.filter(function (it) { return it.norm.indexOf(q) !== -1; });
            // «Sin contrato» siempre disponible, arriba del todo.
            render([SIN_ITEM].concat(matches));
        }
        async function ensureLoaded() {
            const key = obraActual() + "|" + cifActual();
            if (loading) return;
            if (loadedKey === key && items.length) { applyFilter(); return; }
            const obra = obraActual();
            if (!obra) {
                items = []; loadedKey = key;
                msg("Indica primero la obra para buscar sus contratos. Puedes elegir \u00abSin contrato\u00bb.");
                return;
            }
            loading = true; loadedKey = key;
            msg("Cargando de Sigrid\u2026");
            let url = "/api/sigrid/contratos?obra=" + encodeURIComponent(obra);
            const cif = cifActual();
            if (cif) url += "&cif=" + encodeURIComponent(cif);
            try {
                const resp = await fetch(url, { headers: { "Accept": "application/json" } });
                const data = await resp.json();
                loading = false;
                if (!data || !data.ok) {
                    items = []; loadedKey = null;
                    msg("\u26a0 " + ((data && data.error) || "Sigrid no disponible") + " \u2014 puedes elegir \u00abSin contrato\u00bb.", "combo-error");
                    return;
                }
                items = (data.items || []).map(function (it) {
                    const codigo = it.codigo || "";
                    const prov = it.nombre_proveedor || "";
                    const label = codigo
                        + (it.nombre ? " \u2014 " + it.nombre : "")
                        + (prov ? " \u00b7 " + prov : "");
                    return { codigo: codigo, label: label, norm: _norm(label) };
                });
                applyFilter();
            } catch (e) {
                loading = false; items = []; loadedKey = null;
                msg("\u26a0 Error de red consultando Sigrid \u2014 puedes elegir \u00abSin contrato\u00bb.", "combo-error");
            }
        }

        input.addEventListener("focus", function () { input.select(); ensureLoaded(); });
        input.addEventListener("click", function () { ensureLoaded(); });
        input.addEventListener("input", function () {
            if (items.length || loading) applyFilter(); else ensureLoaded();
        });
        input.addEventListener("keydown", function (e) {
            if (e.key === "ArrowDown") {
                if (panel.hidden) { applyFilter(); return; }
                activeIdx = Math.min(activeIdx + 1, filtered.length - 1);
                render(filtered); e.preventDefault();
            } else if (e.key === "ArrowUp") {
                activeIdx = Math.max(activeIdx - 1, 0);
                render(filtered); e.preventDefault();
            } else if (e.key === "Enter") {
                if (activeIdx >= 0 && filtered[activeIdx]) {
                    onPick(filtered[activeIdx].codigo); e.preventDefault();
                }
            } else if (e.key === "Escape") {
                close();
            }
        });
        input.addEventListener("blur", function () { setTimeout(close, 150); });
        document.addEventListener("click", function (e) {
            if (!combo.contains(e.target)) close();
        });
        window.addEventListener("scroll", function () { if (!panel.hidden) close(); }, true);

        // ---- Estado inicial ----
        const current = (hidden.value || "").trim();
        showCard(current);
        syncHeaderBtn(current || null);
        if (valuateBtn) valuateBtn.disabled = !current;
        input.value = current ? (labelByCodigo[current] || current) : "";
        if (!current) input.placeholder = "Sin contrato \u2014 escribe para buscar en Sigrid\u2026";

        async function persistSelection() {
            const resp = await fetch(`/api/documents/${documentId}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(collectPayload(false)),
            });
            if (!resp.ok) {
                let detail = resp.statusText;
                try { const b = await resp.json(); detail = b.detail || detail; } catch (_) {}
                throw new Error(detail);
            }
        }
        async function cacheContratosEnSigrid() {
            // Re-fetch en sv3: consulta Sigrid por la cif+obra YA guardadas y
            // cachea contratos + líneas (necesario para poder valorar).
            try {
                await fetch(
                    `/api/documents/${documentId}/re-fetch-contratos`,
                    { method: "POST", headers: { "Content-Type": "application/json" } }
                );
            } catch (_) { /* best-effort: si falla, triggerValuate avisará */ }
        }

        async function onPick(codigo) {
            close();
            const sinContrato = (codigo === SIN || !codigo);
            const real = sinContrato ? "" : codigo;

            hidden.value = real;
            showCard(real);
            syncHeaderBtn(real || null);
            input.value = sinContrato ? "" : (labelByCodigo[real] || real);
            if (sinContrato) input.placeholder = "Sin contrato \u2014 escribe para buscar en Sigrid\u2026";

            if (sinContrato) {
                if (valuateBtn) valuateBtn.disabled = true;
                status("loading", "Guardando \u00abSin contrato\u00bb\u2026");
                try {
                    await persistSelection();
                    status("ok", "Contrato eliminado. Este albar\u00e1n queda sin contrato (no se valora).");
                } catch (exc) {
                    status("error", "Error al guardar: " + (exc && exc.message || exc));
                }
                return;
            }

            if (valuateBtn) valuateBtn.disabled = false;
            status("loading", "Contrato seleccionado. Trayendo de Sigrid y valorando\u2026");
            try {
                await persistSelection();          // 1) guarda obra/cif/selected
                await cacheContratosEnSigrid();     // 2) re-fetch: cachea líneas
                hidden.value = real;                // 3) re-asegura la selección
                await triggerValuate();             //    guarda + valora + recarga
            } catch (exc) {
                status("error", "Error: " + (exc && exc.message || exc));
            }
        }
    }
    initContratoComboRemoto();

    // Valoracion INICIAL en segundo plano: la lanza el pipeline
    // (sv7 -> sv6 -> sv5) tras asociar el contrato, NO este boton. Si al
    // abrir el documento aun no hay valoracion pero ya hay un contrato
    // seleccionado, lo mas probable es que la valoracion inicial este en
    // curso: sondeamos y avisamos/refrescamos al terminar, sin que el
    // revisor tenga que pulsar nada. (El camino del boton "Valorar ahora"
    // sigue igual: sondea tras el click.)
    (function autoPollInitialValuation() {
        if (!valuateBtn) return;  // vista no editable: no hay nada que sondear
        const yaTieneValoracion =
            (valuateBtn.dataset.currentValId || "").trim() !== "";
        if (yaTieneValoracion) return;   // ya valorado: no hace falta sondear
        let codigo = "";
        try { codigo = collectSelectedContratoCodigo() || ""; } catch (_) {}
        if (!codigo) return;             // sin contrato: no se espera valoracion
        paintValuateStatus(
            "loading",
            "Valoracion inicial en curso… la pagina se actualizara al terminar."
        );
        // baseline vacio: cualquier valoracion que aparezca dispara el refresco.
        pollUntilValued("", "");
    })();

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

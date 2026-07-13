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

    // TANDA 2A: datos completos del documento (incluye document.lines, las
    // líneas BLANCAS del albarán). Ahora las blancas son SOLO LECTURA y ya
    // NO viven en el grid editable; las reenviamos intactas en el PUT desde
    // aquí, porque update_document BORRA las líneas merge que no lleguen en
    // payload.lines. Sin esto, guardar la cabecera vaciaría el albarán.
    let DOC_DATA = {};
    try { DOC_DATA = JSON.parse(dataTag.textContent) || {}; } catch (_) { DOC_DATA = {}; }
    window.__documentData = DOC_DATA;

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
        // TANDA 2B: el combo de línea de contrato está OCULTO por defecto
        // (concepto a una sola línea). El ✎ lo abre/cierra; al abrir, foco.
        const willOpen = !combo.classList.contains("is-open");
        combo.classList.toggle("is-open", willOpen);
        if (willOpen) {
            const inp = combo.querySelector(".combo-input");
            if (inp) { inp.focus(); }
        }
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
                // El panel va FIXED, así que si la página/tabla de detrás
                // se desplaza hay que cerrarlo (se desancla del input).
                // PERO el scroll DENTRO del propio panel (rueda o arrastre
                // del slider) NO debe cerrarlo: antes lo hacía y por eso el
                // desplegable no se podía scrollear de ninguna forma.
                repos = function (e) {
                    if (e && e.type === "scroll" && e.target &&
                        panel.contains(e.target)) {
                        return;  // scroll interno: dejar scrollear
                    }
                    close();
                };
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

    document.addEventListener("click", function (evt) {
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
        td2.colSpan = 8;
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

    document.addEventListener("change", async function (evt) {
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
        // Las líneas SIN conciliación son las que tienen el botón "+"
        // (ahora en la tabla blanca de solo lectura, fuera de linesBody).
        const out = [];
        document.querySelectorAll(".js-add-concilia").forEach(function (btn) {
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

    // --------------------------------------------------------------- //
    // Edición de la fila salmon (jun 2026): la línea que se insertará
    // como albarán en Sigrid.
    //
    // - Todos los campos editables (inputs .js-cedit).
    // - El importe (cant × precio) se refresca en vivo.
    // - Al guardar: PATCH /lines/{vid}/conciliacion y recarga.
    // - Si cambian imputación, unidad o precio (data-identity) en una
    //   fila SIGRID, el backend la convierte a NUEVA conservando el
    //   resto de campos; se avisa con un confirm antes.
    // --------------------------------------------------------------- //
    function parseNumEs(value) {
        const s = String(value || "").trim();
        if (!s) return null;
        const n = parseFloat(s.replace(/\./g, function (m, off, str) {
            // "1.234,56" → quitar puntos de miles solo si hay coma
            return str.indexOf(",") !== -1 ? "" : m;
        }).replace(",", "."));
        return isNaN(n) ? null : n;
    }

    // --------------------------------------------------------------- //
    // Combo de PARTIDA sobre la fila salmon (jun 2026).
    //
    // El campo "Código imputación" (partida) de la fila salmon era un
    // input de texto libre. Ahora se le monta un desplegable con las
    // partidas que existen en el contrato de Sigrid (deduplicadas) para
    // elegir en vez de teclear. Reutiliza el CSS del combo de líneas y
    // el MISMO arreglo de scroll del panel.
    //
    // A diferencia de makeLineCombo (que crea su propio input), aquí el
    // combo se ENGANCHA al input existente .js-partida-combo para que el
    // valor siga fluyendo por el flujo js-cedit (dirty + guardar PATCH).
    // Al elegir, se escribe el código y se dispara 'input' para que
    // wireConciliaEdit detecte el cambio.
    // --------------------------------------------------------------- //
    // Partidas del CONTRATO (Postgres) embebidas: SOLO como fallback si
    // Sigrid no responde o la obra no está fijada.
    let PARTIDAS_FALLBACK = [];
    (function () {
        const el = document.getElementById("partidas-json");
        if (el) {
            try { PARTIDAS_FALLBACK = JSON.parse(el.textContent || "[]") || []; }
            catch (_) { PARTIDAS_FALLBACK = []; }
        }
    })();

    // Carga (una sola vez) las partidas HOJA del presupuesto de la obra
    // desde Sigrid (GET /api/sigrid/partidas?obra=). Devuelve un array
    // [{code,label,norm}] con label = "codigo · descripción agregada".
    // Si Sigrid falla, no hay obra, o devuelve vacío, cae a las partidas
    // del contrato embebidas.
    let _partidasItems = null;
    let _partidasPromise = null;
    function _fallbackPartidas() {
        return (PARTIDAS_FALLBACK || []).map(function (p) {
            const c = String(p);
            return { code: c, label: c, norm: _normLine(c) };
        });
    }
    function loadPartidas() {
        if (_partidasItems) { return Promise.resolve(_partidasItems); }
        if (_partidasPromise) { return _partidasPromise; }
        const obraEl = document.getElementById("obra_codigo");
        const obra = obraEl ? (obraEl.value || "").trim() : "";
        if (!obra) {
            _partidasItems = _fallbackPartidas();
            return Promise.resolve(_partidasItems);
        }
        const url = "/api/sigrid/partidas?obra=" + encodeURIComponent(obra);
        _partidasPromise = fetch(url, { headers: { "Accept": "application/json" } })
            .then(function (r) { return r.json(); })
            .then(function (data) {
                const items = ((data && data.items) || []).map(function (p) {
                    const desc = p.descripcion_agregada || p.descripcion || "";
                    const label = desc ? (p.codigo + "  ·  " + desc) : p.codigo;
                    return { code: p.codigo, label: label, norm: _normLine(label) };
                });
                _partidasItems = items.length ? items : _fallbackPartidas();
                return _partidasItems;
            })
            .catch(function () {
                _partidasItems = _fallbackPartidas();
                return _partidasItems;
            });
        return _partidasPromise;
    }

    function attachInputCombo(input) {
        if (!input || input.dataset.comboAttached) { return; }
        input.dataset.comboAttached = "1";

        const wrap = document.createElement("div");
        wrap.className = "combo combo-lines combo-partida";
        input.parentNode.insertBefore(wrap, input);
        wrap.appendChild(input);
        const panel = document.createElement("div");
        panel.className = "combo-panel";
        panel.hidden = true;
        wrap.appendChild(panel);

        let shown = [];
        let repos = null;
        let justPicked = false;

        function place() {
            const r = input.getBoundingClientRect();
            panel.style.top = (r.bottom + 4) + "px";
            panel.style.left = r.left + "px";
            panel.style.minWidth = Math.max(r.width, 200) + "px";
            panel.style.maxWidth = "480px";
        }
        function close() {
            panel.hidden = true;
            if (repos) {
                window.removeEventListener("scroll", repos, true);
                window.removeEventListener("resize", repos);
                repos = null;
            }
        }
        function pick(it) {
            justPicked = true;
            input.value = it.code;
            close();
            // Notificar a wireConciliaEdit: marca dirty, muestra Guardar.
            input.dispatchEvent(new Event("input", { bubbles: true }));
            input.focus();
        }
        function render() {
            panel.innerHTML = "";
            if (!shown.length) {
                const d = document.createElement("div");
                d.className = "combo-msg";
                d.textContent = "Sin coincidencias";
                panel.appendChild(d);
            } else {
                shown.forEach(function (it) {
                    const d = document.createElement("div");
                    d.className = "combo-item";
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
                // Mismo arreglo que el combo de líneas: el scroll DENTRO
                // del panel (rueda/slider) no debe cerrarlo; solo el
                // scroll de la página/tabla de detrás.
                repos = function (e) {
                    if (e && e.type === "scroll" && e.target &&
                        panel.contains(e.target)) { return; }
                    close();
                };
                window.addEventListener("scroll", repos, true);
                window.addEventListener("resize", repos);
            }
        }
        function filterAndRender() {
            const items = _partidasItems || [];
            const tokens = _normLine(input.value).split(/\s+/).filter(Boolean);
            shown = items.filter(function (it) {
                if (!tokens.length) { return true; }
                const nl = it.norm || _normLine(it.label);
                return tokens.every(function (t) { return nl.indexOf(t) !== -1; });
            });
            render();
        }
        function open() {
            if (justPicked) { justPicked = false; return; }
            if (_partidasItems) { filterAndRender(); return; }
            // Aún no cargadas: panel con "Cargando…" y disparamos la carga.
            panel.innerHTML = "";
            const msg = document.createElement("div");
            msg.className = "combo-msg";
            msg.textContent = "Cargando partidas de Sigrid\u2026";
            panel.appendChild(msg);
            panel.hidden = false;
            place();
            if (!repos) {
                repos = function (e) {
                    if (e && e.type === "scroll" && e.target &&
                        panel.contains(e.target)) { return; }
                    close();
                };
                window.addEventListener("scroll", repos, true);
                window.addEventListener("resize", repos);
            }
            loadPartidas().then(function () {
                if (!panel.hidden) { filterAndRender(); }
            });
        }

        input.addEventListener("focus", open);
        input.addEventListener("click", open);
        input.addEventListener("input", open);
        // Nota: NO interceptamos Enter; lo gestiona wireConciliaEdit
        // (Enter = guardar). El picking es por click en la opción.
        document.addEventListener("click", function (e) {
            if (!wrap.contains(e.target)) { close(); }
        });
    }

    function wirePartidaCombos() {
        document.querySelectorAll(
            "tr.concilia-editable .js-partida-combo"
        ).forEach(function (inp) {
            attachContratoLineCombo(inp);
        });
    }

    // ----------------------------------------------------------------- //
    // Combo de LÍNEA DE CONTRATO sobre la fila salmón (jun 2026).
    //
    // Se engancha TANTO al input de PARTIDA (.js-partida-combo) como al de
    // CONCEPTO (.js-concepto-combo). Busca en las LÍNEAS DE CONTRATO de
    // Sigrid (embebidas en CONTRATO_LINES) filtrando por código o por
    // descripción. Al ELEGIR una línea, en lugar de escribir solo un campo,
    // RELLENA los cuatro: partida, concepto (descripción), unidad y precio
    // unitario, y recalcula el importe. (La cantidad NO se toca: es la
    // recibida del albarán.) Dispara 'input' en cada campo para que
    // wireConciliaEdit marque dirty y sincronice importe. No reconcilia con
    // Sigrid (eso sigue siendo el ✎ de Origen): al guardar, si cambió la
    // identidad de una fila Sigrid, pasará a NUEVA conservando el resto.
    // ----------------------------------------------------------------- //
    function _contratoLineItems() {
        return (CONTRATO_LINES || []).map(function (l) {
            const lab = l.label || l.desc || "";
            return {
                id: (l.id != null ? l.id : null),
                label: lab,
                desc: (l.desc != null ? l.desc : ""),
                part: (l.part != null ? l.part : ""),
                unidad: (l.unidad != null ? l.unidad : ""),
                precio: (l.precio != null ? l.precio : null),
                norm: _normLine(lab),
            };
        }).filter(function (it) { return it.label !== ""; });
    }

    function _setRowField(tr, field, val) {
        const el = tr.querySelector('[data-field="' + field + '"]');
        if (!el) { return; }
        el.value = (val === null || val === undefined) ? "" : String(val);
        // Notifica a wireConciliaEdit (dirty + sincroniza importe). El combo
        // de cada input ignora este 'input' si el campo no tiene el foco.
        el.dispatchEvent(new Event("input", { bubbles: true }));
    }

    function attachContratoLineCombo(input) {
        if (!input || input.dataset.contratoComboAttached) { return; }
        input.dataset.contratoComboAttached = "1";
        const items = _contratoLineItems();

        const wrap = document.createElement("div");
        wrap.className = "combo combo-lines combo-concepto";
        input.parentNode.insertBefore(wrap, input);
        wrap.appendChild(input);
        const panel = document.createElement("div");
        panel.className = "combo-panel";
        panel.hidden = true;
        wrap.appendChild(panel);

        let shown = [];
        let repos = null;
        let justPicked = false;

        function place() {
            const r = input.getBoundingClientRect();
            panel.style.top = (r.bottom + 4) + "px";
            panel.style.left = r.left + "px";
            panel.style.minWidth = Math.max(r.width, 280) + "px";
            panel.style.maxWidth = "560px";
        }
        function close() {
            panel.hidden = true;
            if (repos) {
                window.removeEventListener("scroll", repos, true);
                window.removeEventListener("resize", repos);
                repos = null;
            }
        }
        function pick(it) {
            justPicked = true;
            const tr = input.closest("tr.conciliacion-row");
            // Auto-guardado: re-conciliar con la línea de contrato elegida
            // reutilizando el <select> oculto js-concilia-select (PATCH
            // mode=contract_line: rellena partida/concepto/unidad/precio
            // desde Sigrid, conserva la cantidad, deja la fila SIGRID y
            // recarga). Sin pulsar «Guardar».
            const sel = tr ? tr.querySelector(".js-concilia-select") : null;
            if (sel && it.id != null) {
                sel.value = String(it.id);
                if (sel.value === String(it.id)) {
                    close();
                    sel.dispatchEvent(new Event("change", { bubbles: true }));
                    return;
                }
            }
            // Fallback (sin <select>/opción): rellenar campos en cliente.
            if (tr) {
                _setRowField(tr, "codigo_partida", it.part);
                _setRowField(tr, "descripcion", it.desc);
                _setRowField(tr, "unidad", it.unidad);
                _setRowField(tr, "precio_unitario",
                    it.precio != null ? it.precio : "");
            } else {
                input.value = it.desc;
                input.dispatchEvent(new Event("input", { bubbles: true }));
            }
            close();
            input.focus();
        }
        function render() {
            panel.innerHTML = "";
            if (!shown.length) {
                const d = document.createElement("div");
                d.className = "combo-msg";
                d.textContent = items.length ? "Sin coincidencias" : "Sin líneas de contrato";
                panel.appendChild(d);
            } else {
                shown.forEach(function (it) {
                    const d = document.createElement("div");
                    d.className = "combo-item";
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
                repos = function (e) {
                    if (e && e.type === "scroll" && e.target &&
                        panel.contains(e.target)) { return; }
                    close();
                };
                window.addEventListener("scroll", repos, true);
                window.addEventListener("resize", repos);
            }
        }
        function open() {
            // Solo se abre si el input tiene el foco: así, cuando se
            // rellenan programáticamente otros campos con combo (p.ej. al
            // elegir desde concepto se rellena también partida), su combo
            // NO se despliega.
            if (document.activeElement !== input) { return; }
            if (justPicked) { justPicked = false; return; }
            const tokens = _normLine(input.value).split(/\s+/).filter(Boolean);
            shown = items.filter(function (it) {
                if (!tokens.length) { return true; }
                return tokens.every(function (t) { return it.norm.indexOf(t) !== -1; });
            });
            render();
        }

        input.addEventListener("focus", open);
        input.addEventListener("click", open);
        input.addEventListener("input", open);
        document.addEventListener("click", function (e) {
            if (!wrap.contains(e.target)) { close(); }
        });
    }

    function wireConceptoCombos() {
        document.querySelectorAll(
            "tr.concilia-editable .js-concepto-combo"
        ).forEach(function (inp) {
            attachContratoLineCombo(inp);
        });
    }


    function wireConciliaEdit() {
        linesBody.querySelectorAll("tr.concilia-editable").forEach(function (tr) {
            const inputs = tr.querySelectorAll(".js-cedit");
            const saveBtn = tr.querySelector(".js-cedit-save");
            if (!inputs.length || !saveBtn) return;

            // Sincroniza importe <-> precio unitario, igual que la fila
            // blanca del PDF, pero metiendo el descuento (%) en la ecuación:
            //   importe = cantidad × precio × (1 − dto/100)
            //   precio  = importe / (cantidad × (1 − dto/100))
            // 'campo' es el data-field que el usuario acaba de tocar.
            function syncImporteUnitario(campo) {
                const cantEl = tr.querySelector('[data-field="cantidad"]');
                const puEl = tr.querySelector('[data-field="precio_unitario"]');
                const dtoEl = tr.querySelector('[data-field="descuento"]');
                const impEl = tr.querySelector('[data-field="importe"]');
                const cant = cantEl ? parseNumEs(cantEl.value) : null;
                const dto = dtoEl ? parseNumEs(dtoEl.value) : null;
                const factor = 1 - (dto || 0) / 100;
                if (campo === "importe") {
                    // Cambió el importe -> ajustamos el precio unitario.
                    if (!puEl || !impEl) return;
                    const imp = parseNumEs(impEl.value);
                    if (cant !== null && cant !== 0 && factor !== 0 && imp !== null) {
                        puEl.value = String(Math.round((imp / (cant * factor)) * 10000) / 10000);
                    }
                } else {
                    // Cambió cantidad / precio / descuento -> recomputamos importe.
                    if (!impEl) return;
                    const pu = puEl ? parseNumEs(puEl.value) : null;
                    if (cant !== null && pu !== null) {
                        impEl.value = String(Math.round(cant * pu * factor * 100) / 100);
                    } else {
                        impEl.value = "";
                    }
                }
            }

            inputs.forEach(function (inp) {
                inp.addEventListener("input", function () {
                    inp.classList.toggle("cedit-dirty", inp.value !== inp.defaultValue);
                    // El botón "Guardar" de la fila está SIEMPRE visible; aquí
                    // sincronizamos importe<->unitario y el feedback visual.
                    syncImporteUnitario(inp.dataset.field);
                });
                // Enter guarda directamente.
                inp.addEventListener("keydown", function (e) {
                    if (e.key === "Enter") { e.preventDefault(); saveBtn.click(); }
                });
            });

            saveBtn.addEventListener("click", async function () {
                const vid = (tr.dataset.forValuationLineId || "").trim();
                if (!vid) return;

                // Aviso de conversión SIGRID → NUEVA si cambió un campo
                // identitario en una fila assigned.
                const isSigrid = tr.classList.contains("concilia-assigned");
                const identityChanged = [].some.call(
                    tr.querySelectorAll('.js-cedit[data-identity="1"]'),
                    function (i) { return i.value !== i.defaultValue; }
                );
                if (isSigrid && identityChanged) {
                    if (!window.confirm(
                        "Has cambiado imputación, unidad o precio: la línea " +
                        "dejará de ser de Sigrid y pasará a NUEVA (conservando " +
                        "el resto de campos). ¿Continuar?"
                    )) return;
                }

                const body = {};
                inputs.forEach(function (inp) {
                    const field = inp.dataset.field;
                    if (!field) return;
                    if (inp.dataset.numeric === "1") {
                        body[field] = parseNumEs(inp.value);
                    } else {
                        const v = inp.value.trim();
                        body[field] = v || null;
                    }
                });

                saveBtn.disabled = true;
                const original = saveBtn.textContent;
                saveBtn.textContent = "Guardando…";
                try {
                    const resp = await fetch(
                        `/api/documents/${documentId}/lines/${vid}/conciliacion/campos`,
                        {
                            method: "PATCH",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify(body),
                        }
                    );
                    if (!resp.ok) {
                        let detail = resp.statusText;
                        try {
                            const b = await resp.json();
                            if (Array.isArray(b.detail)) {
                                detail = b.detail.map(function (e) {
                                    const loc = e.loc ? e.loc.join(".") : "";
                                    return (loc ? loc + ": " : "") + (e.msg || "");
                                }).join(" | ");
                            } else if (b.detail) {
                                detail = b.detail;
                            }
                        } catch (_) {}
                        throw new Error(detail);
                    }
                    window.location.reload();
                } catch (exc) {
                    window.alert("Error al guardar la línea: " + (exc && exc.message || exc));
                    saveBtn.disabled = false;
                    saveBtn.textContent = original;
                }
            });
        });
    }
    wireConciliaEdit();
    wirePartidaCombos();
    wireConceptoCombos();

    // ------------------------------------------------------------------ //
    // "Guardar todas las líneas": guarda de una vez TODAS las filas salmón
    // (concilia-editable), hayas tocado o no cada campo, con el mismo PATCH
    // que el "Guardar" de cada fila. (Re-guardar una línea sin cambios es
    // inocuo: el backend reaplica los mismos valores.) Recarga al terminar.
    // ------------------------------------------------------------------ //
    const saveAllLinesBtn = document.getElementById("save-all-lines-btn");
    if (saveAllLinesBtn) {
        function _conciliaBody(tr) {
            const body = {};
            tr.querySelectorAll(".js-cedit").forEach(function (inp) {
                const field = inp.dataset.field;
                if (!field) return;
                if (inp.dataset.numeric === "1") {
                    body[field] = parseNumEs(inp.value);
                } else {
                    const v = inp.value.trim();
                    body[field] = v || null;
                }
            });
            return body;
        }
        saveAllLinesBtn.addEventListener("click", async function () {
            const rows = [].slice.call(
                linesBody.querySelectorAll("tr.concilia-editable")
            );
            if (!rows.length) {
                window.alert("No hay líneas de albarán que guardar.");
                return;
            }
            saveAllLinesBtn.disabled = true;
            const original = saveAllLinesBtn.textContent;
            saveAllLinesBtn.textContent = "Guardando…";
            const errores = [];
            for (let i = 0; i < rows.length; i++) {
                const tr = rows[i];
                const vid = (tr.dataset.forValuationLineId || "").trim();
                if (!vid) continue;
                try {
                    const resp = await fetch(
                        `/api/documents/${documentId}/lines/${vid}/conciliacion/campos`,
                        {
                            method: "PATCH",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify(_conciliaBody(tr)),
                        }
                    );
                    if (!resp.ok) {
                        let d = resp.statusText;
                        try {
                            const b = await resp.json();
                            if (Array.isArray(b.detail)) {
                                d = b.detail.map(function (e) {
                                    const loc = e.loc ? e.loc.join(".") : "";
                                    return (loc ? loc + ": " : "") + (e.msg || "");
                                }).join(" | ");
                            } else if (b.detail) { d = b.detail; }
                        } catch (_) {}
                        errores.push("Línea " + vid + ": " + d);
                    }
                } catch (_) {
                    errores.push("Línea " + vid + ": error de red");
                }
            }
            if (errores.length) {
                window.alert(
                    "Algunas líneas no se guardaron:\n" + errores.join("\n")
                );
                saveAllLinesBtn.disabled = false;
                saveAllLinesBtn.textContent = original;
                return;
            }
            window.location.reload();
        });
    }

    if (addLineBtn) {
        addLineBtn.addEventListener("click", async function () {
            // Añade una línea de albarán suelta (no toca las blancas de la IA).
            // El backend crea una valuation line "Nueva" en blanco; al
            // recargar aparece como fila salmón editable, con su combo de
            // descripción (para copiar una línea de contrato → Sigrid) y su
            // "Borrar ✕".
            addLineBtn.disabled = true;
            try {
                const resp = await fetch(
                    `/api/documents/${documentId}/lines/standalone`,
                    { method: "POST", headers: { "Accept": "application/json" } }
                );
                if (!resp.ok) {
                    let msg = "No se pudo añadir la línea de albarán.";
                    try {
                        const j = await resp.json();
                        if (j && j.detail) msg = j.detail;
                    } catch (_) {}
                    alert(msg);
                    addLineBtn.disabled = false;
                    return;
                }
                window.location.reload();
            } catch (_) {
                alert("Error de red al añadir la línea de albarán.");
                addLineBtn.disabled = false;
            }
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
            // FIX (jun 2026): al cambiar de OBRA, la dirección que hay en
            // pantalla pertenece a la obra anterior. La vaciamos aquí; el
            // sv3 escribirá la dirección canónica de Sigrid en el
            // siguiente "Volver a buscar" / selección de contrato (el
            // refetch ahora refresca obra_nombre + obra_direccion).
            if (kind === "obra") {
                const dEl = document.getElementById("obra_direccion");
                if (dEl) dEl.value = "";
            }
            input.value = it.label;
            combo.dataset.label = it.label;
            close();
            // Auto-guardado: al elegir obra/proveedor de Sigrid, persistir
            // de inmediato (PUT) sin pulsar «Guardar». sendSave navega a la
            // URL de retorno (recarga el detalle ya guardado).
            if (typeof sendSave === "function") { sendSave(false); }
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
        // TANDA 2A: las líneas BLANCAS (merge) ya no se editan en el grid
        // (son solo lectura en su bloque colapsable). Las reenviamos TAL CUAL
        // desde el JSON embebido del documento para que el PUT NO las borre
        // (update_document elimina las merge que no lleguen en payload.lines).
        // Las líneas SALMÓN (valoración) NO van por el PUT: se guardan con su
        // PATCH propio (.../conciliacion/campos) vía «Guardar» de fila /
        // «Guardar todas». Por eso valuation_line_updates va vacío.
        const merge_lines = [];
        const src = (window.__documentData && Array.isArray(window.__documentData.lines))
            ? window.__documentData.lines
            : [];
        let idx = 0;
        src.forEach(function (ln) {
            idx += 1;
            const pick = function (k) {
                return (ln && ln[k] !== undefined && ln[k] !== null) ? ln[k] : null;
            };
            merge_lines.push({
                id: (ln && ln.id !== undefined && ln.id !== null) ? Number(ln.id) : null,
                line_index: idx,
                external_line_id: pick("external_line_id"),
                cabecera_id: pick("cabecera_id"),
                codigo: pick("codigo"),
                cantidad: pick("cantidad"),
                concepto: pick("concepto"),
                precio: pick("precio"),
                descuento: pick("descuento"),
                precio_neto: pick("precio_neto"),
                codigo_imputacion: pick("codigo_imputacion"),
                confianza_pct: pick("confianza_pct"),
                confidence_pct_calc: pick("confidence_pct_calc"),
                line_match_score: pick("line_match_score"),
                comparison_status_json: pick("comparison_status_json"),
                field_scores_json: pick("field_scores_json"),
            });
        });
        return { merge_lines: merge_lines, valuation_updates: [] };
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
        const _valuateTxt = valuateBtn.textContent;
        valuateBtn.textContent = "Valorando…";
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
            // FIX (jun 2026): el backend ahora informa si el orquestador
            // (sv7) ACEPTÓ realmente la valoración. Antes el front decía
            // "Valorando…" y sondeaba 2 minutos aunque sv7 estuviera
            // caído o hubiera respondido no_op (bug: "dice que lanza
            // valorar, pero no lo hace"). Si no se aceptó, avisamos con
            // el motivo y NO sondeamos.
            if (body && body.accepted === false) {
                paintValuateStatus(
                    "error",
                    body.message || "El orquestador no aceptó la valoración."
                );
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
            valuateBtn.textContent = _valuateTxt;
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
            // El boton SIEMPRE visible. Si el mapa cliente tiene la URL
            // directa del PDF, la usamos; si no, dejamos el href del
            // template (/documents/<id>/contrato-pdf), que resuelve el PDF
            // en el servidor. Antes se ocultaba el boton cuando el mapa no
            // tenia la URL: ese era el motivo de que no apareciera.
            const url = codigo ? contratosPdfMap[codigo] : null;
            if (url) {
                headerBtn.href = url;
            }
            if (codigo) {
                headerBtn.title = "Abrir el PDF del contrato " + codigo;
            }
            headerBtn.style.display = "";
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
            // Re-fetch: consulta Sigrid por la cif+obra YA guardadas y cachea
            // contratos + líneas. En solo-front lo hace el fallback LOCAL
            // (síncrono); en producción, sv3 vía cola. Devuelve el outcome
            // {status, count, selected_contrato_codigo, message} o null si
            // la llamada falla.
            try {
                const r = await fetch(
                    `/api/documents/${documentId}/re-fetch-contratos`,
                    { method: "POST", headers: { "Content-Type": "application/json" } }
                );
                if (!r.ok) return null;
                return await r.json();
            } catch (_) { return null; }
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
                const rf = await cacheContratosEnSigrid();  // 2) cachea contrato+líneas
                // Camino ASÍNCRONO (colas): sv3 recibió el encargo, bajará
                // el PDF/MD del contrato y ENCADENARÁ la valoración él
                // mismo. NO es un error (aunque count venga a 0: aún no ha
                // terminado). Sondeamos hasta que aparezca la valoración.
                if (rf && rf.status === "queued") {
                    status("loading",
                        "Contrato guardado \u2713 Trayendo el contrato de "
                        + "Sigrid y preparando la valoraci\u00f3n\u2026 esta "
                        + "p\u00e1gina se actualizar\u00e1 sola al terminar "
                        + "(no hace falta refrescar).");
                    if (valuateBtn) valuateBtn.disabled = true;
                    const baseId = (valuateBtn && valuateBtn.dataset.currentValId) || "";
                    const baseTs = (valuateBtn && valuateBtn.dataset.currentValTs) || "";
                    pollUntilValued(baseId, baseTs);
                    return;
                }
                // Fallo REAL del re-fetch (Sigrid caído, 0 resultados, o sin
                // cola NI fallback local): avisamos claro y NO valoramos
                // (así evitamos el 409 "sin contrato").
                // OJO: NO usar `rf.count === 0` como señal de error. Con el
                // flujo por colas la respuesta llega ANTES de que sv3
                // procese, así que count viene SIEMPRE a 0 aunque todo vaya
                // bien (el caso "queued" ya hizo return arriba). Solo son
                // fallo real los estados explícitos.
                if (rf && (rf.status === "sigrid_error"
                        || rf.status === "no_results")) {
                    status("error",
                        "No se pudo asociar el contrato: "
                        + (rf.message || "el re-fetch no encontró/cacheó el contrato.")
                        + " (revisa colas/sv3 o credenciales Sigrid).");
                    if (valuateBtn) valuateBtn.disabled = true;
                    return;
                }
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
        // Valoración en curso al cargar: el botón rojo queda DESACTIVADO
        // hasta que termine y la página se refresque sola.
        if (valuateBtn) {
            valuateBtn.disabled = true;
            valuateBtn.textContent = "Valorando…";
        }
        paintValuateStatus(
            "loading",
            "Valoracion en curso… la pagina se actualizara al terminar."
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

    // FIX (jun 2026): tras un re-fetch, el sv3 puede haber canonizado la
    // cabecera (nombre de proveedor por CIF; nombre + dirección de la
    // obra) AUNQUE no haya devuelto contratos. Cuando count==0 no se
    // recarga la página, así que traemos el documento y refrescamos los
    // inputs + las etiquetas de los combos para que el revisor vea ya
    // los datos canónicos de Sigrid.
    async function refreshHeaderFromServer() {
        try {
            const resp = await fetch(
                `/api/documents/${documentId}`,
                { headers: { "Accept": "application/json" } }
            );
            if (!resp.ok) return;
            const doc = await resp.json();
            if (!doc) return;

            function setVal(id, value) {
                const el = document.getElementById(id);
                if (el) el.value = value || "";
            }
            setVal("proveedor_nombre", doc.proveedor_nombre);
            setVal("proveedor_cif", doc.proveedor_cif);
            setVal("obra_codigo", doc.obra_codigo);
            setVal("obra_nombre", doc.obra_nombre);
            setVal("obra_direccion", doc.obra_direccion);

            const provCombo = document.querySelector(
                '.combo[data-kind="proveedor"]'
            );
            if (provCombo) {
                const label = (doc.proveedor_nombre || "")
                    + (doc.proveedor_cif ? " — " + doc.proveedor_cif : "");
                provCombo.dataset.label = label;
                const inp = provCombo.querySelector(".combo-input");
                if (inp) inp.value = label;
            }
            const obraCombo = document.querySelector(
                '.combo[data-kind="obra"]'
            );
            if (obraCombo) {
                const label = (doc.obra_codigo || "")
                    + (doc.obra_nombre ? " — " + doc.obra_nombre : "");
                obraCombo.dataset.label = label;
                const inp = obraCombo.querySelector(".combo-input");
                if (inp) inp.value = label;
            }
        } catch (_) { /* best-effort: la cabecera queda como estaba */ }
    }

    // Modo COLAS (jun 2026): el re-fetch es ASÍNCRONO. sv4 publica en
    // q-persistencia y sv3 (worker, puede arrancar en frío) re-consulta
    // Sigrid + UPSERT de contratos en segundo plano. No hay resultado
    // inmediato (status="queued"), así que sondeamos el documento hasta
    // que aparezcan contratos (o se seleccione uno) y recargamos. Con
    // timeout para no quedarnos colgados si sv3 sigue sin encontrar nada.
    async function pollUntilContratosOrReload(baselineCount, maxMs) {
        const started = Date.now();
        const intervalMs = 3000;
        async function poll() {
            const elapsed = Date.now() - started;
            const secsLeft = Math.max(0, Math.ceil((maxMs - elapsed) / 1000));
            paintStatus(
                "info",
                "Re-búsqueda encolada. Esperando a Sigrid\u2026 (" + secsLeft + " s)"
            );
            try {
                const resp = await fetch(
                    `/api/documents/${documentId}`,
                    { headers: { "Accept": "application/json" } }
                );
                if (resp.ok) {
                    const doc = await resp.json();
                    const n = (doc && doc.contratos) ? doc.contratos.length : 0;
                    if (n > baselineCount
                        || (doc && doc.selected_contrato_codigo)) {
                        paintStatus("success", "Contratos actualizados. Recargando\u2026");
                        setTimeout(function () { window.location.reload(); }, 500);
                        return;
                    }
                }
            } catch (_) { /* reintenta en el siguiente tick */ }

            if (elapsed >= maxMs) {
                // Timeout: sv3 no devolvió contratos nuevos (puede ser
                // legítimo: CIF/obra sin contrato). Refrescamos la cabecera
                // en sitio (sv3 pudo canonizar proveedor/obra) y dejamos
                // reintentar.
                paintStatus(
                    "warning",
                    "Sin contratos nuevos por ahora. Cabecera actualizada; "
                    + "recarga o reintenta en unos segundos."
                );
                refreshHeaderFromServer();
                setButtonsDisabled(false);
                return;
            }
            setTimeout(poll, intervalMs);
        }
        poll();
    }

    function handleRefetchOutcome(outcome) {
        // Re-fetch asíncrono (colas): arranca el sondeo y sale.
        if (outcome && outcome.status === "queued") {
            setButtonsDisabled(true);
            paintStatus("info",
                "B\u00fasqueda de contratos en curso\u2026 esta secci\u00f3n "
                + "se actualizar\u00e1 sola en unos segundos.");
            fetch(
                `/api/documents/${documentId}`,
                { headers: { "Accept": "application/json" } }
            )
                .then(function (r) { return r.ok ? r.json() : null; })
                .then(function (doc) {
                    const base = (doc && doc.contratos)
                        ? doc.contratos.length : 0;
                    pollUntilContratosOrReload(base, 60000);
                })
                .catch(function () { pollUntilContratosOrReload(0, 60000); });
            return;
        }

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
        // Sin contratos: no se recarga, pero la cabecera puede haber
        // sido canonizada por el sv3 — refrescarla en sitio.
        refreshHeaderFromServer();
        setButtonsDisabled(false);
    }

    if (saveAndRefetchBtn) saveAndRefetchBtn.addEventListener("click", handleSaveAndRefetch);
    if (refetchOnlyBtn) refetchOnlyBtn.addEventListener("click", handleRefetchOnly);
})();

// ===================================================================== //
//  TANDA 2B — Columnas de tabla GENÉRICAS: reordenar + ancho + orden.    //
//                                                                         //
//  Reutilizable en cualquier <table> que declare:                        //
//    data-cols-store="clave"   -> prefijo localStorage (por usuario)      //
//    data-resizable="1"        -> permite redimensionar (requiere         //
//                                 <colgroup> con <col data-col data-w>)   //
//    data-sortable="1"         -> orden por columna en cliente (▲▼)       //
//  y, en cada <th>/<td>/<col>, data-col="clave". Columnas con            //
//  data-col-fixed no se mueven (p.ej. la columna sticky de acciones).     //
//  El ORDEN, ANCHO y ORDENACIÓN se guardan por usuario en localStorage    //
//  (claves distintas por tabla), de modo que el grid de líneas y la       //
//  tabla de la bandeja recuerdan su configuración por separado.          //
// ===================================================================== //
(function () {
    "use strict";
    var MIN_W = 56;

    function rj(k) { try { return JSON.parse(localStorage.getItem(k) || "null"); } catch (e) { return null; } }
    function wj(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }

    function enhanceTable(table) {
        if (!table) { return null; }
        var store = table.getAttribute("data-cols-store");
        if (!store) { return null; }
        var resizable = table.getAttribute("data-resizable") === "1";
        var sortable = table.getAttribute("data-sortable") === "1";
        var colgroup = table.querySelector("colgroup");
        var theadRows = [].slice.call(table.querySelectorAll("thead tr"));
        var headRow = null;
        for (var i = 0; i < theadRows.length; i++) {
            if (theadRows[i].querySelector("th[data-col]")) { headRow = theadRows[i]; break; }
        }
        if (!headRow) { return null; }
        var tbody = table.querySelector("tbody");

        var DEFAULT_ORDER = [].map.call(headRow.querySelectorAll("th[data-col]"),
            function (th) { return th.getAttribute("data-col"); });
        var FIXED = {};
        [].forEach.call(headRow.querySelectorAll("th[data-col]"), function (th) {
            if (th.hasAttribute("data-col-fixed")) { FIXED[th.getAttribute("data-col")] = true; }
        });
        var MOVABLE = DEFAULT_ORDER.filter(function (k) { return !FIXED[k]; });

        var LS_ORDER = store + ".order.v1";
        var LS_WIDTH = store + ".width.v1";
        var LS_SORT = store + ".sort.v1";

        function movableOrder() {
            var saved = rj(LS_ORDER);
            if (!Array.isArray(saved)) { return MOVABLE.slice(); }
            var known = {}; MOVABLE.forEach(function (k) { known[k] = true; });
            var out = saved.filter(function (k) { return known[k]; });
            MOVABLE.forEach(function (k) { if (out.indexOf(k) === -1) { out.push(k); } });
            return out;
        }
        function sequence() {
            var mo = movableOrder(); var mi = 0; var seq = [];
            DEFAULT_ORDER.forEach(function (k) {
                if (FIXED[k]) { seq.push(k); } else { seq.push(mo[mi++]); }
            });
            return seq;
        }
        function reorderParent(parent, seq) {
            if (!parent) { return; }
            var byCol = {};
            [].forEach.call(parent.children, function (n) {
                var k = n.getAttribute && n.getAttribute("data-col");
                if (k) { byCol[k] = n; }
            });
            seq.forEach(function (k) { if (byCol[k]) { parent.appendChild(byCol[k]); } });
        }
        function applyOrder() {
            var seq = sequence();
            reorderParent(colgroup, seq);
            theadRows.forEach(function (tr) { reorderParent(tr, seq); });
            if (tbody) {
                [].forEach.call(tbody.querySelectorAll("tr"), function (tr) {
                    if (tr.classList.contains("concilia-add") ||
                        tr.classList.contains("salmon-empty")) { return; }
                    if (tr.querySelector("td[data-col]")) { reorderParent(tr, seq); }
                });
            }
        }

        // ---------------- Anchos (solo resizable + colgroup) ------------- //
        function defW(k) {
            var col = colgroup && colgroup.querySelector('col[data-col="' + k + '"]');
            var d = col && Number(col.getAttribute("data-w"));
            return (isFinite(d) && d > 0) ? d : 120;
        }
        function widths() {
            var saved = rj(LS_WIDTH) || {}; var out = {};
            DEFAULT_ORDER.forEach(function (k) {
                var w = Number(saved[k]);
                out[k] = (isFinite(w) && w >= MIN_W) ? w : defW(k);
            });
            return out;
        }
        function applyWidths() {
            if (!colgroup) { return; }
            var W = widths(); var total = 0;
            DEFAULT_ORDER.forEach(function (k) {
                var w = W[k]; total += w;
                var col = colgroup.querySelector('col[data-col="' + k + '"]');
                if (col) { col.style.width = w + "px"; }
                var th = headRow.querySelector('th[data-col="' + k + '"]');
                if (th) { th.style.width = w + "px"; }
            });
            table.style.width = total + "px";
        }
        function syncTableWidth() {
            if (!colgroup) { return; }
            var total = 0;
            [].forEach.call(colgroup.querySelectorAll("col[data-col]"), function (col) {
                total += parseFloat(col.style.width) || 0;
            });
            if (total) { table.style.width = total + "px"; }
        }

        // ---------------- Ordenación cliente (▲▼) ------------------------ //
        function getSort() { var s = rj(LS_SORT); return (s && s.col) ? s : null; }
        function cellVal(tr, col, type) {
            var td = tr.querySelector('td[data-col="' + col + '"]');
            if (!td) { return type === "num" ? -Infinity : ""; }
            var inp = td.querySelector("input");
            var raw = inp ? inp.value : td.getAttribute("data-sort-value");
            if (raw === null || raw === undefined) { raw = td.textContent || ""; }
            if (type === "num") {
                var n = parseFloat(String(raw).replace(",", "."));
                return isFinite(n) ? n : -Infinity;
            }
            return String(raw).trim().toLowerCase();
        }
        function sortRows(col, dir) {
            if (!tbody) { return; }
            var th = headRow.querySelector('th[data-col="' + col + '"]');
            var type = (th && th.getAttribute("data-sort-type") === "num") ? "num" : "text";
            var rows = [].slice.call(tbody.querySelectorAll("tr")).filter(function (tr) {
                return tr.querySelector("td[data-col]") &&
                    !tr.classList.contains("salmon-empty") &&
                    !tr.classList.contains("concilia-add");
            });
            rows.sort(function (a, b) {
                var va = cellVal(a, col, type), vb = cellVal(b, col, type);
                if (va < vb) { return dir === "asc" ? -1 : 1; }
                if (va > vb) { return dir === "asc" ? 1 : -1; }
                return 0;
            });
            rows.forEach(function (tr) { tbody.appendChild(tr); });
        }
        function applySortIndicator() {
            [].forEach.call(headRow.querySelectorAll("th[data-col]"), function (th) {
                th.classList.remove("sorted-asc", "sorted-desc");
            });
            var s = getSort(); if (!s) { return; }
            var th = headRow.querySelector('th[data-col="' + s.col + '"]');
            if (!th) { return; }
            th.classList.add(s.dir === "asc" ? "sorted-asc" : "sorted-desc");
            sortRows(s.col, s.dir);
        }

        // ---------------- Cableado de interacciones ---------------------- //
        function wireResize() {
            var rz = null;
            headRow.addEventListener("mousedown", function (ev) {
                var h = ev.target.closest ? ev.target.closest(".col-resizer") : null;
                if (!h) { return; }
                var th = h.closest("th[data-col]"); if (!th) { return; }
                ev.preventDefault();
                var k = th.getAttribute("data-col");
                var col = colgroup.querySelector('col[data-col="' + k + '"]');
                rz = { k: k, col: col, th: th, x: ev.clientX, w: th.getBoundingClientRect().width };
                document.body.classList.add("col-resizing");
            });
            document.addEventListener("mousemove", function (ev) {
                if (!rz) { return; }
                var w = Math.max(MIN_W, Math.round(rz.w + (ev.clientX - rz.x)));
                if (rz.col) { rz.col.style.width = w + "px"; }
                rz.th.style.width = w + "px"; rz._w = w;
                syncTableWidth();
            });
            document.addEventListener("mouseup", function () {
                if (!rz) { return; }
                if (rz._w) { var W = widths(); W[rz.k] = rz._w; wj(LS_WIDTH, W); }
                rz = null; document.body.classList.remove("col-resizing");
            });
        }
        function wireReorder() {
            var dragKey = null;
            function cleanup() {
                dragKey = null;
                [].forEach.call(headRow.querySelectorAll("th"), function (t) {
                    t.classList.remove("col-dragging"); t.classList.remove("col-drop-target");
                });
            }
            headRow.addEventListener("dragstart", function (ev) {
                var th = ev.target.closest ? ev.target.closest("th[data-col]") : null;
                if (!th) { return; }
                if (FIXED[th.getAttribute("data-col")]) { ev.preventDefault(); return; }
                if (ev.target.closest && ev.target.closest(".col-resizer")) { ev.preventDefault(); return; }
                dragKey = th.getAttribute("data-col");
                th.classList.add("col-dragging");
                headRow._didDrag = false;
                try { ev.dataTransfer.effectAllowed = "move"; ev.dataTransfer.setData("text/plain", dragKey); } catch (e) {}
            });
            headRow.addEventListener("dragover", function (ev) {
                if (!dragKey) { return; }
                ev.preventDefault();
                try { ev.dataTransfer.dropEffect = "move"; } catch (e) {}
                var th = ev.target.closest ? ev.target.closest("th[data-col]") : null;
                [].forEach.call(headRow.querySelectorAll("th"), function (t) { t.classList.remove("col-drop-target"); });
                if (th && !FIXED[th.getAttribute("data-col")] &&
                    th.getAttribute("data-col") !== dragKey) { th.classList.add("col-drop-target"); }
            });
            headRow.addEventListener("drop", function (ev) {
                if (!dragKey) { return; }
                ev.preventDefault();
                var th = ev.target.closest ? ev.target.closest("th[data-col]") : null;
                if (th && !FIXED[th.getAttribute("data-col")]) {
                    var target = th.getAttribute("data-col");
                    if (target && target !== dragKey) {
                        var mo = movableOrder();
                        var from = mo.indexOf(dragKey), to = mo.indexOf(target);
                        if (from !== -1 && to !== -1) {
                            mo.splice(from, 1); mo.splice(to, 0, dragKey);
                            wj(LS_ORDER, mo); applyOrder();
                            if (resizable && colgroup) { applyWidths(); }
                        }
                    }
                }
                headRow._didDrag = true;
                setTimeout(function () { headRow._didDrag = false; }, 0);
                cleanup();
            });
            headRow.addEventListener("dragend", function () {
                headRow._didDrag = true;
                setTimeout(function () { headRow._didDrag = false; }, 0);
                cleanup();
            });
        }
        function wireSort() {
            headRow.addEventListener("click", function (ev) {
                if (ev.target.closest && ev.target.closest(".col-resizer")) { return; }
                if (headRow._didDrag) { return; }
                var th = ev.target.closest ? ev.target.closest("th[data-col]") : null;
                if (!th) { return; }
                var col = th.getAttribute("data-col");
                var s = getSort(); var dir = "asc";
                if (s && s.col === col) { dir = s.dir === "asc" ? "desc" : "asc"; }
                wj(LS_SORT, { col: col, dir: dir });
                applySortIndicator();
            });
        }

        // ---------------- Arranque --------------------------------------- //
        applyOrder();
        if (resizable && colgroup) { applyWidths(); }
        if (sortable) { applySortIndicator(); }
        wireReorder();
        if (resizable && colgroup) { wireResize(); }
        if (sortable) { wireSort(); }

        return {
            reset: function () {
                try {
                    localStorage.removeItem(LS_ORDER);
                    localStorage.removeItem(LS_WIDTH);
                    localStorage.removeItem(LS_SORT);
                } catch (e) {}
                applyOrder();
                if (resizable && colgroup) { applyWidths(); }
                if (sortable) { applySortIndicator(); }
            }
        };
    }

    // Aplicar a las tablas con configuración: grid de líneas (detalle) y
    // tabla de la bandeja (lista). Cada una recuerda su layout por separado.
    var gridApi = enhanceTable(document.getElementById("lines-table"));
    [].forEach.call(document.querySelectorAll('table[data-cols-store]'), function (t) {
        if (t.id === "lines-table") { return; }
        enhanceTable(t);
    });

    // Botón «↺ Columnas» del detalle: restablece el grid de líneas.
    var resetBtn = document.getElementById("reset-cols-btn");
    if (resetBtn && gridApi) {
        resetBtn.addEventListener("click", function () { gridApi.reset(); });
    }
})();

// ===================================================================== //
//  TANDA 2B — Atajos de teclado del grid de líneas: F7 / F8.            //
//    F7 = duplicar la línea salmón en foco (crea una «Nueva» con los     //
//         mismos valores: POST standalone + PATCH campos).               //
//    F8 = copiar a la celda en foco el valor de la celda de ARRIBA       //
//         (misma columna, fila salmón anterior). Marca la fila como       //
//         editada (no guarda hasta pulsar «Guardar»).                    //
// ===================================================================== //
(function () {
    "use strict";
    var table = document.getElementById("lines-table");
    if (!table) { return; }
    var tbody = table.querySelector("tbody");
    if (!tbody) { return; }
    var dataTag = document.getElementById("document-data");
    if (!dataTag) { return; }
    var documentId;
    try { documentId = JSON.parse(dataTag.textContent).id; }
    catch (e) { documentId = (dataTag.dataset && dataTag.dataset.documentId) || null; }
    if (!documentId) { return; }

    function parseNumEs(value) {
        var s = String(value || "").trim();
        if (!s) { return null; }
        var n = parseFloat(s.replace(/\./g, function (m, off, str) {
            return str.indexOf(",") !== -1 ? "" : m;
        }).replace(",", "."));
        return isNaN(n) ? null : n;
    }

    // Cuerpo para .../conciliacion/campos a partir de los inputs de la fila.
    function camposBody(tr) {
        var out = {
            codigo_partida: null, descripcion: null, cantidad: null,
            unidad: null, precio_unitario: null, descuento: null, codigo_externo: null
        };
        [].forEach.call(tr.querySelectorAll(".js-cedit"), function (inp) {
            var f = inp.dataset.field;
            if (!f || !(f in out)) { return; }
            if (inp.dataset.numeric === "1") { out[f] = parseNumEs(inp.value); }
            else { var t = (inp.value || "").trim(); out[f] = t || null; }
        });
        return out;
    }

    function duplicateRow(tr) {
        var body = camposBody(tr);
        fetch("/api/documents/" + documentId + "/lines/standalone",
            { method: "POST", headers: { "Accept": "application/json" } })
            .then(function (r) {
                if (!r.ok) { throw new Error("standalone"); }
                return r.json();
            })
            .then(function (j) {
                var nid = j && j.valuation_line_id;
                if (!nid) { window.location.reload(); return null; }
                return fetch("/api/documents/" + documentId + "/lines/" + nid + "/conciliacion/campos",
                    { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
            })
            .then(function () { window.location.reload(); })
            .catch(function () { window.alert("No se pudo duplicar la línea."); });
    }

    function prevSalmonRow(tr) {
        var p = tr.previousElementSibling;
        while (p && !(p.classList.contains("conciliacion-row") && p.querySelector("td[data-col]"))) {
            p = p.previousElementSibling;
        }
        return p;
    }

    document.addEventListener("keydown", function (ev) {
        var active = document.activeElement;
        var inGridInput = active && active.classList &&
            active.classList.contains("js-cedit") && table.contains(active);

        if (ev.key === "F7") {
            var tr = active && active.closest ? active.closest("tr.conciliacion-row") : null;
            if (!tr || !table.contains(tr)) { return; }
            ev.preventDefault();
            if (!window.confirm(
                "¿Duplicar esta línea salmón? Se creará una copia «Nueva» con " +
                "los mismos valores (partida, concepto, cantidad, precio…)."
            )) { return; }
            duplicateRow(tr);
        } else if (ev.key === "F8") {
            if (!inGridInput) { return; }
            var td = active.closest("td[data-col]");
            var row = active.closest("tr.conciliacion-row");
            if (!td || !row) { return; }
            var col = td.getAttribute("data-col");
            var prev = prevSalmonRow(row);
            if (!prev) { return; }
            var ptd = prev.querySelector('td[data-col="' + col + '"]');
            if (!ptd) { return; }
            var pinp = ptd.querySelector(".js-cedit");
            var val = pinp ? pinp.value : ((ptd.getAttribute("data-sort-value") || "").trim());
            ev.preventDefault();
            active.value = val;
            active.dispatchEvent(new Event("input", { bubbles: true }));
            active.focus();
        }
    });
})();

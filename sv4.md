# albaranes-review-web (Servicio 4 / sv4)

> **Frontend humano** del sistema. Portal web de revisión, edición y aprobación
> de albaranes para los revisores de Construcciones Ruesma. **No es una SPA**:
> es **FastAPI + Jinja2 server-rendered** con un mínimo de JavaScript vanilla
> (~478 LOC) para interacciones puntuales (edición inline de líneas, re-fetch
> de contratos, selector dinámico de contrato).

---

## 1. ¿Qué hace exactamente?

`albaranes-review-web` es la cara visible del sistema para los revisores. Sus
responsabilidades:

1. **Bandeja paginada** de albaranes con filtros (búsqueda, aprobación, revisión
   requerida, rango de confianza), ordenación por columna y stats.
2. **Vista de detalle** de un albarán con:
   - Visor del PDF/imagen del albarán original (proxy de SharePoint vía Graph).
   - Selector de "fuente de datos" (vista merge consolidada · vista por proveedor
     LLM: openai · gemini · claude — para trazabilidad).
   - Bloque de contratos del ERP encontrados (0 / 1 / varios) con UI distinta
     en cada caso.
   - Banner de **valoración aplicada** con totales, conteo de matchings y flag
     de revisión requerida.
   - Formulario editable de campos extraídos + tabla editable de líneas
     (mezclando líneas del albarán con sintéticas de la valoración).
3. **Aprobación / desaprobación** de documentos.
4. **Re-fetch manual de contratos** del ERP cuando hay 0 contratos o el revisor
   ha corregido el CIF/obra (botones "Guardar y volver a buscar" / "Solo volver
   a buscar"). Llama directamente a `sigrid-api`.
5. **Edición de líneas sintéticas** (sub-tanda 2D): el revisor puede tocar
   campos visibles de las líneas M1-M7 (incrementos de hormigón) que generó
   sv5/sv6. Estos cambios se persisten directamente en `albaran_line_valuations`
   por `valuation_line_id`.

---

## 2. Lugar dentro del ecosistema

```
                  ┌──────────────────────────────────────┐
                  │   Revisor humano (Construcciones     │
                  │   Ruesma) – navegador                │
                  └─────────────────┬────────────────────┘
                                    │ HTTP
                                    ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  sv4 · albaranes-review-web        ← ESTE SERVICIO               │
   │  (FastAPI server-rendered + Jinja2 + JS vanilla)                 │
   │                                                                  │
   │  GET  /documents          (bandeja paginada)                     │
   │  GET  /documents/{id}     (detalle editable)                     │
   │  GET  /documents/{id}/preview  (proxy SharePoint del PDF)        │
   │  POST /documents/{id}/approve (form simple)                      │
   │  PUT  /api/documents/{id} (JSON: editar campos + líneas + sint.) │
   │  POST /api/documents/{id}/re-fetch-contratos                     │
   └────┬─────────────────────────┬──────────────────────────┬────────┘
        │ SELECT/UPDATE/DELETE    │ Graph (PDF preview +      │ POST SQL
        ▼                          │  upload contratos PDF)    ▼
   ┌──────────┐              ┌────┴─────┐               ┌──────────────┐
   │ Postgres │              │SharePoint│               │ sigrid-api   │
   │ (escribe │              │  (lee +  │               │ (Function    │
   │ tablas   │              │ escribe) │               │  on-prem,    │
   │ merge    │              └──────────┘               │  refetch     │
   │ + flags) │                                          │  contratos)  │
   └──────────┘                                          └──────────────┘

   ⚠ sv4 NO llama a sv5 ni a sv6 directamente — ver §13.2
```

> **Comparación con el ecosistema**: sv4 es uno de los **dos servicios que
> escriben** las tablas `albaran_documents_merge` / `albaran_lines_merge` (el
> otro es sv3). Coordina con sv6 vía BBDD compartida (lee `albaran_valuations`
> y `albaran_line_valuations` para mostrar la valoración en pantalla y para
> editar las sintéticas).

---

## 3. Arquitectura interna

```
albaranes-review-web/
├─ main.py                                                       # uvicorn.run(build_app(settings))
├─ config/
│  ├─ settings.py                                                # Pydantic-settings + database_url + admin_database_url
│  └─ logging_config.py
├─ domain/
│  ├─ models/
│  │  ├─ review_models.py                                        # ⭐ DTOs Pydantic completos del front
│  │  ├─ contrato_sigrid_models.py                               # ⚠ Réplica del sv3
│  │  └─ contrato_refetch_models.py                              # ContratoRefetchOutcome
│  └─ ports/
│     └─ contrato_refetch_port.py                                # Protocol: ContratoLookupClient + ContratoPdfStorage
├─ application/
│  └─ services/
│     ├─ review_service.py                                       # Fachada delgada sobre el repo (58 LOC)
│     ├─ contrato_refetch_service.py                             # ⭐ Orquesta refetch manual (366 LOC)
│     └─ obra_code_normalizer.py                                 # ⚠ Réplica EXACTA del sv3
├─ infrastructure/
│  ├─ database/
│  │  ├─ session_factory.py
│  │  ├─ orm_models.py                                           # Réplica del sv3 (sin tablas del sv6)
│  │  └─ review_repository.py                                    # ⭐ MONOLITO de 1581 LOC
│  ├─ graph/
│  │  └─ token_provider.py                                       # OAuth2 client_credentials
│  ├─ storage/
│  │  └─ sharepoint_contrato_pdf_storage.py                      # ⚠ Réplica focalizada del sv3
│  └─ sigrid/
│     └─ sigrid_api_contrato_client.py                           # ⚠ Réplica focalizada del sv3
├─ interface_adapters/
│  └─ web/
│     └─ app.py                                                  # FastAPI: rutas HTML + JSON + filtros Jinja
├─ templates/
│  ├─ base.html                                                  # Layout
│  ├─ documents_list.html                                        # Bandeja
│  └─ document_detail.html                                       # Detalle (629 líneas)
└─ static/
   ├─ styles.css                                                 # Estilos (458 LOC)
   └─ app.js                                                     # JS vanilla (478 LOC)
```

### Patrones aplicados

| Patrón                                    | Dónde                                                                | Por qué                                                                                  |
|-------------------------------------------|----------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| **Server-rendered (Jinja2)**              | `templates/*` + `app.py` rutas HTML                                  | Decisión consciente: una bandeja interna no justifica una SPA. Menos build, menos JS, más simple para el equipo. |
| **JS vanilla minimalista**                | `static/app.js`                                                      | Solo lo imprescindible: selector de contrato, alta/baja de filas, save, refetch.         |
| **Hexagonal / Ports & Adapters**          | `domain/ports` ↔ `infrastructure/*`                                  | Dos puertos: lookup ERP y storage PDF (Protocol-typed).                                  |
| **Service layer**                         | `ReviewService`, `ContratoRefetchService`                            | `ReviewService` es fachada delgada; `ContratoRefetchService` orquesta el refetch.        |
| **Repository (monolítico)**               | `AlbaranReviewRepository` (1581 LOC)                                 | Un único repo para todas las queries. Muy grande; ver §13.4 para mejora.                 |
| **Composition root**                      | `build_app(settings)`                                                | Cableado único, decide qué servicios opcionales se activan según `.env`.                 |
| **Inicialización perezosa idempotente**   | `_tables_ready()` + ALTERs en `initialize()`                         | sv4 puede arrancar antes que sv3. La inicialización **NO se cachea** hasta éxito real.   |
| **UPDATE in-place de líneas**             | `update_document()`                                                  | Crítico: si borrara líneas merge, las valoraciones del sv6 (FK CASCADE) se perderían.    |
| **DTO unificado para tabla de líneas**    | `DisplayLine`                                                        | El template itera **una sola lista** mezclando from_albaran + synthetic_modifier.        |
| **Proxy de SharePoint** (no expone token) | `GET /documents/{id}/preview`                                        | El navegador no necesita Graph token: el servidor descarga y reenvía con `Cache-Control: no-store`. |
| **Filtros Jinja2 personalizados**         | `_format_fecha_int_iso`, `_format_importe_eur`, `view_label`         | Formateo determinista en servidor (es-ES `335.370,42 €`, INT `20260115` → `2026-01-15`). |
| **Mix form-based + JSON API**             | `POST /approve` (form) vs `PUT /api/documents` (JSON)                | Formularios para acciones de un click; JSON para edición compleja desde JS.              |

---

## 4. Endpoints HTTP

### 4.1 HTML

| Método | Ruta                                          | Qué hace                                                                                          |
|--------|-----------------------------------------------|---------------------------------------------------------------------------------------------------|
| `GET`  | `/`                                           | Redirect 302 a `/documents`.                                                                      |
| `GET`  | `/documents`                                  | Bandeja paginada con filtros (search, approved, review_required, min/max confidence, sort, page). |
| `GET`  | `/documents/{id}?view=merge\|openai\|gemini\|claude` | Detalle. Vista `merge` editable; vistas por proveedor solo lectura (trazabilidad).         |
| `GET`  | `/documents/{id}/preview`                     | **Proxy** a SharePoint del PDF/imagen del albarán original. Descarga vía Graph, sirve inline.     |
| `POST` | `/documents/{id}/approve` (form)              | Aprueba desde la lista (sin abrir detalle). Conserva los filtros activos.                         |
| `POST` | `/documents/{id}/unapprove` (form)            | Marca como pendiente.                                                                              |

### 4.2 JSON

| Método | Ruta                                                   | Body                              | Qué hace                                                                                       |
|--------|--------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| `GET`  | `/health`                                              | —                                 | Estado: BBDD, page sizes, preview, refetch wired, etc.                                          |
| `GET`  | `/api/documents/{id}?view=...`                         | —                                 | `DocumentDetailPayload` completo (campos + líneas + display_lines + contratos + valuation).    |
| `PUT`  | `/api/documents/{id}`                                  | `MergeDocumentUpdatePayload`      | Guardar cambios. Update in-place de líneas + edición de sintéticas + recálculo de importes.    |
| `POST` | `/api/documents/{id}/re-fetch-contratos`               | `{}`                              | Re-buscar contratos en el ERP y actualizar BBDD. 503 si SIGRID no está configurado.            |

### 4.3 Detalle del payload `MergeDocumentUpdatePayload`

```json
{
  "proveedor_nombre": "ACME Hormigones, S.L.",
  "proveedor_cif": "B12345678",
  "fecha": "2026-04-12",
  "numero_albaran": "ALB-2026-0142",
  "forma_pago": "30 días",
  "obra_codigo": "0695",
  "obra_nombre": "Ampliación Polígono Norte",
  "obra_direccion": "...",
  "review_notes": "...",
  "selected_contrato_codigo": "C-2026-014",
  "approved": true,
  "approved_by": "Juan Pérez",
  "lines": [
    {
      "id": 4521,
      "line_index": 1,
      "codigo_imputacion": "OBRA-2026-014.04",
      "concepto": "Hormigón HM-25/B/20/IIa",
      "cantidad": 7.5,
      "precio": 87.5,
      "descuento": 0.0,
      "precio_neto": 656.25,
      "codigo": "HM25"
    }
  ],
  "valuation_line_updates": [
    {
      "valuation_line_id": 9912,
      "codigo_partida_final": "OBRA-2026-014.04",
      "descripcion_linea": "INCREMENTO POR EXCESO DE TIEMPO",
      "cantidad_albaran": 33,
      "unidad_contrato": "min",
      "precio_unitario_final": 0.5,
      "importe_calculado": 16.5
    }
  ]
}
```

---

## 5. Vista de detalle (`document_detail.html`, 629 líneas)

### 5.1 Tres bloques visuales

```
┌────────────────────────────────────────────────────────────┐
│  Header: título · selector de vista · botones Volver/Open  │
├────────────────────────────────────────────────────────────┤
│  Banner amarillo si vista no editable (ej. proveedor LLM)  │
│  Banner azul si hay valoración aplicada                    │
├────────────────────────────────────────────────────────────┤
│  Split layout (50% / 50%):                                 │
│  ┌──────────────────────┐   ┌──────────────────────────┐  │
│  │  Visor de PDF        │   │  Bloque contratos        │  │
│  │  (iframe a /preview) │   │  (0/1/varios)            │  │
│  │                      │   ├──────────────────────────┤  │
│  │                      │   │  Formulario editable     │  │
│  │                      │   │  (campos + líneas)       │  │
│  │                      │   ├──────────────────────────┤  │
│  │                      │   │  Snapshots por proveedor │  │
│  │                      │   │  (collapsible JSON)      │  │
│  └──────────────────────┘   └──────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

### 5.2 Bloque de contratos (lógica condicional)

| Cuántos contratos | Pinta                                                                                                                     |
|-------------------|--------------------------------------------------------------------------------------------------------------------------|
| **0**             | Alert amarillo con CIF/obra, botones **Guardar y volver a buscar** y **Solo volver a buscar**, status panel para feedback. |
| **1**             | Tarjeta del único contrato con sus datos + link al PDF si existe. `selected_contrato_codigo` se rellena por defecto.    |
| **>1**            | Selector dropdown + tarjetas (todas excepto la seleccionada con `display:none`). JS las muestra/oculta al cambiar el `<select>` y sincroniza el botón "Abrir contrato en SharePoint" del header con el PDF correcto. |

### 5.3 Tabla de líneas — **decisión arquitectónica clave**

El `<tbody>` se renderiza en **servidor**, no en JS. Razón: hay que mezclar
valores extraídos del albarán con valores valorados (sv6) y producir el flag
`is_valued` para CSS `.valued`. Hacerlo en JS implicaría:
- Pasar todo el contexto de valoración como JSON en el HTML.
- Replicar la lógica de mezcla en JavaScript.
- Riesgo de divergencia con el backend.

Soluciones aplicadas:
- **Servidor itera `display_lines`** (DTO unificado, ver §6) y pinta cada fila
  con sus inputs correctamente preconfigurados.
- **JS solo gestiona alta/baja de filas y la recolección al guardar.**
- Cada `<tr>` lleva `data-line-kind="from_albaran"|"synthetic_modifier"` y
  `data-line-id` / `data-valuation-line-id`. El JS los usa para decidir a qué
  colección del payload va cada fila al recolectar.

#### Comportamiento por columna según `line_kind`

| Columna             | from_albaran        | synthetic_modifier       |
|---------------------|---------------------|---------------------------|
| `codigo_imputacion` | editable            | editable (`codigo_partida_final`) |
| `concepto`          | editable            | editable (`descripcion_linea`)    |
| `cantidad`          | editable            | editable (`cantidad_albaran`)     |
| `unidad`            | **read-only** (display) | editable (`unidad_contrato`)  |
| `precio_unitario`   | **read-only** (display) | editable (`precio_unitario_final`) |
| `importe`           | editable (→ `precio_neto`) | editable (`importe_calculado`) |
| `descuento`         | editable            | **read-only**             |
| `codigo`            | editable            | **read-only**             |
| `Eliminar`          | botón visible       | **no se permite borrar** (solo desaparecen al re-valorar) |

### 5.4 Inputs `_display`

Los inputs cuyo `data-field` termina en `_display` (`unidad_display`,
`precio_unitario_display`) son **puramente informativos**: el JS los **ignora**
en `collectLines()`. Sirven para enseñar al revisor el valor del contrato (que
no se persiste en `albaran_lines_merge` porque pertenece a la valoración).

---

## 6. `DisplayLine` — el DTO unificado

`_build_display_lines()` ([review_repository.py:1354](#)) construye una lista
de `DisplayLine` mezclando:

1. **Líneas merge** (from_albaran), iteradas en orden por `line_index`.
2. **Sintéticas** que cuelgan de cada base, agrupadas por `parent_merge_line_id`
   y colocadas inmediatamente después de su base.
3. **Sintéticas huérfanas** (parent no encontrado) al final, con
   `line_index = len(merge_lines) + N`. Raro pero soportado defensivamente.

```python
@dataclass
class DisplayLine:
    line_kind: str                 # 'from_albaran' | 'synthetic_modifier'
    merge_line_id: int | None      # solo si from_albaran
    valuation_line_id: int | None  # solo si synthetic_modifier
    line_index: int | None
    codigo_imputacion: str | None  # del valorado o del merge
    concepto: str | None
    cantidad: float | None         # cantidad_convertida si valorada, sino del merge
    unidad: str | None
    precio_unitario: float | None
    importe: float | None
    descuento: float | None
    codigo: str | None
    confianza_pct: float | None
    is_valued: bool                # True → CSS class .valued
    parent_merge_line_id: int | None
```

**Lógica de mezcla** (importante):

```
codigo_imputacion = valoracion.codigo_partida_final  o  merge.codigo_imputacion
cantidad          = valoracion.cantidad_convertida   o  merge.cantidad
unidad            = valoracion.unidad_contrato       o  valoracion.unidad_albaran
precio_unitario   = valoracion.precio_unitario_final  (None si no valorada)
importe           = valoracion.importe_calculado     o  merge.precio_neto
```

> El objetivo es **enseñar al revisor el valor que se va a facturar**. Si la
> línea está valorada, los datos del valorado son los autoritativos (con CSS
> `.valued` para que se distinga visualmente).

---

## 7. Endpoint `/documents/{id}/preview` (proxy SharePoint)

**Patrón curioso**: el iframe del visor apunta a una URL **del propio sv4**, no
a SharePoint directamente. El servidor:

1. Valida que `preview_enabled` (hay `GRAPH_KEY` y `SHAREPOINT_DRIVE_ID`).
2. Obtiene el `relative_path` del albarán de la BBDD.
3. **Llama a Graph 2 veces**:
   - `GET /v1.0/drives/{drive_id}/root:/{path}` → metadata, extrae `id`.
   - `GET /v1.0/drives/{drive_id}/items/{id}/content` → bytes.
4. Devuelve `Response(content=bytes, media_type=guessed, Cache-Control=no-store)`.
5. **Si algo falla**, devuelve un `_preview_error_response()` HTML inline con
   un card que muestra el mensaje + botón "Abrir documento en SharePoint" como
   fallback (link directo al `document_url` original).

**Por qué proxy y no link directo a SharePoint**:
- El revisor no puede ver el iframe de SharePoint embebido sin login del usuario.
- El servidor sí tiene un token de aplicación (client_credentials).
- Cache-Control no-store: cada preview es una petición fresca a Graph (ver mejora #11).

**Detección de tipo MIME** (`_guess_media_type`): basada en sufijo del filename
(`.pdf`, `.jpg/.jpeg`, `.png`, `.webp`). Por defecto `application/octet-stream`.

---

## 8. Re-fetch manual de contratos

Funcionalidad sólida y bien pensada. Cuando un albarán llega con 0 contratos
(sv3 no encontró match en el ERP), el revisor ve el alert amarillo con dos
botones:

### 8.1 Flujo "Guardar y volver a buscar"

1. JS llama a `PUT /api/documents/{id}` con los campos editados (CIF, obra, etc.).
2. Espera respuesta OK (en BBDD ya están los nuevos valores).
3. JS llama a `POST /api/documents/{id}/re-fetch-contratos`.
4. Backend → `ContratoRefetchService.refetch()`:
   - Lee `(cif, obra)` actuales del merge (los recién guardados).
   - Normaliza `obra_codigo` con `normalize_obra_code()` (idéntico al sv3).
   - Si falta CIF u obra inválida → `skipped_missing_data` (no llama a Sigrid).
   - Llama a `SigridApiContratoClient.fetch_contratos(cif, obra_norm)`.
   - **Reutilización de PDFs**: si `gra_rep_ide` nuevo == previo → reusa los
     `pdf_sharepoint_*` paths inyectándolos en el DTO antes del replace.
   - `replace_contratos_and_select(contratos, selected=None|único)`.
   - Si exactamente 1 → auto-selecciona; si 0/varios → NULL.
   - Para los contratos **no reutilizados** y con `gra_rep_ide`: descarga el
     PDF de Sigrid (vía `/api/documents/read` de la sigrid-api), lo sube a
     SharePoint y actualiza paths en BBDD. **Best-effort**: un fallo no bloquea
     los demás ni el outcome global.
5. Devuelve `ContratoRefetchOutcome` con uno de 5 status:

| status                  | Significado                                                |
|-------------------------|------------------------------------------------------------|
| `skipped_missing_data`  | Faltan CIF u obra inválida; no se llamó a Sigrid.           |
| `no_results`            | Sigrid OK pero 0 contratos.                                 |
| `found_single`          | 1 contrato → auto-seleccionado.                             |
| `found_multiple`        | >1 contratos → revisor elige en el dropdown.                |
| `sigrid_error`          | Fallo de red/5xx/persistencia. Contratos previos intactos. |

### 8.2 Feedback en el front

JS (`handleRefetchOutcome`) pinta un status colorizado:
- `success` (found_single, found_multiple) — verde.
- `warning` (no_results, skipped_missing_data) — amarillo.
- `error` (sigrid_error) — rojo.
- `info` (cualquier otro) — azul.

**Si `count > 0`** → recarga la página tras 700 ms para que el revisor vea los
contratos nuevos pintados en el bloque correspondiente.

---

## 9. Inicialización idempotente — el detalle más importante del `repository`

`AlbaranReviewRepository.initialize()` se llama **al arrancar** y al inicio de
cada operación. Tiene una particularidad crítica:

```python
def initialize(self) -> bool:
    if self._initialized and self._tables_ready():
        return True
    self._rename_legacy_tables_if_needed()
    if not self._tables_ready():
        return False                # NO se cachea — reintentar en la próxima
    with session: ... apply ALTERs ...
    self._initialized = True
    return True
```

**Bug histórico que esto resuelve** (documentado en código):

> *"Antes, si sv4 arrancaba antes que sv3 creara las tablas, marcábamos
> `initialized=True` sin haber ejecutado nada, y cuando sv3 ya las había
> creado después, sv4 nunca llegaba a añadir sus columnas → la primera query
> cascaba por 'no existe columna `approved`'"*.

La solución: NO cachear el flag hasta que se ejecuta DDL con éxito. Cada
llamada al endpoint reintenta hasta que las tablas merge existen y los
ALTER pueden aplicarse.

### 9.1 Qué añade sv4 al schema

Vía `ALTER TABLE ADD COLUMN IF NOT EXISTS`:

```
albaran_documents_merge:
  + approved BOOLEAN DEFAULT FALSE NOT NULL
  + approved_at_utc VARCHAR(64)
  + approved_by VARCHAR(255)
  + reviewed_at_utc VARCHAR(64)
  + last_modified_at_utc VARCHAR(64)
  + review_notes TEXT
  + selected_contrato_codigo VARCHAR(64)

albaran_contratos_merge:
  + gra_rep_ide INTEGER
  + pdf_sharepoint_relative_path VARCHAR(1024)
  + pdf_sharepoint_web_url VARCHAR(1024)

CREATE TABLE albaran_contrato_lines_merge (...) IF NOT EXISTS
  + índices

CREATE INDEX IF NOT EXISTS:
  ix_albaran_documents_merge_approved
  ix_albaran_documents_merge_conf_calc
```

> **Acoplamiento de schema**: sv3 y sv4 ambos crean
> `albaran_contrato_lines_merge`. Ambos `CREATE IF NOT EXISTS`, así que es
> idempotente en producción, pero es **una alarma de diseño**: dos servicios
> tocan el schema de la misma tabla. Si difirieran las definiciones, gana el
> que arranque primero.

### 9.2 Rename de tablas legacy

```python
albaran_documents_gem  →  albaran_documents_merge
albaran_lines_gem      →  albaran_lines_merge
```

Código de migración antigua que sigue ahí. Se ejecuta automáticamente al
arrancar si las tablas legacy existen y las nuevas no. Probable que ya no se
dispare en ningún despliegue real.

---

## 10. Variables de entorno

| Variable                     | Default                | Descripción                                                                |
|------------------------------|------------------------|----------------------------------------------------------------------------|
| `PG_HOST`                    | `localhost`            |                                                                            |
| `PG_PORT`                    | `5432`                 |                                                                            |
| `PG_DB`                      | `albaranes`            | Compartida con sv3/sv5/sv6.                                                |
| `PG_USER`                    | `postgres`             |                                                                            |
| `PG_PASSWORD`                | *obl.*                 |                                                                            |
| `PG_ADMIN_DB`                | `postgres`             | Para auto_create_database.                                                 |
| `PG_ADMIN_USER`              | `postgres`             |                                                                            |
| `PG_ADMIN_PASSWORD`          | *obl.*                 |                                                                            |
| `AUTO_CREATE_DATABASE`       | `true`                 | Si la BBDD no existe la crea (igual que sv3).                              |
| `SIGRID_API_BASE_URL`        | *opcional*             | Si falta, refetch endpoint devuelve 503.                                   |
| `SIGRID_API_FUNCTION_KEY`    | *opcional*             |                                                                            |
| `SIGRID_API_DATABASE`        | *opcional*             |                                                                            |
| `SIGRID_API_TIMEOUT_S`       | `30.0`                 |                                                                            |
| `GRAPH_KEY`                  | *opcional*             | Si falta, preview de PDF no funciona.                                      |
| `SHAREPOINT_DRIVE_ID`        | *opcional*             | Idem.                                                                       |
| `GRAPH_TIMEOUT_S`            | `60`                   |                                                                            |
| `API_HOST`                   | `127.0.0.1`            |                                                                            |
| `API_PORT`                   | **`8002`**             | ⚠ **Mismo puerto que sv5** — ver §13.1                                      |
| `LOG_LEVEL`                  | `INFO`                 |                                                                            |
| `LOG_DIR`                    | `logs`                 |                                                                            |
| `SERVICE_VERSION`            | `1.0.0`                |                                                                            |
| `APP_TITLE`                  | `Revisión de Albaranes IA` | Se usa en `<title>` y header.                                          |
| `DEFAULT_PAGE_SIZE`          | `25`                   |                                                                            |
| `MAX_PAGE_SIZE`              | `100`                  |                                                                            |
| `DEFAULT_REVIEWER`           | *opcional*             | Pre-rellena `approved_by` si no se pasa explícitamente.                    |

> **`preview_enabled`** es una propiedad calculada: `bool(graph_key AND sharepoint_drive_id)`. Si cualquiera falta, el endpoint `/preview` devuelve un HTML de error con link a SharePoint como fallback (sin cascar).

> **`contrato_refetch_wired`** es similar: el endpoint solo se activa si hay las 3 SIGRID_API_* presentes. Si falta cualquiera, devuelve 503 con un mensaje útil.

### Ejemplo `.env`

```dotenv
# BBDD compartida
PG_HOST=localhost
PG_PORT=5432
PG_DB=albaranes
PG_USER=albaranes_app
PG_PASSWORD=********
PG_ADMIN_DB=postgres
PG_ADMIN_USER=postgres
PG_ADMIN_PASSWORD=********
AUTO_CREATE_DATABASE=true

# sigrid-api (opcional para refetch)
SIGRID_API_BASE_URL=https://func-sigrid-dev-spaincentral-001.azurewebsites.net
SIGRID_API_FUNCTION_KEY=xxxxxxxxxxxxxxxx
SIGRID_API_DATABASE=ruesma_dev
SIGRID_API_TIMEOUT_S=30.0

# SharePoint (opcional para preview + subida tras refetch)
GRAPH_KEY={"tenant_id":"...","client_id":"...","client_secret":"..."}
SHAREPOINT_DRIVE_ID=b!...
GRAPH_TIMEOUT_S=60

# Servidor
API_HOST=0.0.0.0
API_PORT=8004      # ⚠ NO usar 8002 si sv5 corre en local
LOG_LEVEL=INFO
LOG_DIR=logs
APP_TITLE="Revisión de Albaranes IA"
DEFAULT_PAGE_SIZE=25
MAX_PAGE_SIZE=100
DEFAULT_REVIEWER="Equipo Ruesma"
```

---

## 11. Flujo del guardado (PUT /api/documents/{id})

```
1. PUT /api/documents/{id} { campos + lines + valuation_line_updates + approved? }

2. ReviewService.save_document(...) → AlbaranReviewRepository.update_document(...)

3. UPDATE in-place de cabecera:
   - Campos: proveedor, CIF, fecha, número, forma_pago, obra_*, review_notes, ...
   - reviewed_at_utc + last_modified_at_utc = ahora.
   - selected_contrato_codigo: validado contra los códigos existentes (NULL si no existe).
   - approved + approved_at_utc + approved_by si payload.approved.

4. Persistencia de líneas (UPDATE in-place, NO delete+insert):
   a) DELETE solo las líneas que desaparecieron (por id).
   b) Para cada línea entrante:
        - Si tiene id → UPDATE por id.
        - Si no → INSERT nueva.
   c) flush.

5. Edición de sintéticas (sub-tanda 2D):
   - Solo si payload.valuation_line_updates no vacía.
   - Validación de seguridad: filtra por (valuation.document_id = X AND line_kind = 'synthetic_modifier').
   - UPDATE por valuation_line_id de campos editables.

6. Recálculo de importes valorados:
   - Si el revisor cambió cantidades de líneas from_albaran:
     · cantidad_convertida = factor × nueva_cantidad
     · importe_calculado = pu × cantidad_efectiva
     · importe_source pasa a 'calculated'
   - UPDATE total_valorado en la cabecera de la valoración.

7. session.commit()

8. Re-fetch del detalle completo y devuelve SaveResponse con redirect_url.
```

### 11.1 Por qué UPDATE in-place y no DELETE+INSERT (decisión muy importante)

```python
# Importante: las valoraciones del servicio 6 (tabla
# albaran_line_valuations) tienen FK
#   merge_line_id -> albaran_lines_merge.id ON DELETE CASCADE
# Si borráramos y reinsertáramos las líneas merge, la valoración
# entera se perdería en cada save. Por eso:
#   - Las líneas con id conocido se UPDATE en su sitio.
#   - Las líneas nuevas (sin id) se INSERT.
#   - Las líneas que estaban y el revisor eliminó (ya no vienen en
#     el payload) se DELETE explícitamente.
# Así los ids sobreviven y la valoración asociada también.
```

Excelente decisión. Sin ella, **cada save destruiría la valoración** porque el
DELETE en cascade eliminaría las filas en `albaran_line_valuations`. La regla
es: **conserva el id**, y la valoración asociada sobrevive.

---

## 12. Decisiones técnicas relevantes

1. **No-SPA, server-rendered.** Decisión consciente: una bandeja interna no
   justifica un build de Vue/React/Svelte ni un equipo de frontend. Jinja2 +
   478 LOC de JS vanilla es suficiente y mucho más mantenible.
2. **`DisplayLine` como DTO unificado.** Mezcla from_albaran + synthetic_modifier
   en una sola lista ordenada. El template itera una sola vez. El JS distingue
   por `data-line-kind` qué endpoint del payload corresponde a cada fila.
3. **UPDATE in-place de líneas merge** (§11.1) — la decisión más importante de
   este servicio. Sin ella, el sistema pierde la valoración en cada save.
4. **Inicialización perezosa idempotente** que NO cachea hasta éxito real
   (§9). Resuelve un bug histórico de carrera con sv3.
5. **Filtros Jinja2 en español** (`importe_eur`, `fecha_int_iso`) — formateo
   determinista en servidor para no depender del locale del cliente.
6. **Proxy de SharePoint** (`/preview`) — el navegador no necesita conocer el
   token de Graph; el servidor descarga y reenvía con `Cache-Control: no-store`.
7. **Replicación intencionada de código entre sv3 y sv4** —
   `obra_code_normalizer` lleva el comentario explícito: *"Debe coincidir
   EXACTAMENTE con la función homónima del servicio 3 para que re-ejecutar la
   búsqueda desde el portal dé los mismos resultados que la búsqueda automática
   al persistir."* Es una decisión consciente: **prefieren duplicación a
   acoplamiento de paquetes**, y se controla con tests/inspección humana.
8. **`SigridApiContratoClient` y `SharePointContratoPdfStorage` también son
   réplicas focalizadas del sv3.** Mismo razonamiento: cada microservicio se
   queda con sus propias dependencias mínimas.
9. **5 outcomes explícitos del refetch** (`skipped_missing_data`, `no_results`,
   `found_single`, `found_multiple`, `sigrid_error`) con mensajes pensados para
   pintarse directamente en el front, sin ramas adicionales en JS.
10. **`approved_by` es texto libre** — el revisor escribe su nombre. No hay
    auth ni Entra ID. Decisión consciente del cliente para esta fase del
    proyecto. Lo dejo como mejora #5.
11. **Mix form-based + JSON API** para acciones simples vs complejas.
    Aprobar desde la lista es un POST de formulario; editar desde el detalle es
    un PUT JSON desde JS.

---

## 13. Limitaciones conocidas y mejoras propuestas

| #  | Limitación                                                                                                          | Mejora propuesta                                                                                                  |
|----|---------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| 1  | ⚠ **`API_PORT=8002` igual que sv5** — conflicto evidente en local                                                   | Cambiar default a 8004 (sv1 daemon, sv2: 8000, sv3: 8001, sv5: 8002, sv6: 8003 → sv4: 8004). Es un fix trivial.   |
| 2  | 🔥 **`sv4` NO dispara la re-valoración cuando el revisor cambia `selected_contrato_codigo`**                          | Ver §13.2 — hallazgo arquitectónico crítico.                                                                      |
| 3  | Duplicación de código sv3↔sv4 (Sigrid client, SharePoint storage, obra_code_normalizer, ORM models)                  | Extraer un paquete común `albaranes-shared` con esos módulos, o generar SDK desde el OpenAPI del sigrid-api.       |
| 4  | `review_repository.py` = 1581 LOC                                                                                    | Descomponer en `DocumentsListReader`, `DocumentDetailReader`, `DocumentWriter`, `ContratosWriter`, `ValuationLineEditor`. |
| 5  | Sin auth — cualquiera con acceso al puerto puede aprobar                                                             | Easy Auth (Entra ID) en Container App + leer identidad del header `X-MS-CLIENT-PRINCIPAL-NAME` para `approved_by`.  |
| 6  | Sin CSRF en formularios POST (`/approve`, `/unapprove`)                                                              | Añadir token CSRF (FastAPI tiene paquetes de middleware listos).                                                  |
| 7  | Sin paginación de líneas en el detalle                                                                                | Si una factura llega con 200+ líneas, virtualizar la tabla (ej. con Intersection Observer o paginación servidor). |
| 8  | El proxy `/preview` no cachea — cada visualización son 2 llamadas a Graph                                            | Cache LRU en memoria por `(drive_id, relative_path, sha256)` con TTL corto (5 min).                               |
| 9  | El `_rename_legacy_tables_if_needed` (gem→merge) es código muerto en producción actual                                | Eliminarlo en una limpieza futura cuando se confirme que ningún despliegue tiene tablas legacy.                   |
| 10 | Sin soft-delete del historial — al editar, se sobrescribe el dato anterior                                           | Tabla `albaran_documents_merge_history` con triggers de auditoría, o `temporal tables` de Postgres 14+.          |
| 11 | Edición concurrente sin control optimista (dos revisores editando el mismo documento)                                | Añadir `version BIGINT` en `albaran_documents_merge` y comprobarla en el UPDATE.                                  |
| 12 | El JS recolecta toda la tabla en cada save (no diff)                                                                  | Solo enviar las filas modificadas. Pero solo merece la pena si hay >50 líneas/factura en producción.              |
| 13 | El refetch service llama 1 a 1 `download + upload` PDFs                                                              | `ThreadPoolExecutor` para paralelizar (suelen ser 1-3 contratos, ganancia marginal).                              |
| 14 | Replica DDL `albaran_contrato_lines_merge` con sv3 (ambos `IF NOT EXISTS`)                                            | Migrar a Alembic en uno solo (idealmente sv3) y que sv4 solo lea.                                                 |

### 13.2 Hallazgo crítico — falta de integración con sv6

**Problema**: cuando el revisor cambia el `selected_contrato_codigo` en el
detalle (selector dropdown si hay >1 contratos) y pulsa "Guardar", sv4
**solo escribe en `albaran_documents_merge.selected_contrato_codigo`**. No
llama a sv6, no dispara re-valoración. Resultado: la valoración persistida
en `albaran_valuations` queda obsoleta apuntando al contrato anterior.

**Verificado**: `grep -rn "valuation_api\|run-async\|/v1/valuation" sv4/` →
**cero coincidencias**. sv4 no integra con sv6 en absoluto.

**Comparación con sv3**: sv3 PATCH `/v1/albaranes/{id}/selected-contrato`
**sí** dispara `sv6 /v1/valuation/run-async` cuando recibe `trigger_valuation=True`.
sv4 no replica este comportamiento.

**Tres opciones de solución (en orden de menos a más invasiva)**:

| Opción | Descripción                                                                                              | Pros / Contras                                                                                  |
|--------|----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| **A**  | sv4 detecta cambio de `selected_contrato_codigo` y llama a `sv6 /v1/valuation/{id}/re-run` tras commit.   | Limpia, mínima. Requiere `VALUATION_API_BASE_URL` en sv4 y un cliente HTTP.                      |
| **B**  | sv4 llama a `sv3 PATCH /selected-contrato` con `trigger_valuation=True` en lugar de escribir a BBDD.      | Centraliza la lógica en sv3 (single source of truth). Pero requiere que sv3 sea reachable.      |
| **C**  | El UI muestra un botón explícito **"Re-valorar"** que dispara `sv6 /re-run`.                              | Da control al revisor. Riesgo: olvidan pulsarlo y aprueban con valoración obsoleta.             |

Mi recomendación: **A**, con un cliente HTTP simple igual que el
`HttpValuationIaClient` de sv6 pero apuntando al `re-run` de sv6. Disparar en
background (`BackgroundTasks` o `asyncio.create_task`) para no bloquear el save
del revisor. Y, como red de seguridad, añadir el botón de la opción **C** para
casos de fallo silencioso.

> Es un bug claro **a corto plazo** porque el revisor verá el banner de
> valoración apuntando a otro contrato sin entender por qué. **Prioridad alta**
> para la próxima iteración.

---

## 14. Cómo se invoca este servicio

### 14.1 Quién lo llama

- **Revisores humanos** desde un navegador. URL típica:
  `http://review.albaranes.ruesma.local/documents`.

### 14.2 Arranque local

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env       # rellenar PG_*, SIGRID_API_*, GRAPH_KEY...
python main.py               # uvicorn en API_HOST:API_PORT
```

> ⚠ Asegúrate de que `API_PORT` ≠ 8002 si tienes sv5 corriendo en local.

### 14.3 Decisión de despliegue Azure

**Azure Container App** con:
- `minReplicas=1`, `maxReplicas=2-3`.
- **Ingress externo** con FQDN custom (es la única vía de entrada de los
  revisores humanos).
- **Easy Auth (Entra ID)** activado para autenticación → resolvería §13.5 y §13.6.
- Identidad gestionada para Key Vault (`PG_PASSWORD`, `GRAPH_KEY`,
  `SIGRID_API_FUNCTION_KEY`).
- `WEBSITES_PORT=8004` o el que se elija.
- Health check en `/health`.

> El front es la única pieza del sistema que **necesita ingress externo**. El
> resto (sv2, sv3, sv5, sv6) son internos al spoke.

---

## 15. Inputs / Outputs del servicio

### Inputs

| Origen          | Naturaleza                                                                          |
|-----------------|-------------------------------------------------------------------------------------|
| HTTP (humanos)  | Navegación HTML + envíos de formulario + llamadas JSON desde JS                     |
| `.env`          | Configuración estática (BBDD, SIGRID, Graph, paginación, título, revisor por defecto) |
| PostgreSQL      | Lectura de `albaran_documents_merge`, `albaran_lines_merge`, `albaran_contratos_merge`, `albaran_contrato_lines_merge`, `albaran_valuations`, `albaran_line_valuations` |
| sigrid-api      | Refetch de contratos (`/api/sql/read`) + descarga de PDFs (`/api/documents/read`)   |
| SharePoint/Graph| Descarga de PDFs/imágenes para preview, subida de PDFs de contratos tras refetch    |

### Outputs

| Destino          | Naturaleza                                                                   |
|------------------|------------------------------------------------------------------------------|
| HTTP (humanos)   | HTML rendered (Jinja2) + JSON API + `Response(bytes)` para preview           |
| PostgreSQL       | UPDATE in-place + DELETE selectivo + UPDATE de sintéticas de valoración + recálculo de importes |
| SharePoint/Graph | Subida de PDFs de contratos al refetch (mismo storage que sv3)               |
| Filesystem       | Logs rotados (`logs/review_web.log`)                                          |

### Lo que **no** hace

- No llama a sv2, sv3, sv5 ni sv6 directamente.
- No procesa albaranes nuevos (eso es sv1+sv2+sv3).
- No genera valoraciones (eso es sv5+sv6).
- No envía notificaciones, emails ni mensajes.
- No tiene WebSockets ni SSE.

---

## 16. Frontera del microservicio

sv4 es la cara visible del sistema y, **por construcción**, es el más acoplado
con sv3 (comparten escritura de las tablas merge). Esta es una de las facetas
incómodas del modelo "microservicios + BBDD compartida": cuando varios
servicios escriben las mismas tablas, las fronteras se difuminan.

**Lo que está bien delimitado**:
- Toda la presentación está aquí (templates, JS, CSS, filtros Jinja).
- Toda la edición humana del merge está aquí (no se hace desde sv3).
- El refetch manual está aquí (sv3 hace refetch automático al persistir; sv4
  hace refetch manual desde el portal).

**Lo que se sale de la frontera**:
- Replicación de código con sv3: documentada con el comentario "duplicación
  consciente entre microservicios". Decisión legítima pero hay que mantenerla
  sincronizada.
- DDL compartido (`albaran_contrato_lines_merge` lo crean sv3 y sv4). Mejor
  que solo lo cree sv3.
- **Falta de integración con sv6** (§13.2) — el revisor edita el contrato
  pero la valoración no se entera. Esto sí es un fallo de delimitación.

**Mejora estructural que merece la pena**:

> Un servicio `albaranes-merge-store-api` que encapsule todas las escrituras a
> las tablas merge, y que sv3 y sv4 lo consuman vía HTTP. Centralizaría la
> lógica de UPDATE in-place (§11.1) y eliminaría la mayor parte de la
> duplicación. **Pero es over-engineering** si el sistema no va a crecer mucho
> más; la duplicación actual está bien acotada y documentada.

---

## 17. Resumen de un vistazo

| Característica         | Valor                                                                                |
|------------------------|--------------------------------------------------------------------------------------|
| Tipo                   | Web app server-rendered (FastAPI + Jinja2 + JS vanilla)                              |
| Lenguaje               | Python 3.12 (servidor) + JS ES2018+ (cliente, sin transpilación)                     |
| Páginas                | Bandeja `/documents` · Detalle `/documents/{id}` con vistas merge/openai/gemini/claude |
| API JSON               | GET/PUT `/api/documents/{id}` · POST `/api/documents/{id}/re-fetch-contratos`         |
| Persistencia propia    | PostgreSQL — añade columnas a tablas del sv3 (ALTERs idempotentes)                    |
| Storage externo        | SharePoint/Graph (preview de albarán + subida de PDFs de contratos tras refetch)     |
| LLMs                   | Ninguno (solo lee `raw_extraction_json` para mostrarlo)                              |
| Concurrencia           | uvicorn workers (sin BackgroundTasks porque no hay tareas asíncronas)                |
| Despliegue objetivo    | Azure Container App con **ingress externo + Easy Auth (Entra ID)**                    |
| Punto de entrada       | `python main.py`                                                                     |
| Dependencias clave     | `fastapi`, `uvicorn`, `jinja2`, `sqlalchemy`, `psycopg`, `httpx`, `pydantic-settings` |
| Servicios upstream     | Revisores humanos (navegador)                                                        |
| Servicios downstream   | PostgreSQL · SharePoint/Graph · sigrid-api                                           |
| Cierra el ciclo        | **No**, pero es la cara visible donde el ciclo del ecosistema desemboca               |

---

*Documento generado a partir del análisis del código del paquete `sv4.zip` aportado.*

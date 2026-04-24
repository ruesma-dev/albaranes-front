# Fix 2D svc4 v2 — Sintéticas editables en la UI

## Contexto

Segunda iteración del fix del svc4 tras la Sub-tanda 2D. La primera
iteración (fix_2D_svc4.zip) arreglaba el crash al abrir el detalle
pero dejaba las sintéticas invisibles en la UI.

Este v2 hace que las líneas sintéticas de valoración aparezcan como
**una fila más** en la tabla del detalle, **editables** e
**indistinguibles visualmente** de las líneas normales del albarán.
Las ediciones sobre sintéticas se persisten en
`albaran_line_valuations` (preserva `modifier_source`,
`modifier_reason` y demás metadatos de trazabilidad).


## Qué cambia

### Para el revisor
- Al abrir un albarán valorado con 2D, la tabla de líneas muestra
  **todas las filas** (base del albarán + modificadores sintéticos).
- Todas son editables: concepto, cantidad, unidad, precio unitario,
  importe, código imputación.
- No hay marcador visual que las distinga. La única diferencia: las
  sintéticas no tienen botón "Eliminar" (desaparecen solo al
  re-valorar) ni son "descuento" ni "código de producto" editables.

### Para el backend
- `MergeDocumentUpdatePayload` ahora acepta un nuevo campo
  `valuation_line_updates: list[ValuationLineUpdate]` con las
  ediciones del revisor sobre filas sintéticas.
- El save aplica esas ediciones como UPDATE a
  `albaran_line_valuations` por `id` (la PK de la fila).
- `DocumentDetailPayload` ahora incluye `display_lines`: una lista
  unificada de filas a pintar en la UI (mezcla líneas del merge +
  sintéticas en orden natural, cada sintética justo tras su base).
- `LineValuationPayload` expone ahora `valuation_line_id` (la PK de
  la fila) para que el front pueda identificarla al editar.


## Ficheros a sustituir

```
domain/models/review_models.py
infrastructure/database/review_repository.py
templates/document_detail.html
static/app.js
```

Los cuatro se sustituyen por completo. Ninguno requiere cambios
manuales adicionales.


## Qué NO cambia

- `app.py`: sin cambios.
- `review_service.py`: sin cambios (delega en el repo).
- BBDD: sin DDL nuevo. La Sub-tanda 2D ya había añadido las columnas
  necesarias.
- CSS: sin cambios (decisión de UI: las sintéticas no llevan
  marcador visual).
- Vistas readonly de proveedor (OpenAI/Gemini/Claude): sin cambios.
  Esas vistas son de trazabilidad del OCR crudo y no incluyen
  valoración, por lo que tampoco incluyen sintéticas.


## Modelo de datos

### En `albaran_line_valuations`

Las filas sintéticas tienen:
- `line_kind = 'synthetic_modifier'`
- `merge_line_id IS NULL`
- `parent_merge_line_id` apunta al `merge_line_id` de la base
- `descripcion_linea`, `modifier_source`, `modifier_reason` rellenos

Las ediciones del revisor impactan solo estos campos:
- `codigo_partida_final`
- `descripcion_linea`
- `cantidad_albaran` (y se replica en `cantidad_convertida` con
  `factor_conversion = 1.0`, porque las sintéticas no tienen
  conversión de unidad)
- `unidad_contrato`
- `precio_unitario_final` (con `precio_unitario_source = 'pdf_inference'`
  si no es null)
- `importe_calculado` (con `importe_source = 'calculated'`)

El resto de columnas (`modifier_source`, `parent_merge_line_id`,
`matched_contrato_line_id`, etc.) **NO se tocan** desde el formulario,
para preservar trazabilidad.

### Filtro de seguridad

El UPDATE filtra `line_kind = 'synthetic_modifier'` y
`valuation_id` perteneciente al `document_id` del payload. Un payload
malicioso no puede tocar filas `from_albaran` ni filas de otros
documentos.


## Flujo de guardado

1. Revisor edita filas (mezcla de `from_albaran` y `synthetic_modifier`)
   y pulsa Guardar / Aprobar.
2. `app.js` recorre el `<tbody>` y diferencia por `data-line-kind`:
   - `from_albaran` → `payload.lines[]` (formato `MergeLinePayload`
     como antes; compatibilidad total con el save existente).
   - `synthetic_modifier` → `payload.valuation_line_updates[]`
     (formato `ValuationLineUpdate` con el `valuation_line_id` leído
     de `data-valuation-line-id`).
3. `PUT /api/documents/{id}` recibe el payload. El repo:
   - Aplica los UPDATE/INSERT/DELETE de líneas del merge (flujo
     existente).
   - Aplica los UPDATE sobre `albaran_line_valuations` filtrando por
     `document_id` y `line_kind='synthetic_modifier'`.
   - Recalcula total de la cabecera vía
     `_recalc_valuation_importes` (tolerante a `merge_line_id=NULL`).


## Orden de despliegue

1. Backup de BBDD recomendado (buena práctica, aunque no hay DDL).
2. Sustituye los 4 ficheros.
3. Reinicia svc4.
4. Abre un albarán valorado con 2D (ej. un hormigón con sintéticas).
5. Verifica:
   - La tabla muestra todas las filas (base + sintéticas).
   - Los campos están rellenos con los valores valorados.
   - Editas la cantidad de una sintética y el importe que se
     recalcula al guardar es cantidad × precio unitario.


## Prueba end-to-end

```
1. Abrir /documents/<id> de un albarán con valoración + sintéticas.
2. Contar filas en la tabla: debe coincidir con
   SELECT COUNT(*) FROM albaran_line_valuations
   WHERE valuation_id = (SELECT id FROM albaran_valuations
                         WHERE document_id = '<id>')
3. Editar una sintética: cambia cantidad de 10 a 20.
4. Guardar.
5. Verificar en BBDD:
   SELECT id, line_kind, cantidad_albaran, importe_calculado
   FROM albaran_line_valuations
   WHERE valuation_id = (...)
   La fila editada debe tener cantidad_albaran=20 y importe=20*precio.
6. Verificar que total_valorado en albaran_valuations se actualizó.
```


## Limitaciones conocidas

1. **No se pueden añadir sintéticas manualmente**: el botón "Añadir
   línea" solo crea filas `from_albaran`. Si quieres añadir un
   incremento no detectado, tienes que re-valorar.
2. **No se pueden borrar sintéticas individualmente** desde la UI.
   Solo desaparecen al re-valorar.
3. **Re-valorar sobrescribe las ediciones manuales** sobre sintéticas
   (no hay campo `was_edited_by_reviewer` todavía). Si el revisor
   editó una sintética y después se re-valora el albarán, las
   sintéticas se regeneran desde cero.

Las tres son mejorables en iteraciones futuras si aparece la
necesidad.


## Rollback

Revierte los 4 ficheros (git). No hay DDL que deshacer. Las filas
sintéticas que el revisor hubiera editado mantendrán sus ediciones
en BBDD, pero la UI volverá a mostrarlas solo parcialmente (como
en el fix v1: en `valuation.synthetic_lines` pero sin pintarlas).

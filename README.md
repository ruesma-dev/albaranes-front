# README.md
# Servicio 4 - Revisión web de albaranes

Servicio web para revisar y aprobar el resultado final guardado en la tabla
`albaran_documents_merge` y sus líneas `albaran_lines_merge`.

## Funcionalidades

- Lista filtrable y ordenable de documentos pendientes/aprobados.
- Aprobación rápida desde la lista.
- Pantalla de detalle con visor del documento.
- Edición de cabecera y líneas.
- Guardar y aprobar el documento.
- Trazabilidad por proveedor leyendo `albaran_documents`.
- Añade automáticamente columnas de revisión en la tabla merge:
  - `approved`
  - `approved_at_utc`
  - `approved_by`
  - `reviewed_at_utc`
  - `last_modified_at_utc`
  - `review_notes`

## Vista previa del documento

La vista previa ya no intenta embeber la página web de SharePoint dentro de un
`iframe`, porque eso suele ser bloqueado por SharePoint/navegador.

En su lugar, el servicio lee el archivo binario desde SharePoint usando
Microsoft Graph y sirve una ruta local:

- `/documents/{id}/preview`

Esto requiere:

- `GRAPH_KEY`
- `SHAREPOINT_DRIVE_ID`

Si faltan, la web sigue funcionando y deja el botón `Abrir en SharePoint`, pero
la vista previa embebida mostrará un mensaje informativo.

## Arranque en local

```powershell
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python main.py
```

La web quedará en:

```text
http://127.0.0.1:8002/documents
```

## Variables importantes

- `PG_*`: conexión a PostgreSQL.
- `AUTO_CREATE_DATABASE=true`: crea la BBDD si no existe.
- `DEFAULT_REVIEWER`: nombre por defecto para aprobaciones rápidas.
- `GRAPH_KEY`: credenciales/token para Microsoft Graph.
- `SHAREPOINT_DRIVE_ID`: drive donde está almacenado el PDF/JPG.
- `GRAPH_TIMEOUT_S`: timeout de llamadas Graph para la vista previa.

## Notas

- El servicio intenta renombrar tablas antiguas `_gem` a `_merge` si aún existen.
- La ruta `document_storage_ref` se usa para localizar el archivo exacto dentro
  del drive de SharePoint.
- Si el archivo no puede resolverse o descargarse, el `iframe` muestra un mensaje
  legible con opción de abrir el documento en SharePoint.

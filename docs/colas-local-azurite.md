# Ejecutar el sv4 con colas en local (Azurite)

El portal (sv4) tiene dos modos:

- **solo-front** (sin `COLAS_*`): para iterar UI. El publicador es NULO,
  así que los flujos que encolan (re-fetch de contratos, valoración,
  feedback de aprobación) **no salen del sv4**. Con el *fallback local*
  (jun 2026) el **re-fetch sí funciona** (Sigrid directo → persiste
  contratos+líneas), pero **la valoración NO** (el orquestador va por
  cola).
- **con colas** (`COLAS_*` definida): flujo completo. El re-fetch y la
  valoración se encolan y los consumen los workers (sv3, sv7/sv6/sv5).

Para probar **selección de contrato + valoración de punta a punta** hay
que levantar las colas (Azurite) y los workers.

---

## 1. Instalar y arrancar Azurite

Azurite es el emulador local de Azure Storage (incluye Queues + Blobs).

### Opción A — npm (recomendada en Windows/PyCharm)

```powershell
npm install -g azurite
# Carpeta para los datos del emulador (la que quieras)
mkdir C:\azurite 2>$null
azurite --silent --location C:\azurite --debug C:\azurite\debug.log
```

Deja esa consola abierta. Por defecto expone:

- Blob   `http://127.0.0.1:10000`
- Queue  `http://127.0.0.1:10001`
- Table  `http://127.0.0.1:10002`

### Opción B — Docker

```powershell
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
```

### Opción C — extensión de VS Code

Instala "Azurite" en VS Code y pulsa *Azurite: Start* (mismos puertos).

---

## 2. Definir `COLAS_CONNECTION_STRING`

Azurite usa una **cadena de conexión de desarrollo bien conocida** (la
misma clave dummy para todos los emuladores):

```
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1
```

En la consola PowerShell donde arrancas el sv4 (antes de lanzarlo):

```powershell
$env:COLAS_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"
```

> En `main.py` se lee `COLAS_CONNECTION_STRING` (local) o
> `COLAS_ACCOUNT_URL` (nube). Con la variable definida, el publicador
> deja de ser NULO y el log de arranque ya **no** dice "modo solo-front";
> el *fallback local* de re-fetch se desactiva (vuelve el de cola → sv3).

En PyCharm: *Run/Debug Configurations → Environment variables* del
runner de `main.py`, añade `COLAS_CONNECTION_STRING` con ese valor.

---

## 3. Crear las colas

Los workers/SDK suelen crearlas si no existen, pero si quieres dejarlas
listas (necesitas Azure CLI):

```powershell
$cs = $env:COLAS_CONNECTION_STRING
foreach ($q in @("q-persistencia","q-valoracion","q-feedback")) {
    az storage queue create --name $q --connection-string $cs | Out-Null
}
az storage queue list --connection-string $cs -o table
```

Colas que usa el flujo:

| Cola             | Quién publica | Quién consume        | Para qué                                  |
|------------------|---------------|----------------------|-------------------------------------------|
| `q-persistencia` | sv4 (re-fetch)| **sv3**              | Re-enriquecer contratos (force_refetch)   |
| `q-valoracion`   | sv4 (/valuate)| **sv7 → sv6 → sv5**  | Lanzar la valoración contra el contrato   |
| `q-feedback`     | sv4 (aprobar) | sv-feedback          | Feedback de revisión aprobada             |

---

## 4. Arrancar los workers

Para el flujo completo, además del sv4 necesitas, **con la MISMA**
`COLAS_CONNECTION_STRING` y contra **el mismo Postgres de dev**:

- **sv3** (albaranes-persistencia): consume `q-persistencia`, cachea
  contratos + líneas (y PDF) → habilita la **selección** del contrato.
- **sv7 / sv6 / sv5** (orquestador + valoración): consumen
  `q-valoracion` → generan la valoración → el sv4 sondea y refresca.

Cada uno se arranca como su propio proceso (su `main.py` / `func start`
según el servicio), exportando antes la misma cadena de conexión.

---

## 5. Probar

1. Azurite arrancado (paso 1).
2. `COLAS_CONNECTION_STRING` definida en las consolas de sv4 **y** de
   cada worker (paso 2).
3. Colas creadas (paso 3, opcional).
4. sv4 + sv3 + sv7/sv6/sv5 arrancados (paso 4).
5. En el portal: elige el contrato → guarda → re-fetch (sv3 lo cachea) →
   la selección persiste → `/valuate` encola → la valoración corre y la
   página se refresca al persistir.

---

## Resumen rápido

| Quiero…                                    | Necesito                                  |
|--------------------------------------------|-------------------------------------------|
| Iterar UI                                  | solo-front (sin nada más)                 |
| Asociar contrato encontrado en vivo        | solo-front (fallback local ya lo cachea)  |
| Lanzar valoración de verdad                | Azurite + `COLAS_*` + sv3 + sv7/sv6/sv5   |

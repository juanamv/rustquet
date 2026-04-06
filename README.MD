# rustquet

Microservicio de ingesta de telemetria en Rust.

Recibe eventos por HTTP, los persiste en RocksDB de forma inmediata y los convierte a archivos Parquet en batches estrictos por cantidad.

## Indice

1. [Resumen](#resumen)
2. [Ejecucion](#ejecucion)
3. [API HTTP](#api-http)
4. [Schema de configuracion](#schema-de-configuracion)
5. [Columnas Parquet](#columnas-parquet)
6. [Versionado de schema](#versionado-de-schema)
7. [Recuperacion ante reinicios](#recuperacion-ante-reinicios)
8. [Archivos generados](#archivos-generados)
9. [Comandos](#comandos)
10. [Limpieza de datos](#limpieza-de-datos)

## Resumen

- `POST /ingest` — recibe eventos JSON
- persistencia inmediata en RocksDB antes de responder
- conversion a Parquet en `parquet_output/` cuando un batch alcanza su tamanho exacto
- salida particionada por rango de fechas del batch
- manifest JSON por batch en `manifests/`
- flujo `push` opcional definido en `config.json` para buckets compatibles con S3
- recuperacion automatica de batches pendientes tras reinicios
- columnas indexadas configurables via `config.json`
- versionado de schema con reglas append-only

## Ejecucion

Requiere Rust 1.93.1 o superior.

Configuracion runtime por defecto:

- `SERVER_ADDR=127.0.0.1:3000`
- `DB_PATH=rustquet_data`
- `PARQUET_OUTPUT_DIR=parquet_output`
- `BATCH_SIZE=10000`
- `INGEST_CHANNEL_CAPACITY=8192`
- `PARQUET_CHANNEL_CAPACITY=1`
- `SCHEMA_CONFIG_PATH=config.json`

Schema por defecto (`config.json`):

```bash
cargo run --release
```

Schema explicito via argumento:

```bash
cargo run --release -- config_v2.json
```

Tambien puedes configurar runtime con `.env`:

```env
SERVER_ADDR=127.0.0.1:3000
DB_PATH=rustquet_data
PARQUET_OUTPUT_DIR=parquet_output
BATCH_SIZE=10000
INGEST_CHANNEL_CAPACITY=8192
PARQUET_CHANNEL_CAPACITY=1
SCHEMA_CONFIG_PATH=config.json

# valores resueltos por un destino push definido en config.json
LAKE_BUCKET=telemetry-bucket
LAKE_PREFIX=rustquet/prod
LAKE_ENDPOINT=http://127.0.0.1:9000
LAKE_REGION=us-east-1
LAKE_ACCESS_KEY_ID=...
LAKE_SECRET_ACCESS_KEY=...
LAKE_ALLOW_HTTP=true
```

`rustquet` usa `object_store` para el upload remoto. Los valores concretos de bucket, endpoint, region y credenciales se leen desde variables de entorno cuyos nombres se declaran en `config.json`.

Precedencia para el schema:

- argumento CLI
- `SCHEMA_CONFIG_PATH` en entorno o `.env`
- `config.json`

El servidor escucha en `127.0.0.1:3000`.

## API HTTP

### POST /ingest

Recibe un evento JSON.

**Request:**

```json
{
  "id": "abc123",
  "path": "/pagina/productos",
  "event_name": "click_boton",
  "metadata": {
    "h2o": true,
    "slot": "hero",
    "region": "us-east"
  }
}
```

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `id` | `string` | identificador unico del evento |
| `path` | `string` | ruta asociada al evento |
| `event_name` | `string` | nombre del evento |
| `metadata` | `object` | datos adicionales, campos arbitarios |
| `timestamp` | `string` | generado por el servidor si se omite (RFC 3339) |

**Respuesta:** `201 Created`

**Ejemplo con curl:**

```bash
curl -X POST http://127.0.0.1:3000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "id": "abc123",
    "path": "/pagina/productos",
    "event_name": "click_boton",
    "metadata": {
      "h2o": true,
      "slot": "hero",
      "region": "us-east"
    }
  }'
```

## Schema de configuracion

El archivo `config.json` define tres secciones:

- `schema`: version y columnas indexadas a extraer de `metadata`
- `dataset`: descripcion del dataset generado por `rustquet`
- `push`: lista ordenada de destinos remotos opcionales

Por ahora, `dataset` describe propiedades estables del dataset que `rustquet` produce. El particionado no se configura desde el archivo: el sistema lo decide internamente para mantener un layout consistente y robusto.

**Formatos disponibles en el repositorio:**

| Archivo | Version | Descripcion |
|---------|---------|-------------|
| `config.json` | 1 | columnas `h2o` (bool) y `slot` (string) |
| `config_v2.json` | 2 | agrega `region` (string) |
| `config_v2_overwrite.json` | 2 | invalido: sobrescribe tipo de `h2o` |
| `config_v3_nested.json` | 3 | agrega `country` via path anidado |

**Estructura del archivo:**

```json
{
  "dataset": {
    "write_manifest": true
  },
  "push": [
    {
      "name": "lake_primary",
      "kind": "s3_compatible",
      "env": {
        "bucket": "LAKE_BUCKET",
        "prefix": "LAKE_PREFIX",
        "endpoint": "LAKE_ENDPOINT",
        "region": "LAKE_REGION",
        "access_key_id": "LAKE_ACCESS_KEY_ID",
        "secret_access_key": "LAKE_SECRET_ACCESS_KEY",
        "allow_http": "LAKE_ALLOW_HTTP"
      }
    }
  ],
  "schema": {
    "schema_version": 1,
    "columns": [
      {
        "name": "h2o",
        "source": "metadata.h2o",
        "type": "boolean"
      },
      {
        "name": "slot",
        "source": "metadata.slot",
        "type": "string"
      }
    ]
  }
}
```

| Campo | Requisito | Descripcion |
|-------|-----------|-------------|
| `dataset.write_manifest` | obligatorio | indica si `rustquet` genera manifests por batch |
| `push` | opcional | lista ordenada de destinos remotos |
| `push[].name` | obligatorio | identificador unico y estable del destino |
| `push[].kind` | obligatorio | hoy: `s3_compatible` |
| `push[].artifacts` | opcional | `parquet`, `manifest` o ambos; si se omite, usa `parquet` y agrega `manifest` si `dataset.write_manifest=true` |
| `push[].env.*` | obligatorio/segun campo | nombres de variables de entorno; no guarda secretos ni valores reales |
| `push[].env.bucket` | obligatorio | variable que contiene el bucket |
| `push[].env.prefix` | opcional | variable que contiene el prefijo remoto |
| `push[].env.endpoint` | obligatorio | variable que contiene el endpoint S3-compatible |
| `push[].env.region` | obligatorio | variable que contiene la region |
| `push[].env.access_key_id` | obligatorio | variable que contiene el access key id |
| `push[].env.secret_access_key` | obligatorio | variable que contiene el secret access key |
| `push[].env.session_token` | opcional | variable que contiene un session token |
| `push[].env.allow_http` | opcional | variable booleana `true` o `false` |
| `push[].env.virtual_hosted_style` | opcional | variable booleana `true` o `false` |
| `schema.schema_version` | obligatorio | numero entero, debe crecer con cada version |
| `schema.columns` | obligatorio | lista de columnas indexadas |
| `schema.columns[].name` | obligatorio | nombre de la columna en Parquet |
| `schema.columns[].source` | obligatorio | ruta dentro de `metadata`, debe empezar con `metadata.` |
| `schema.columns[].type` | obligatorio | `boolean`, `string` o `number` |

**Validacion de `push`:**

- `push[].name` debe ser unico
- `push[].artifacts` no puede repetir valores
- si `dataset.write_manifest=false`, no se permite subir `manifest`
- cada `push[].env.*` debe apuntar a un nombre de variable no vacio
- dentro de un mismo destino no se puede reutilizar la misma variable para campos distintos

**Reglas de versionado:**

- una nueva version solo puede agregar columnas; no puede modificar ni eliminar columnas existentes
- si una version existente cambia su definicion, el servidor rechaza el arranque
- no se permiten numeros de version menores a los ya persistidos

**Tipos indexables admitidos:**

- `boolean` -> columna Parquet `Boolean`
- `string` -> columna Parquet `Utf8`
- `number` -> columna Parquet `Float64`; acepta enteros y decimales JSON sin distinguirlos en el schema

**Particionado del dataset:**

- no se define en `config.json`
- `rustquet` lo resuelve internamente a partir del `timestamp` de cada evento
- los archivos se organizan automaticamente con layout canonico tipo Hive: `year=YYYY/month=MM/day=DD`
- si un batch cruza medianoche, `rustquet` escribe varios archivos, uno por dia, sin romper el batch logico por cantidad

## Columnas Parquet

Cada archivo Parquet contiene estas columnas fijas:

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| `id` | `string` | identificador del evento |
| `path` | `string` | ruta del evento |
| `event_name` | `string` | nombre del evento |
| `timestamp` | `string` | instante del evento (RFC 3339) |
| `metadata` | `string` | JSON crudo del campo `metadata` original |

Mas las columnas indexadas definidas en el schema activo.

**Manejo de errores:**

- si un campo de `metadata` no existe → la columna tipada es `NULL`
- si el tipo en `metadata` no coincide con el declarado → la columna tipada es `NULL`
- `metadata` se conserva siempre como JSON crudo sin perdida

## Versionado de schema

El sistema mantiene consistencia entre batches durante actualizaciones de schema.

- cada batch en curso persiste su `schema_version` en RocksDB
- al reiniciar, los batches pendientes se completan con el schema bajo el cual iniciaron
- los batches nuevos usan el schema cargado actualmente
- si no hay batch activo y llega un evento, se asigna la version actual del schema

**Validacion al arranque:**

- la version del archivo `config.json` se contrasta contra lo almacenado en RocksDB
- una redefinicion de columnas en la misma version genera un error de arranque
- una version menor a una ya existente genera un error de arranque

## Recuperacion ante reinicios

El servidor recupera el estado de RocksDB automaticamente:

1. lee `next_event_id` y `next_batch_start_id`
2. si existe un `active_batch`, verifica si los archivos `.parquet` esperados del batch ya existen
3. si los archivos existen y `dataset.write_manifest=true` y falta el manifest → regenera el manifest
4. si los archivos existen → limpia el batch de RocksDB sin reescribir Parquet
5. si falta alguno de los archivos → regenera el batch desde RocksDB
6. si existe un archivo `.tmp` obsoleto → se descarta y se genera uno nuevo

No se requiere intervencion manual.

## Archivos generados

Ubicacion principal: `parquet_output/`

Particionado automatico por fecha del evento:

```text
parquet_output/
  year=2026/
    month=04/
      day=05/
        batch_0000000001_20260405T140000Z_20260405T140459Z.parquet
      day=06/
        batch_0000000001_20260406T000000Z_20260406T000500Z.parquet
```

El nombre del archivo incluye:

- `batch_id` deterministico (`start_event_id`)
- `timestamp_min` del archivo en UTC
- `timestamp_max` del archivo en UTC

Un batch puede generar uno o varios archivos Parquet. Si todos los eventos caen en el mismo dia, genera un solo archivo. Si el batch cruza de dia, el writer divide la salida fisica por dia y mantiene el mismo `batch_id` en todos los archivos generados.

Si `dataset.write_manifest=true`, se genera un manifest por batch:

```
manifests/
  date_start=2026-04-05/
    date_end=2026-04-05/
      batch_0000000001_20260405T140000Z_20260405T140459Z.json
```

Cada manifest incluye:

- `batch_id`
- `schema_version`
- `files`: lista de archivos Parquet del batch
- cada entrada de `files` incluye `file_path`, `row_count`, `timestamp_min`, `timestamp_max`, `date`, `year`, `month` y `day`
- `row_count`
- `event_id_min` y `event_id_max`
- `timestamp_min` y `timestamp_max`
- `date_start` y `date_end`
- `created_at`

El servidor escribe primero a archivos `.tmp` y luego hace `rename` atomico para garantizar consistencia.

Si `push` contiene destinos remotos, despues de escribir los archivos locales `rustquet` sube los artifacts configurados para cada destino antes de limpiar el batch en RocksDB. Las keys remotas reutilizan el mismo layout relativo del dataset y pueden quedar precedidas por el prefijo resuelto desde el entorno.

## Comandos

```bash
# compilar en modo debug
cargo build

# compilar en modo release
cargo build --release

# formatear codigo
cargo fmt

# lint estricto
cargo clippy -- -D warnings

# ejecutar tests
cargo test

# ejecutar tests de integracion
cargo test --test '*'

# ejecutar benchmark de absorcion de ingesta
cargo bench --bench ingest_bench

# ejecutar benchmark end-to-end hasta parquet
cargo bench --bench end_to_end_bench

# verificar documentacion
cargo doc
```

## Limpieza de datos

```powershell
Remove-Item -Recurse -Force rustquet_data, parquet_output
```

```bash
rm -rf rustquet_data parquet_output
```

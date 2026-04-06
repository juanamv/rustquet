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
- recuperacion automatica de batches pendientes tras reinicios
- columnas indexadas configurables via `config.json`
- versionado de schema con reglas append-only

## Ejecucion

Requiere Rust 1.93.1 o superior.

Schema por defecto (`config.json`):

```bash
cargo run --release
```

Schema explicito via argumento:

```bash
cargo run --release -- config_v2.json
```

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

El archivo `config.json` define la version del schema y las columnas indexadas a extraer de `metadata`.

**Formatos disponibles en el repositorio:**

| Archivo | Version | Descripcion |
|---------|---------|-------------|
| `config.json` | 1 | columnas `h2o` (bool) y `slot` (string) |
| `config_v2.json` | 2 | agrega `region` (string) |
| `config_v2_overwrite.json` | 2 | invalido: sobrescribe tipo de `h2o` |
| `config_v3_nested.json` | 3 | agrega `country` via path anidado |

**Estructura del schema:**

```json
{
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
```

| Campo | Requisito | Descripcion |
|-------|-----------|-------------|
| `schema_version` | obligatorio | numero entero, debe crecer con cada version |
| `columns` | obligatorio | lista de columnas indexadas |
| `name` | obligatorio | nombre de la columna en Parquet |
| `source` | obligatorio | ruta dentro de `metadata`, debe empezar con `metadata.` |
| `type` | obligatorio | `boolean`, `string` o `number` |

**Reglas de versionado:**

- una nueva version solo puede agregar columnas; no puede modificar ni eliminar columnas existentes
- si una version existente cambia su definicion, el servidor rechaza el arranque
- no se permiten numeros de version menores a los ya persistidos

**Tipos indexables admitidos:**

- `boolean` -> columna Parquet `Boolean`
- `string` -> columna Parquet `Utf8`
- `number` -> columna Parquet `Float64`; acepta enteros y decimales JSON sin distinguirlos en el schema

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
2. si existe un `active_batch`, verifica si el archivo `.parquet` final ya existe
3. si el archivo `.parquet` existe → limpia el batch de RocksDB sin regenerar
4. si el archivo `.parquet` falta → regenera el batch desde RocksDB
5. si existe un archivo `.tmp` obsoleto → se descarta y se genera uno nuevo

No se requiere intervencion manual.

## Archivos generados

Ubicacion: `parquet_output/`

Nombrado deterministico:

```
batch_0000000001.parquet
```

El servidor escribe a un archivo `.tmp` y luego hace `rename` atomico para garantizar consistencia.

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

# rustquet

Microservicio de ingesta de telemetria en Rust.

Recibe eventos por HTTP, los persiste primero en RocksDB y luego los convierte a archivos Parquet en batches estrictos por cantidad.

## Que hace

- expone `POST /ingest`
- guarda cada evento en RocksDB antes de responder
- genera Parquet en `parquet_output/`
- recupera batches pendientes tras reinicios
- carga un esquema de columnas indexadas desde `config.json`
- evita sobreescrituras de schema entre versiones

## Requisitos

- Rust 1.93.1 o superior
- RocksDB se compila automaticamente con Cargo

## Ejecutar

Schema por defecto:

```bash
cargo run --release
```

Schema explicito por argumento:

```bash
cargo run --release -- config_v2.json
```

El servidor escucha en `127.0.0.1:3000`.

## Payload HTTP

El payload de ingreso no cambia aunque agregues columnas indexadas al schema.

Ejemplo:

```json
{
  "session_id": "abc123",
  "path": "/pagina/productos",
  "event_name": "click_boton",
  "metadata": {
    "h2o": true,
    "slot": "hero",
    "region": "us-east"
  }
}
```

Ejemplo con `curl`:

```bash
curl -X POST http://127.0.0.1:3000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc123",
    "path": "/pagina/productos",
    "event_name": "click_boton",
    "metadata": {
      "h2o": true,
      "slot": "hero",
      "region": "us-east"
    }
  }'
```

Respuesta esperada: `201 Created`.

## Schema JSON

El schema se define en archivos `.json` como este:

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

Reglas:

- `schema_version` debe subir cuando agregas una nueva version
- `source` debe empezar con `metadata.`
- el mismo nombre de columna no puede cambiar de tipo ni de origen
- los cambios permitidos son `append-only`
- si una version existente cambia su definicion, el arranque falla

Ejemplos incluidos en el repo:

- `config.json`: version 1
- `config_v2.json`: version 2, agrega `region`
- `config_v2_overwrite.json`: version 2 invalida, intenta sobrescribir `h2o`
- `config_v3_nested.json`: version 3 con path anidado `metadata.geo.country`

## Columnas Parquet

Cada archivo Parquet siempre incluye estas columnas fijas:

- `session_id`
- `path`
- `event_name`
- `timestamp`
- `metadata`

Ademas incluye las columnas indexadas definidas en el schema activo.

Con `config.json` se generan tambien:

- `h2o`
- `slot`

Con `config_v2.json` se agrega:

- `region`

Si un campo configurado no existe en `metadata` o tiene el tipo incorrecto, la columna tipada se escribe como `NULL` y `metadata` raw se conserva completo.

## Versionado y reinicios

- los batches nuevos usan una sola `schema_version` activa
- cada `active_batch` persiste su `schema_version` en RocksDB
- si reinicias con una version nueva, un batch viejo pendiente se termina con su schema viejo
- las versiones nuevas se validan contra RocksDB antes de arrancar

## Archivos generados

Los archivos Parquet se guardan en `parquet_output/` con el formato:

```text
batch_0000000001.parquet
```

La escritura usa archivo temporal `.tmp` y luego `rename` atomico.

## Comandos utiles

Compilar:

```bash
cargo build --release
```

Formatear:

```bash
cargo fmt
```

Lint estricto:

```bash
cargo clippy -- -D warnings
```

Tests:

```bash
cargo test
```

## Limpiar datos locales

PowerShell:

```powershell
Remove-Item -Recurse -Force rustquet_data, parquet_output
```

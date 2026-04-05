# rustquet

Microservicio de ingesta de telemetría en Rust.

Recibe eventos por HTTP, los guarda en RocksDB y los convierte a archivos Parquet en batches.

## Requisitos

- Rust 1.93.1 o superior
- RocksDB (se descarga automáticamente con cargo)

## Compilar y ejecutar

```bash
cargo build --release
cargo run --release
```

El servidor escucha en `127.0.0.1:3000`.

## Enviar eventos

```bash
curl -X POST http://127.0.0.1:3000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "abc123",
    "path": "/pagina/productos",
    "event_name": "click_boton"
  }'
```

Respuesta esperada: `201 Created`

## Archivos generados

Los archivos Parquet se guardan en `parquet_output/` con el formato:

```
telemetry_SESSIONID_BATCH.parquet
```

## Limpiar datos

Para eliminar todos los datos acumulados:

```bash
rm -rf rustquet_data parquet_output
```

# Project: jupyter-datafusion-iceberg

## Architecture

```
       Jupyter
          |
          v
DataFusion Python API
          |
          v
      DataFusion
          |
          v
Iceberg Table Provider
          |
          v
   Iceberg Catalog
          |
          v
  Iceberg Data Files
          |
          v
        Arrow
          |
          v
       Pandas
```

Docker Compose stack with these services:
- **Airflow** (webserver, scheduler, worker, triggerer) — orchestrates ETL via Spark
- **Spark** (master + worker) — writes data into Iceberg tables
- **MinIO** — S3-compatible object storage for Iceberg data files (Parquet)
- **Iceberg REST Catalog** (`tabulario/iceberg-rest`) — catalog service
- **Jupyter Notebook** — interactive querying via DataFusion native Iceberg Table Provider
- **Spark History Server** — Spark job monitoring
- **PostgreSQL** — Airflow metadata DB
- **Redis** — Celery broker for Airflow

## Key Files

- `docker-compose.yaml` — all services defined here
- `Dockerfile` (root) — custom Airflow image with Java + PySpark
- `jupyter/Dockerfile` — custom Jupyter image with `pyiceberg[s3fs,datafusion,pyiceberg-core]`
- `notebooks/sample_datafusion_iceberg.ipynb` — main notebook querying Iceberg via DataFusion
- `src/` — Airflow DAGs (mounted to `/opt/airflow/dags`)
- `spark/apps/` — Spark applications

## Conventions

### Jupyter Dockerfile
- Base image: `jupyter/scipy-notebook:latest`
- Do NOT add `apt-get` commands — GPG signature errors on ARM in cached layers
- Only `pip install` on top of base image
- Do NOT pin `datafusion` explicitly — `pyiceberg[datafusion]` controls the version (currently requires `datafusion>=51,<52`)
- The `pyiceberg-core` extra is required for the Rust bindings that power the Iceberg Table Provider

### DataFusion + Iceberg Table Provider Pattern
- PyIceberg connects to Iceberg REST catalog via `load_catalog()`
- Iceberg table is registered as a native DataFusion table provider via `register_table()`
- DataFusion reads Iceberg data files (Parquet) directly through the table provider — no manual Arrow scan
- Flow: `load_catalog() → load_table() → IcebergDataFusionTable → register_table() → ctx.sql() → .to_pandas()`
- Reference: https://datafusion.apache.org/python/user-guide/data-sources.html

### REST Catalog AuthManager Workaround
- REST catalog injects `LegacyOAuth2AuthManager` into `io.properties`
- `pyiceberg_core` (Rust) expects all values to be strings — crashes on AuthManager objects
- Workaround: filter `io.properties` to string-only values, construct `IcebergDataFusionTable` manually
- Tracked at: https://github.com/apache/iceberg-python/issues/2544
- Once fixed upstream, simplify to: `ctx.register_table("raw_orders", iceberg_table)`

### Key Difference from DuckDB Version
- DuckDB has native Iceberg extension (ATTACH + SQL directly)
- DataFusion uses native Iceberg Table Provider via `pyiceberg-core` Rust bindings
- DataFusion queries run on native Arrow — zero-copy to Pandas

## Credentials (Local Dev Only)

- MinIO: `minioadmin` / `minioadmin`
- Airflow: `airflow` / `airflow`
- Jupyter: no auth (token disabled)

## Ports

| Service | Port |
|---------|------|
| Airflow UI | 8080 |
| Jupyter | 8888 |
| Spark Master UI | 9090 |
| Spark History | 18080 |
| MinIO API | 9000 |
| MinIO Console | 9001 |
| Iceberg REST | 8181 |

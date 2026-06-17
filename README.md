# Jupyter DataFusion Iceberg Demo Project

This project allows user to load sample data into Iceberg via a DAG. Use a Jupyter Notebook with DataFusion to query Iceberg tables natively via the Iceberg Table Provider.

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

## Execution Flow

```
Jupyter Notebook
       |
       v
User submits SQL query via DataFusion Python API
       |
       v
DataFusion delegates to Iceberg Table Provider
       |
       v
Table Provider reads Iceberg metadata from REST catalog
       |
       v
Table Provider identifies required Parquet data files
       |
       v
DataFusion reads Parquet files into Arrow batches
       |
       v
DataFusion executes query on Arrow data
       |
       v
Zero-copy conversion to Pandas DataFrame
       |
       v
Data Scientist performs analysis
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Git

## Getting Started

```bash
# Clone the repository
git clone <repo-url>
cd jupyter-datafusion-iceberg

# Start all services
./start.sh
```

The startup script will:
1. Build a custom Airflow image with pre-installed dependencies
2. Initialize the Airflow database and create an admin user
3. Start all services (Airflow, Spark, MinIO, Iceberg, Jupyter)

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:9090 | - |
| Spark History Server | http://localhost:18080 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Jupyter Lab | http://localhost:8888 | no auth (token disabled) |

## Project Structure

```
jupyter-datafusion-iceberg/
├── src/                          # DAG files (mounted as /opt/airflow/dags)
│   └── Insert_Customer_Order_Data_To_Iceberg.py
├── spark/
│   └── apps/                     # PySpark scripts
│       ├── generate_sample_data.py
│       └── verify_data.py
├── jupyter/
│   └── Dockerfile                # Custom Jupyter image with DataFusion + PyIceberg
├── notebooks/
│   └── sample_datafusion_iceberg.ipynb  # Interactive DataFusion queries on Iceberg tables
├── docker-compose.yaml           # Full stack definition
├── Dockerfile                    # Custom Airflow image with Java + Python deps
├── start.sh                      # Startup script with graceful shutdown
├── CLAUDE.md                     # Project context for Claude Code
└── README.md
```

## How to run the demo
1. Access http://localhost:8080 via a browser. Enable the Insert_Customer_Order_Data_To_Iceberg DAG and run it. This will load sample data into Iceberg
2. Access http://localhost:8888 via a browser. Open the sample_datafusion_iceberg.ipynb in the Jupyter Notebook. Select Kernel -> Run all cells

## DAG
### a. Insert_Customer_Order_Data_To_Iceberg
Seeds 100 synthetic customer orders into an Iceberg table using PySpark.

**Pipeline:**
```
create_namespace → generate_sample_data → verify_data
```

- Creates the `ecommerce` namespace in Iceberg via REST catalog
- Generates 100 random orders with products, quantities, prices, and dates
- Writes data to `iceberg.ecommerce.raw_orders`
- Verifies the insert by reading back and printing row count and sample data

**Sample data schema (`raw_orders`):**

| Field | Type | Example |
|-------|------|---------|
| order_id | string | ORD-00001 |
| customer_id | string | CUST-0042 |
| product_name | string | Wireless Mouse |
| quantity | integer | 3 |
| price | decimal(10,2) | 29.99 |
| order_date | date | 2024-06-01 |
| status | string | completed |

## Jupyter Notebook — Querying Iceberg with DataFusion

The `notebooks/sample_datafusion_iceberg.ipynb` notebook demonstrates querying Iceberg tables using DataFusion with the native Iceberg Table Provider — no manual Arrow scanning needed.

**Architecture:**
```
Jupyter → DataFusion Python API → DataFusion → Iceberg Table Provider → Iceberg Data Files → Arrow → Pandas
```

**What the notebook covers:**
1. Connect to Iceberg REST catalog via PyIceberg
2. Register Iceberg table as a native DataFusion table provider
3. Query `raw_orders` table → Pandas DataFrame
4. Aggregation — revenue by product
5. Time-series — orders by month
6. Visualization — monthly revenue bar chart
7. Customer analysis — top customers by spend
8. Filtered query — completed orders from June 2024+
9. Performance summary — timing for each step

**Why DataFusion instead of PySpark for queries?**

| | PySpark | DataFusion + Iceberg Table Provider |
|---|---|---|
| Runtime | JVM + cluster | In-process, no JVM |
| Dependencies | pyspark + pyiceberg + pyarrow + Java | `pyiceberg[datafusion,pyiceberg-core]` |
| Startup | Slow (SparkSession init) | Instant |
| Iceberg access | SparkSQL built-in catalog | Native table provider via `register_table()` |
| Data format | Custom Spark rows | Native Apache Arrow |
| To get DataFrame | `spark.sql(...).toPandas()` | `.to_pandas()` — zero-copy from Arrow |
| Best for | Large-scale ETL, TB-scale writes | Interactive queries, GB-scale reads |

> **Note:** Run `Insert_Customer_Order_Data_To_Iceberg` DAG first to populate sample data.

## Tech Stack

- **Apache Airflow 2.10.5** — Workflow orchestration
- **Apache Spark 3.5.6** — Distributed data processing (writes to Iceberg)
- **Apache Iceberg** — Open table format for analytics
- **Apache DataFusion** — In-process SQL query engine with native Iceberg Table Provider
- **PyIceberg** — Python client for Iceberg REST catalog + DataFusion table provider bindings
- **Apache Arrow** — In-memory columnar format (zero-copy from DataFusion to Pandas)
- **MinIO** — S3-compatible object storage
- **PostgreSQL 16** — Airflow metadata database
- **Redis 7** — Celery message broker
- **Jupyter Lab** — Interactive notebook environment
- **Docker Compose** — Container orchestration

## Stopping Services

Press `Ctrl+C` if running via `./start.sh` (graceful shutdown), or:

```bash
docker compose down       # Stop all services
docker compose down -v    # Stop and remove all data volumes
```

# 🗄️ Source Database — Agentic Postgres (Ghost)

This folder contains everything needed to create and populate the **operational
source database** that feeds the pipeline: table DDL, the full Walmart sample
dataset (CSV), and a loader script.

The database used in this project is [Ghost](https://ghost.build) — a hosted,
**agentic Postgres**: ordinary Postgres from the SQL side, but you can also ask
its AI client questions in natural language ("what is the primary key of
orders?") instead of inspecting schemas by hand. Any Postgres 14+ works too.

## Contents

```
source-database/
├── ddl/
│   └── walmart_schema.sql   # CREATE TABLE statements for the 6 source tables
├── data/                    # sample dataset (CSV)
│   ├── customers.csv        #   2,000 rows
│   ├── employees.csv        #     250 rows
│   ├── order_items.csv      #  30,021 rows
│   ├── orders.csv           #  10,000 rows
│   ├── products.csv         #     500 rows
│   └── stores.csv           #     25 rows
├── load_data.py             # Option A: loads CSVs into Postgres via COPY
└── push_to_bronze.py        # Option B: seeds Databricks bronze directly from CSVs
```

## Two ways to get the data flowing

| | Option A — full source DB | Option B — direct bronze seed |
|---|---|---|
| How | Postgres/Ghost DB (below) + Databricks Lakeflow CDC ingest job | `python push_to_bronze.py` uploads the CSVs into a UC Volume and builds `walmart.bronze.*` tables directly |
| CDC semantics | ✅ Real change capture — what `ingest_cdc` orchestrates | ❌ Static landing (no source system) |
| Best for | The complete, production-style architecture | Fastest way to run the dbt + Airflow layers for learning |
| Requires | Ghost account or any Postgres + Databricks ingest job | Only a Databricks token with catalog permissions |

Both paths end in the same place: populated Delta tables under
`walmart.bronze`, ready for the dbt silver/gold layers.

## Schema overview

Six tables in a classic retail shape. Every table carries
`created_timestamp` / `updated_timestamp` audit columns — `updated_timestamp`
is the **CDC cursor** the Databricks ingest job uses — plus an `is_active`
flag.

```
customers ──< orders ──< order_items >── products
                │              │
              stores ─────< employees
```

| Table | Grain | Notable columns |
|---|---|---|
| `customers` | 1 per customer | name, email, geography |
| `stores` | 1 per store | name, geography |
| `products` | 1 per product | category, brand, `price` |
| `employees` | 1 per employee | `store_id` FK, job title, salary |
| `orders` | 1 per order | `customer_id`/`store_id` FKs, status, `total_amount` |
| `order_items` | 1 per order line | `order_id`/`product_id` FKs, qty, unit price, line amount |

## Setup — step by step

### 1. Create the database

Sign up at [ghost.build](https://ghost.build) (free tier is enough) and create
a database. You'll get a connection string like:

```
postgresql://<user>:<password>@<host>/<db>?sslmode=require
```

> 💡 Because Ghost is agentic, you can connect it to an MCP-capable AI client
> and explore/modify it conversationally. With plain Postgres, use `psql` or a
> GUI instead — everything below works identically.

### 2. Create the tables

Run the DDL against the database (adjust the file path):

```bash
psql "$WALMART_DB_CONNECTION_STRING" -f ddl/walmart_schema.sql
```

Or paste `ddl/walmart_schema.sql` into your SQL editor of choice. In Ghost's
agentic client you can also simply attach the file and ask it to create the
tables.

### 3. Load the sample data

The loader reads the connection string from the environment (**never hardcode
it**) and uses Postgres `COPY` for speed:

```bash
export WALMART_DB_CONNECTION_STRING="postgresql://..."
pip install psycopg2-binary
python load_data.py
```

Expected output:

```
Loading customers.csv into raw.customers...
✓ Successfully loaded customers.csv
...
✓ All data loaded successfully!
```

> **Note on schema name:** the loader targets `raw.<table>` (Ghost's default
> schema in the original build). If your tables live in `public` or another
> schema, edit the `csv_files` mapping at the top of `load_data.py` accordingly,
> or create a `raw` schema first: `CREATE SCHEMA raw;`

### 4. Verify

```sql
SELECT 'customers' t, count(*) FROM raw.customers
UNION ALL SELECT 'stores', count(*) FROM raw.stores
UNION ALL SELECT 'products', count(*) FROM raw.products
UNION ALL SELECT 'employees', count(*) FROM raw.employees
UNION ALL SELECT 'orders', count(*) FROM raw.orders
UNION ALL SELECT 'order_items', count(*) FROM raw.order_items;
```

Expected: 2000 / 25 / 500 / 250 / 10000 / 30021.

### 5. Connect to Databricks

The pipeline's CDC ingest job reads from this database. Follow
[../docs/setup-guide.md](../docs/setup-guide.md) (Step 2) to create the
Databricks Lakeflow ingestion job that pulls these tables into the
`walmart.bronze` catalog — after that, everything downstream is automated.

## Option B — skip the source DB entirely

Want to run the dbt + Airflow layers right now? `push_to_bronze.py` seeds
`walmart.bronze` straight from the CSVs in `data/`:

```bash
pip install databricks-sdk databricks-sql-connector
export DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com   # no https://
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx   # any SQL warehouse
python push_to_bronze.py                                # [--catalog walmart] [--schema bronze]
```

What it does: creates catalog/schema if missing, uploads the CSVs into a
`bronze.raw_files` Unity Catalog Volume, and runs `CREATE TABLE ... AS SELECT
* FROM read_files(...)` per table (schema inference). Credentials come only
from env vars. Afterwards, `dbt run` / the Airflow DAG work as documented —
just note the bronze tables are static (no CDC), which is fine for learning
the transformation side.

## Credits

This project follows the architecture of a real-world portfolio tutorial by
**Ansh Lamba** — [Build an End-to-End Data Engineering Project
(Walmart)](https://www.youtube.com/watch?v=ZEE-jNAthB0a) — which introduces
the Ghost agentic database, the Walmart dataset, and the Databricks + dbt +
Airflow stack used here. The DDL and sample CSVs in this folder originate
from that project's materials.

## Why this dataset ages

The CSVs are a static snapshot: after loading, `updated_timestamp` stops
advancing. The pipeline's [freshness SLA](../walmart_project/models/source.yml)
is therefore configured leniently by default (warn 7d / error 365d). To see CDC
live, `UPDATE` some rows in the source (e.g. flip an `order_status` and bump
`updated_timestamp`) and watch the next ingest pull exactly those changes.

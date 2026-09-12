"""
push_to_bronze.py — load the Walmart sample CSVs straight into Databricks bronze.

This is the NO-SOURCE-DB path: it recreates the bronze landing zone under the
`walmart` Unity Catalog directly from the CSVs in ./data, so the whole dbt +
Airflow side of the project can run without standing up a Postgres/Ghost source
database or a Lakeflow CDC ingest job.

WHICH PATH TO PICK?
    Option A (production-style): Postgres source + Lakeflow CDC ingest job.
        -> See README.md in this folder and docs/setup-guide.md.
        -> Real CDC semantics; what the Airflow `ingest_cdc` task orchestrates.
    Option B (this script): seed bronze directly from CSVs.
        -> Fastest way to get the dbt + Airflow layers running for learning.
        -> Note: with this path, run `dbt run`/`dbt test` manually or via the
           DAG after skipping `ingest_cdc` (the DAG expects the ingest job id).

HOW IT WORKS
    1. Ensures catalog `walmart` and schema `bronze` exist (created if missing).
    2. Ensures a Unity Catalog Volume `bronze.raw_files` exists.
    3. Uploads every CSV in ./data into the Volume.
    4. Creates/replicates each bronze table as
           CREATE OR REPLACE TABLE walmart.bronze.<name>
           AS SELECT * FROM read_files('<volume path>/<file>.csv', ...)
       using Databricks SQL with schema inference (auto-packed from the same
       DDL shapes as ddl/walmart_schema.sql).

USAGE
    export DATABRICKS_HOST=dbc-xxxx.cloud.databricks.com   # no https://
    export DATABRICKS_TOKEN=dapi...                        # PAT with catalog perms
    python push_to_bronze.py [--catalog walmart] [--schema bronze]

    Requires: pip install databricks-sdk databricks-sql-connector
    (credentials always come from env vars — nothing is hardcoded)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SchemaInfo, VolumeInfo, VolumeType

# CSV file -> bronze table name (file names match data/ contents exactly)
CSV_TABLES = {
    "customers.csv": "customers",
    "stores.csv": "stores",
    "products.csv": "products",
    "employees.csv": "employees",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
}

DATA_DIR = Path(__file__).parent / "data"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", default="walmart")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--volume", default="raw_files")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("ERROR: set DATABRICKS_HOST and DATABRICKS_TOKEN env vars first.")
        return 2
    if not DATA_DIR.exists():
        print(f"ERROR: data folder not found at {DATA_DIR}")
        return 2

    ws = WorkspaceClient(host=host, token=token)
    full_schema = f"{args.catalog}.{args.schema}"
    volume_full_name = f"{full_schema}.{args.volume}"

    # -- 1. catalog + schema ------------------------------------------------
    print(f"Ensuring catalog '{args.catalog}' exists...")
    try:
        ws.catalogs.get(args.catalog)
    except Exception:
        ws.catalogs.create(name=args.catalog)
        print(f"  created catalog {args.catalog}")

    print(f"Ensuring schema '{full_schema}' exists...")
    try:
        ws.schemas.get(full_schema)
    except Exception:
        ws.schemas.create(SchemaInfo(catalog_name=args.catalog, name=args.schema))
        print(f"  created schema {full_schema}")

    # -- 2. volume ----------------------------------------------------------
    print(f"Ensuring volume '{volume_full_name}' exists...")
    try:
        ws.volumes.get(volume_full_name)
    except Exception:
        ws.volumes.create(
            VolumeInfo(
                catalog_name=args.catalog,
                schema_name=args.schema,
                name=args.volume,
                volume_type=VolumeType.MANAGED,
            )
        )
        print(f"  created volume {volume_full_name}")

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"

    # -- 3. upload CSVs -----------------------------------------------------
    for csv_file in CSV_TABLES:
        local = DATA_DIR / csv_file
        remote = f"{volume_path}/{csv_file}"
        print(f"Uploading {csv_file} -> {remote} ({local.stat().st_size:,} bytes)...")
        with open(local, "rb") as f:
            ws.files.upload(remote, f, overwrite=True)

    # -- 4. build bronze tables via SQL -------------------------------------
    from databricks import sql as dbsql  # databricks-sql-connector

    print("\nCreating bronze tables from uploaded files...")
    with dbsql.connection(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=token,
        catalog=args.catalog,
    ) as conn:
        for csv_file, table in CSV_TABLES.items():
            qualified = f"{full_schema}.{table}"
            print(f"  CREATE OR REPLACE TABLE {qualified} ...")
            conn.cursor().execute(
                f"""
                CREATE OR REPLACE TABLE {qualified} AS
                SELECT * FROM read_files(
                    '{volume_path}/{csv_file}',
                    format => 'csv',
                    header => true,
                    inferSchema => true,
                    timestampFormat => 'yyyy-MM-dd HH:mm:ss'
                )
                """
            )

    print("\nDone. Bronze tables seeded under "
          f"{args.catalog}.{args.schema} — run `dbt run` to build silver/gold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

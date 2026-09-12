# 🌀 Airflow Guide — walmart-airflow

How the orchestration platform is built and how to operate it.

## Components

```
walmart-airflow/
├── dags/orchestrate.py     # the pipeline DAG (TaskFlow API)
├── docker-compose.yaml     # full CeleryExecutor stack
├── Dockerfile              # apache/airflow:3.3.1 + dbt + databricks-sdk
├── requirements.txt        # image dependencies (dbt-core, dbt-databricks, databricks-sdk)
└── .env.example            # template for .env (secrets; git-ignored)
```

`docker compose build` extends the official Airflow image with **dbt** and the
**Databricks SDK** — that's why the DAG can simply shell out to `dbt` CLI commands.
The dbt project at repo root is **bind-mounted** into every container at
`/opt/airflow/walmart_project` (see the volumes block in `docker-compose.yaml`).

## The DAG: `walmart_medallion_pipeline`

| Property | Value | Why |
|---|---|---|
| Schedule | `0 11 * * *` daily | Adjust to your SLA |
| Catchup | `False` | Skip missed days rather than backfill storm |
| Max active runs | `1` | Prevent overlapping runs writing the same tables |
| Retries | `1` (5 min delay) | Transient warehouse/network hiccups |
| Owner/tag | `data-engineering`, `walmart dbt databricks medallion` | Ownership + filtering |

### Task graph

```
ingest_cdc → clean_workspace → source_freshness → silver_technical_run
→ silver_technical_tests → silver_business_run → silver_business_tests
→ gold_ephemeral → gold_dimensions_scd2 → gold_facts_run
```

- **`ingest_cdc`** (Python task) — triggers the Databricks CDC job via
  `databricks-sdk`, then **polls until it terminates** (a naive trigger returns
  immediately and would let dbt race the ingest). It enforces a hard timeout
  (`CDC_POLL_TIMEOUT_SECONDS`, default 3600s) so a stuck run fails the task instead
  of hanging a worker slot forever.
- **`clean_workspace`** — removes stale `target/` and `logs/` from the dbt project
  so each run compiles from a clean manifest.
- **`source_freshness`** — `dbt source freshness`; fails before transforms run if
  bronze is stale.
- **silver/gold tasks** — one `@task.bash` per stage, each a `dbt` CLI invocation
  against the bind-mounted project. One task per stage gives per-stage retries,
  clear logs, and a readable Graph view.

### Configuration surface (all via env vars — see `.env.example`)

| Variable | Used by | Purpose |
|---|---|---|
| `DATABRICKS_HOST` | DAG + profiles.yml | Workspace host (no scheme) |
| `DATABRICKS_TOKEN` | DAG + profiles.yml | PAT / OAuth token |
| `DATABRICKS_HTTP_PATH` | profiles.yml | SQL warehouse for dbt |
| `DATABRICKS_CDC_JOB_ID` | DAG | Job to trigger for ingestion |
| `CDC_POLL_INTERVAL_SECONDS` | DAG | Poll cadence (default 15s) |
| `CDC_POLL_TIMEOUT_SECONDS` | DAG | Hard failure threshold (default 3600s) |
| `SOURCE_FRESHNESS_WARN_DAYS` | dbt (via profiles/project) | Freshness warn SLA in days (default 7) |
| `SOURCE_FRESHNESS_ERROR_DAYS` | dbt | Freshness error SLA in days (default 365 for the static demo dataset) |

## Operating the stack

```bash
cd walmart-airflow
docker compose up -d          # start everything
docker compose ps             # health of apiserver/scheduler/worker/triggerer
docker compose logs -f scheduler
docker compose down           # stop (keeps the postgres volume)
docker compose build && docker compose up -d   # after changing requirements.txt
```

Useful inside-container commands:

```bash
docker compose exec airflow-worker bash
cd /opt/airflow/walmart_project
dbt run --select silver_t      # debug a specific stage with real env vars
```

## Testing DAG changes

Before committing, sanity-check the DAG file compiles and imports cleanly:

```bash
cd walmart-airflow/dags
python -m py_compile orchestrate.py
```

(The full import requires the Airflow environment — easiest check is to open the
Airflow UI after `docker compose up` and confirm the DAG has no import errors.)

## Local development flow

1. Change dbt SQL in `walmart_project/models/...` — it is live inside containers
   via the bind mount, no rebuild needed.
2. Re-run just the affected stage from the Airflow UI (clear the task, then let it
   run downstream), or use the container exec command above.
3. Change DAG code — the dag-processor reloads automatically; no restart needed.
4. Change Python dependencies — edit `requirements.txt`, then rebuild the image.

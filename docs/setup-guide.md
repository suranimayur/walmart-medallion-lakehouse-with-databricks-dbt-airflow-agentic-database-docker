# 🚀 Setup Guide — Zero to Running Pipeline

This guide takes you from nothing to a fully orchestrated end-to-end run: source
database → Databricks → dbt → Airflow. Estimated time: **60–90 minutes** (plus
Databricks workspace provisioning if you don't have one).

## Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| Python | 3.11 or 3.12 | dbt CLI locally, env tooling |
| Docker Desktop | recent | Airflow platform |
| Databricks workspace | any (Free Edition works) | Lakehouse + SQL warehouse |
| Git | any | version control |
| A source database | — | We use a Ghost (agentic Postgres) database; any Postgres works |

## Step 1 — Create the source database and tables

The project expects six source tables: `customers`, `orders`, `order_items`,
`products`, `employees`, `stores`.

1. Create a [ghost.build](https://ghost.build) account (free tier is fine) — it's a
   hosted, agentic Postgres you can also query from an AI client via MCP.
2. Create the tables (any DDL works; see the Walmart dataset DDL from the original
   project material). Load the sample CSVs supplied with the dataset.
3. Note the connection string — the Databricks ingestion will need host/port/user/
   password/database.

> 💡 Because Ghost is agentic, you can literally ask its chat client things like
> "what is the primary key of orders?" instead of inspecting schemas by hand.

## Step 2 — Set up the Databricks side

1. **Catalog + schemas**: in Catalog Explorer create a catalog `walmart` with schemas
   `bronze`, `silver_t`, `silver_b`, `gold` (adjust `custom_schema.sql` if you rename).
2. **SQL warehouse**: start a Serverless/Pro SQL warehouse. From its *Connection
   details* tab note:
   - Server hostname → `DATABRICKS_HOST` (no `https://`)
   - HTTP path → `DATABRICKS_HTTP_PATH`
3. **Access token**: Settings → Developer → Access tokens → generate a PAT with a
   lifetime (e.g. 90 days). This is `DATABRICKS_TOKEN`.
4. **CDC ingest job**: create a Lakeflow (Delta Live/declarative) ingestion pipeline
   that pulls the six Postgres tables into `walmart.bronze` using change-data-capture.
   Note its **Job ID** → `DATABRICKS_CDC_JOB_ID`.

## Step 3 — Configure environment variables

```bash
cd walmart-airflow
cp .env.example .env
# then edit .env and fill in:
#   DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH,
#   DATABRICKS_CDC_JOB_ID, FERNET_KEY, AIRFLOW__API_AUTH__JWT_SECRET,
#   _AIRFLOW_WWW_USER_PASSWORD
```

Generate the Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

> 🔒 `.env` is git-ignored. Never commit it. See [security.md](security.md).

For **local dbt development** (running dbt on your host, outside Docker), the
simplest option: **copy the `.env` into the dbt project folder** — dbt auto-loads
`.env` from the working directory (it is git-ignored, so the secret stays local):

```powershell
cp walmart-airflow\.env walmart_project\.env
cd walmart_project
dbt debug               # should end with "All checks passed!"
```

Alternatives:

```powershell
# PowerShell session loader (no copy needed):
cd walmart-airflow
. .\load-env.ps1        # note the leading dot - loads .env into your session
cd ..\walmart_project
dbt debug
```

> ⚠️ On Windows, avoid Git Bash `export $(grep ... .env | xargs)`: it silently
> path-converts values starting with `/` (e.g. the warehouse `http_path`) into
> `C:/Program Files/Git/...` before dbt sees them, breaking the connection.

## Step 4 — Build the Airflow image and start the stack

```bash
cd ../walmart-airflow
docker compose build          # builds the custom image with dbt + databricks-sdk
docker compose up -d
docker compose ps             # wait until apiserver/scheduler/worker are healthy
```

Airflow UI: http://localhost:8080 (login with `_AIRFLOW_WWW_USER_USERNAME/_PASSWORD`
from `.env`; default `airflow`/`change-me`).

## Step 5 — First run

1. In the Airflow UI, unpause **`walmart_medallion_pipeline`**
   (DAGs are paused at creation by design).
2. Trigger it (▶️). Watch the Graph view:

   ```
   ingest_cdc → clean_workspace → source_freshness → silver_technical_run
   → silver_technical_tests → silver_business_run → silver_business_tests
   → gold_ephemeral → gold_dimensions_scd2 → gold_facts_run
   ```

3. When it's green, verify in Databricks Catalog Explorer:

   ```sql
   SELECT count(*) FROM walmart.silver_b.obt_b;
   SELECT * FROM walmart.gold.fact_orders LIMIT 10;
   SELECT * FROM walmart.gold.dim_customers LIMIT 10;
   ```

## Step 6 — Schedule

The DAG ships with `schedule="0 11 * * *"` (daily 11:00 UTC). Change it in
`walmart-airflow/dags/orchestrate.py` if your SLA differs. `catchup=False` means
missed days are skipped, and `max_active_runs=1` prevents overlapping runs.

## Troubleshooting first run

| Symptom | Fix |
|---|---|
| `dbt debug` fails with auth error | Token wrong/expired, or host includes `https://` (it must not) |
| DAG import error in UI | Check `walmart-airflow/logs/scheduler` and that `.env` exists with all keys |
| `ingest_cdc` fails immediately | Wrong `DATABRICKS_CDC_JOB_ID` or the PAT lacks job-run permission |
| `source_freshness` fails | Bronze data is stale — run the ingest manually first; check `source.yml` freshness thresholds |
| dbt tasks fail with "profile not found" | The bind mount moved — check `docker-compose.yaml` mounts `../walmart_project` |

Next: understand what you just built in [dbt-guide.md](dbt-guide.md) and
[airflow-guide.md](airflow-guide.md).

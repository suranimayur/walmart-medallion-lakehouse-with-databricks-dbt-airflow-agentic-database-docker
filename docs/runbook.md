# 🛡️ Operations Runbook

Day-2 operations: monitoring, common failures, and reprocessing recipes.

## Daily operation

- The DAG runs daily at 11:00 UTC. Check the Airflow UI (or enable email/Slack
  alerts on `dag_run` failure) each morning.
- Green DAG = bronze refreshed, silver tested, gold star schema current.

## Monitoring checklist

| What | Where | Healthy when |
|---|---|---|
| DAG runs | Airflow UI → `walmart_medallion_pipeline` | Last run `success`; no long-queued tasks |
| Worker health | `docker compose ps` | scheduler/worker/triggerer/apiserver all healthy |
| dbt run results | task logs → `dbt run` output | `DONE. 1 PASS`, no `WARN` on error-severity tests |
| Source freshness | `source_freshness` task | Passes; check Databricks query history for slow sources |
| Warehouse cost | Databricks SQL warehouse monitor | Runs stay within expected compute time |

## Common failures → fixes

### 1. `ingest_cdc` fails: `result_state=FAILED`
- Open the Databricks job run from the error message; the ingest pipeline logs
  show whether it's connectivity to the source DB or schema drift.
- After fixing, **clear only `ingest_cdc`** and downstream — Airflow will re-run
  the chain from that task.

### 2. `ingest_cdc` times out (`did not finish within ...s`)
- The Databricks run is still executing. Check the job UI: if it eventually
  succeeds, raise `CDC_POLL_TIMEOUT_SECONDS` in `.env` and restart the stack.
- If it is genuinely stuck, cancel the run in Databricks, then clear the task.

### 3. `silver_technical_tests` fails
- The task log lists failing tests. Query the failing rows via the test SQL shown
  in the log; decide: fix source data (re-ingest) or fix the model.
- Do **not** force-pass tests to "unblock" — they gate gold correctness.

### 4. `source_freshness` fails
- Bronze hasn't been updated within the SLA — usually the CDC job didn't run.
  Trigger the Databricks job manually, then clear `source_freshness`.

### 5. dbt tasks fail with connection/timeout errors
- SQL warehouse may be stopped (serverless auto-stops). Start it, clear the task.
- PAT expired → rotate: new token in `.env`, then `docker compose up -d`
  (recreates containers with the new env).
- **HTTP 404 on `OpenSession`** → almost always a wrong `DATABRICKS_HTTP_PATH`:
  it must be the *warehouse* path `/sql/1.0/warehouses/<id>` (Connection details
  pane), NOT the workspace host. Verify with `dbt debug` inside the worker
  (see [airflow-guide.md](airflow-guide.md)).

### 5b. `source_freshness` fails with `ERROR STALE` on ALL sources
- The freshness SLA (`SOURCE_FRESHNESS_*_DAYS` in `.env`) is tighter than the real
  change cadence of the source. With a static demo dataset the bronze tables age
  indefinitely — the ingest job succeeds but has nothing new to pull.
- Fix: relax the SLA (shipped defaults: warn 7d / error 365d) or feed the source
  new rows so CDC actually lands fresh data. See `models/source.yml` comments.

### 5c. Generic test fails with `PARSE_SYNTAX_ERROR` (e.g. at `>=`)
- For column-attached generic tests like `dbt_utils.expression_is_true`, dbt passes
  the column name automatically — the `expression` argument must be
  **operator-only** (`">= 0"`), not repeat the column (`"price >= 0"` renders
  `price price >= 0`). See the note in `models/silver_t/properties.yml`.
- Diagnose fast: `dbt compile --select <test>` and read the compiled SQL under
  `target/compiled/`.

### 5d. Local dbt fails: `Env var required but not provided: 'DATABRICKS_HOST'`
- Local (non-Docker) dbt has no `.env` loaded. Simplest fix: copy
  `walmart-airflow/.env` into `walmart_project/` — dbt auto-loads it from the
  working directory (it's git-ignored). Alternatives: the `load-env.ps1` session
  loader, or exporting the variables manually (see [setup-guide.md](setup-guide.md)).

### 6. Snapshot errors: duplicate keys
- Indicates two rows share a snapshot unique key (e.g. `(order_id, order_item_id)`).
- Deduplicate upstream (silver model) before re-running `dbt snapshot`.

## Reprocessing recipes

| Scenario | Action |
|---|---|
| Rerun everything for today | Airflow UI → clear → "Downstream + recursive" from `ingest_cdc` |
| Rebuild one silver model | `docker compose exec airflow-worker bash` → `cd /opt/airflow/walmart_project` → `dbt run --select orders_t --full-refresh` (add `--full-refresh` to rebuild from scratch) |
| Rebuild a dimension snapshot | ⚠️ `--full-refresh` on a snapshot **deletes history**; prefer fixing data and re-running normally |
| Backfill after a long outage | Temporarily set `catchup=True` or trigger manual runs per day; the incremental models' merge logic makes repeats safe |

## Cost & hygiene tips

- Serverless warehouse auto-stops; keep dbt `threads: 1` on small warehouses
  (already configured) to avoid queuing timeouts.
- `clean_workspace` keeps the bind-mounted project tidy; if logs grow large,
  prune `walmart-airflow/logs/` periodically (git-ignored).
- Rotate the Databricks PAT on a schedule (see [security.md](security.md)).

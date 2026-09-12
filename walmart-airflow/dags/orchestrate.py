"""
orchestrate.py — End-to-end Walmart medallion pipeline DAG.

Architecture
------------
    [Ghost Postgres (source DB)]
            |  Databricks Lakeflow ingest job (cursor-based CDC)
            v
        bronze schema  ──►  [Airflow: ingest_cdc]
                                │
                                ▼
                    dbt transformations (this DAG):
                    clean → source freshness → silver_t run/test
                    → silver_b run/test → gold ephemeral → gold
                    snapshots (SCD2) → gold facts

How it works
------------
1.  ``ingest_cdc`` triggers the Databricks Lakeflow ingestion job over the
    Workspace SDK REST API and polls the run until it terminates. The task
    blocks until the run finishes (an API trigger alone returns immediately,
    which would let downstream dbt tasks race against an unfinished ingest).
2.  ``clean_workspace`` removes stale dbt build artifacts (``target/``,
    ``logs/``) so subsequent dbt commands always start from a clean manifest.
3.  The remaining tasks shell out to the ``dbt`` CLI (installed in the custom
    Airflow image, see ../Dockerfile) against the dbt project bind-mounted at
    ``/opt/airflow/walmart_project``. Each command is prefixed with ``cd`` so
    dbt resolves dbt_project.yml / profiles.yml from the right folder.

Design notes
------------
*   Secrets come exclusively from environment variables (docker compose loads
    ``.env`` from this folder into every container). Nothing sensitive lives
    in this file.
*   Every dbt command is a separate ``@task.bash`` task, giving one node per
    layer in the Graph view and a natural retry/restart boundary per stage.
*   Polling has a hard timeout (``CDC_POLL_TIMEOUT_SECONDS``) so a stuck
    Databricks run fails the task instead of hanging a worker slot forever.
"""

from __future__ import annotations

import os
import time
from datetime import timedelta

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# --------------------------------------------------------------------------- #
# Configuration (environment-driven; see walmart-airflow/.env.example)
# --------------------------------------------------------------------------- #
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]                 # no scheme
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]               # PAT / OAuth M2M
DATABRICKS_CDC_JOB_ID = int(os.environ["DATABRICKS_CDC_JOB_ID"])

# How often to poll the Databricks run status, and the maximum time the task
# may wait before it fails instead of occupying a worker slot indefinitely.
CDC_POLL_INTERVAL_SECONDS = int(os.environ.get("CDC_POLL_INTERVAL_SECONDS", "15"))
CDC_POLL_TIMEOUT_SECONDS = int(os.environ.get("CDC_POLL_TIMEOUT_SECONDS", "3600"))

# Absolute path of the dbt project inside the containers (bind mount defined
# in docker-compose.yaml). Kept in one place so every task stays DRY.
DBT_PROJECT_DIR = "/opt/airflow/walmart_project"


def _dbt(command: str) -> str:
    """Build a shell command that runs a dbt CLI command in the project dir.

    dbt resolves ``dbt_project.yml`` and ``profiles.yml`` relative to the
    current working directory, so every dbt invocation must first ``cd`` into
    the bind-mounted project folder.
    """
    return f"cd {DBT_PROJECT_DIR} && {command}"


# Common DAG/task defaults: retries + alerting-friendly ownership metadata.
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #


@dag(
    dag_id="walmart_medallion_pipeline",
    description=(
        "CDC ingest from Postgres via Databricks, then dbt build-out of the "
        "silver (technical + business/OBT) and gold (star schema) layers."
    ),
    schedule="0 11 * * *",          # daily at 11:00 UTC — adjust per business SLA
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,                  # no backfill for missed runs
    max_active_runs=1,              # avoid overlapping runs racing on tables
    default_args=DEFAULT_ARGS,
    tags=["walmart", "dbt", "databricks", "medallion"],
    doc_md=__doc__,
)
def walmart_medallion_pipeline():
    """Build the task graph for the daily Walmart medallion pipeline."""

    # ------------------------------------------------------------------ #
    # 1. Ingestion — trigger the Databricks CDC job and wait for it
    # ------------------------------------------------------------------ #

    @task
    def ingest_cdc() -> str:
        """Trigger the Databricks CDC ingest job and block until it finishes.

        The job is configured in Databricks (Lakeflow declarative pipeline)
        to pull changed rows from the source Postgres database into the
        ``walmart.bronze`` schema using cursor-based incremental loading.

        Returns:
            The Databricks run id, useful for cross-referencing logs.

        Raises:
            AirflowFailException: if the job fails, times out, or the
                configuration is missing — the pipeline must not proceed on
                stale bronze data.
        """
        ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

        print(f"Triggering Databricks CDC ingest job {DATABRICKS_CDC_JOB_ID}…")
        trigger = ws.jobs.run_now(job_id=DATABRICKS_CDC_JOB_ID)
        run_id = trigger.run_id
        print(f"CDC ingest run started: {run_id}")

        # Hard deadline: fail instead of hanging a worker slot forever. The
        # Databricks run itself keeps executing and can be inspected in the UI.
        deadline = time.monotonic() + CDC_POLL_TIMEOUT_SECONDS

        while True:
            run = ws.jobs.get_run(run_id)
            state = run.state
            print(
                f"Job run status: life_cycle={state.life_cycle_state}, "
                f"result={state.result_state}"
            )

            if state.life_cycle_state in (
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ):
                if state.result_state == RunResultState.SUCCESS:
                    print("CDC ingest completed successfully.")
                    return str(run_id)
                raise AirflowFailException(
                    f"CDC ingest job {DATABRICKS_CDC_JOB_ID} (run {run_id}) "
                    f"finished with result_state={state.result_state}. "
                    f"Check the Databricks job UI for details."
                )

            if time.monotonic() > deadline:
                raise AirflowFailException(
                    f"CDC ingest run {run_id} did not finish within "
                    f"{CDC_POLL_TIMEOUT_SECONDS}s."
                )

            time.sleep(CDC_POLL_INTERVAL_SECONDS)

    # ------------------------------------------------------------------ #
    # 2. Workspace hygiene + source checks
    # ------------------------------------------------------------------ #

    @task.bash
    def clean_workspace() -> str:
        """Delete stale dbt artifacts so every run compiles from scratch.

        ``target/`` holds the compiled manifest and run results, ``logs/`` the
        dbt logs. Stale copies (e.g. from an interrupted previous run) can make
        later dbt commands resolve an outdated manifest.
        """
        return f"rm -rf {DBT_PROJECT_DIR}/target {DBT_PROJECT_DIR}/logs"

    @task.bash
    def source_freshness() -> str:
        """Check the bronze sources are within their freshness SLA.

        Fails (or warns, depending on severity configured in the project)
        before any transformation runs, preventing reads on stale data.
        """
        return _dbt("dbt source freshness")

    # ------------------------------------------------------------------ #
    # 3. Silver layer — technical (cleansed, incremental 1:1 bronze copies)
    # ------------------------------------------------------------------ #

    @task.bash
    def silver_technical_run() -> str:
        """Build all silver_t models (orders_t, customers_t, …) incrementally."""
        return _dbt("dbt run --select silver_t")

    @task.bash
    def silver_technical_tests() -> str:
        """Run data tests for silver_t; must pass before silver_b is built."""
        return _dbt("dbt test --select silver_t")

    # ------------------------------------------------------------------ #
    # 4. Silver layer — business (metadata-driven One Big Table: obt_b)
    # ------------------------------------------------------------------ #

    @task.bash
    def silver_business_run() -> str:
        """Build the One Big Table (obt_b) from the silver_t models."""
        return _dbt("dbt run --select silver_b")

    @task.bash
    def silver_business_tests() -> str:
        """Run OBT integrity tests (null dimension keys, etc.)."""
        return _dbt("dbt test --select silver_b")

    # ------------------------------------------------------------------ #
    # 5. Gold layer — ephemeral staging, SCD2 snapshots (dimensions), facts.
    #    NOTE: the gold_ephemeral "run" only registers lineage — ephemeral
    #    models are inlined as CTEs by their dependents, nothing is built.
    # ------------------------------------------------------------------ #

    @task.bash
    def gold_ephemeral() -> str:
        """Register gold ephemeral staging models (lineage only — no build)."""
        return _dbt("dbt run --select gold/ephimeral")

    @task.bash
    def gold_dimensions_scd2() -> str:
        """Build/refresh all SCD2 dimension snapshots (dbt snapshot)."""
        return _dbt("dbt snapshot")

    @task.bash
    def gold_facts_run() -> str:
        """Build fact_orders incrementally from the refreshed OBT."""
        return _dbt("dbt run --select gold/fact")

    # Linear dependency chain — keep in sync with the task definitions above.
    # (>> is the TaskFlow shift operator; all tasks here are strictly sequential.)
    (
        ingest_cdc()
        >> clean_workspace()
        >> source_freshness()
        >> silver_technical_run()
        >> silver_technical_tests()
        >> silver_business_run()
        >> silver_business_tests()
        >> gold_ephemeral()
        >> gold_dimensions_scd2()
        >> gold_facts_run()
    )


# Module-level instantiation — the DAG processor picks this up.
walmart_medallion_pipeline_dag = walmart_medallion_pipeline()

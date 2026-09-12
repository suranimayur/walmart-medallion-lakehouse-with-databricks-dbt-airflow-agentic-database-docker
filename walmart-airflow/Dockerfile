# ==============================================================================
# Custom Apache Airflow image for the Walmart pipeline
# ------------------------------------------------------------------------------
# The official apache/airflow image ships without dbt, so we extend it with the
# packages listed in requirements.txt (dbt-core, dbt-databricks, databricks-sdk).
# docker compose (see docker-compose.yaml, `build: .`) builds this image for
# every Airflow service, which is why the DAGs can simply invoke the `dbt` CLI.
#
# Rebuild after changing requirements.txt:
#   docker compose build && docker compose up -d
# ==============================================================================

# Base image — keep in sync with the version reference in docker-compose.yaml
FROM apache/airflow:3.3.1

# gcc is needed to build any sdist-only wheels pulled in by dbt's dependency
# tree (e.g. on some plugin releases); drop it if your builds never compile.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies as the unprivileged airflow user
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

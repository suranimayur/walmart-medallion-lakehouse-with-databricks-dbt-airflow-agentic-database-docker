# 🧑‍💻 Git & Release Guide

How this repository is organized for git, and how to push it to GitHub as a
professional portfolio repo.

## What is (and is not) committed

Committed:

- `walmart_project/` — the dbt project (SQL, YAML, macros, snapshots, tests)
- `walmart-airflow/` — DAG, Dockerfile, docker-compose, requirements, `.env.example`
- `docs/` + `README.md` — documentation
- `.gitignore`

Never committed (enforced by `.gitignore`):

- `.env` — real credentials
- `walmart_project/target/`, `walmart_project/logs/` — dbt build artifacts
- `walmart-airflow/logs/` — Airflow task logs (contain query text)
- `__pycache__/`, `.venv/`, `.user.yml`, IDE folders

> **Before your first push, verify nothing sensitive is tracked:**
> ```bash
> git ls-files | xargs grep -lE "(dapi|dbc-[0-9a-f]{8})" 2>/dev/null
> # Expected output: nothing.
> ```

## Pushing to GitHub

```bash
git checkout -b main                # if not already on main
git add .
git commit -m "feat: walmart medallion pipeline (Databricks + dbt + Airflow)"
git remote add origin https://github.com/<you>/<repo>.git
git push -u origin main
```

For the token-based auth prompt, use a fine-grained PAT (repo-scoped), and store it
with `git config credential.helper` or a credential manager — never in the repo.

## Repo recommendations (professional portfolio)

- **Name**: `walmart-medallion-lakehouse` (see below for more options)
- **Description**: "End-to-end medallion lakehouse: CDC from Postgres into Databricks,
  dbt transformations (OBT + star schema, SCD2), orchestrated by Airflow in Docker."
- **Topics**: `databricks`, `dbt`, `airflow`, `delta-lake`, `medallion-architecture`,
  `data-engineering`, `star-schema`, `cdc`
- Pin the repo on your profile; the architecture diagram in the README is the first
  thing a hiring manager should see.
- Keep `main` green: one commit per logical change; conventional-commit prefixes
  (`feat:`, `fix:`, `docs:`) read well in history.

### Name ideas

| Name | Angle |
|---|---|
| `walmart-medallion-lakehouse` | architecture-first, precise |
| `dbt-databricks-airflow-pipeline` | stack-first, searchable |
| `retail-lakehouse-cdc-dbt-airflow` | domain + stack |
| `walmart-airflow-dbt-project` | closest to the original folder names |

# 🔐 Security Guide

How secrets are handled in this project, and the rules that keep them out of git.

## Golden rules

1. **No secret ever lives in a tracked file.** All credentials enter through
   environment variables.
2. **`.env` is the single local store.** It is loaded by docker compose into every
   Airflow container and read by dbt's `profiles.yml` via `env_var()` for local runs.
3. **Rotate anything that was ever committed.** A token in git history is
   compromised, even if later deleted. Rotate first, clean history second
   (or simply keep the repo private and start fresh — see below).

## Where each secret lives

| Secret | Source of truth | Used by |
|---|---|---|
| `DATABRICKS_HOST` | `.env` (from warehouse Connection details) | DAG, profiles.yml |
| `DATABRICKS_TOKEN` | `.env` (rotating PAT) | DAG, profiles.yml |
| `DATABRICKS_HTTP_PATH` | `.env` | profiles.yml (dbt SQL warehouse) |
| `DATABRICKS_CDC_JOB_ID` | `.env` | DAG (`ingest_cdc`) |
| `FERNET_KEY`, `JWT_SECRET`, UI passwords | `.env` | Airflow platform |

Template with full instructions: [`walmart-airflow/.env.example`](../walmart-airflow/.env.example).

## Git hygiene — what is ignored

`.gitignore` (repo root) excludes:

- `.env`, `*.env` (except `.env.example`) — secrets
- `walmart_project/target/`, `logs/` — dbt build artifacts
- `airflow logs` (`walmart-airflow/logs/`) — Airflow task logs (contain query text)
- `__pycache__/`, `.pytest_cache/`, `.venv/` — Python by-products
- `.user.yml`, `.idea/`, `.vscode/` — personal tool state

**Verify before pushing** (this was run as part of the original setup — rerun any time):

```bash
git ls-files | xargs grep -lE "(dapi|dbc-[0-9a-f]{8}|https://dbc-)" 2>/dev/null
# Expected output: nothing.
```

Also spot-check: `git grep -iE "token|secret" -- '*.py' '*.yml' '*.sql' | grep -v env_var | grep -v .env.example`

## Rotating credentials

1. Databricks → Settings → Developer → Access tokens → revoke old token.
2. Generate a new one (set a lifetime).
3. Update `DATABRICKS_TOKEN` in `walmart-airflow/.env`.
4. `cd walmart-airflow && docker compose up -d` — recreates containers with the new
   value. Local dbt shells pick it up on next export.

## Production hardening (beyond this repo)

- Replace PATs with **OAuth M2M service principals** (short-lived tokens).
- Store secrets in a real manager (Azure Key Vault / AWS Secrets Manager / HashiCorp
  Vault) and inject into Airflow via a secrets backend rather than a plaintext `.env`.
- Scope the Databricks token's service principal to the minimum: can run the ingest
  job + use the one SQL warehouse.
- Enable Airflow's audit logs and Databricks system tables for access tracing.

## If a secret does get committed

1. **Rotate immediately** (that's the real fix).
2. Optionally rewrite history with `git filter-repo` or BFG, then force-push —
   coordinate with anyone who has cloned.
3. Never try to "hide" a secret by moving it to another tracked file.

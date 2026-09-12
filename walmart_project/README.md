# walmart_project — dbt transformations

dbt project implementing the silver + gold layers of the Walmart medallion
pipeline on Databricks (Unity Catalog `walmart`). Bronze is ingested by a
Databricks Lakeflow CDC job; orchestration lives in `../walmart-airflow/`.

📖 **End-to-end guide:** see [../docs/](../docs/) — start with
[architecture.md](../docs/architecture.md), then [dbt-guide.md](../docs/dbt-guide.md).

## Layout

| Path | Purpose |
|---|---|
| `models/silver_t/` | Incremental 1:1 cleansed copies of bronze tables |
| `models/silver_b/` | `obt_b` — metadata-driven One Big Table (order-item grain) |
| `models/gold/ephimeral/` | Ephemeral staging projections feeding the snapshots |
| `models/gold/fact/` | `fact_orders` — star-schema fact (incremental) |
| `snapshots/` | SCD Type 2 dimensions (`dim_*`) built by `dbt snapshot` |
| `tests/` | Singular data tests (zero-rows convention) |
| `macros/` | `generate_schema_name` override (custom schemas used verbatim) |

## Common commands

```bash
cd walmart_project
dbt debug                  # validate connection + project
dbt run --select silver_t  # build one layer
dbt test                   # run all data tests
dbt snapshot               # build/refresh SCD2 dimensions
dbt docs generate && dbt docs serve
```

Connection details come from environment variables — see
[../walmart-airflow/.env.example](../walmart-airflow/.env.example).

> 💡 **Local dev tip:** copy `../walmart-airflow/.env` into this folder — dbt
> auto-loads it from the working directory (it's git-ignored, so secrets stay
> local). Then just run `dbt debug` / `dbt run` with no exports.

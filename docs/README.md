# 📚 Documentation — Walmart Medallion Pipeline

Welcome to the documentation for the **Walmart Lakehouse project**: a production-style,
end-to-end data platform that ingests change-data-capture (CDC) streams from an
operational Postgres database into Databricks, transforms them with **dbt**, and serves
a **star schema** — all orchestrated daily by **Apache Airflow** running in Docker.

## Documentation map

| Doc | Read it when you want to… |
|---|---|
| [Architecture](architecture.md) | Understand the medallion layers, data flow, and star schema design |
| [Setup guide](setup-guide.md) | Build and run the entire project from zero (sources → Databricks → dbt → Airflow) |
| [dbt project guide](dbt-guide.md) | Understand every dbt model, macro, test, and snapshot in this repo |
| [Airflow guide](airflow-guide.md) | Understand the DAG, the custom Docker image, and scheduling |
| [Operations runbook](runbook.md) | Run day-2 operations: monitoring, reprocessing, troubleshooting |
| [Security guide](security.md) | Manage secrets, env vars, and keep credentials out of git |
| [Git & release guide](git-guide.md) | Push the project to a professional GitHub repository |

## The 60-second version

```
Ghost Postgres (source)                                  Databricks Unity Catalog
┌──────────────────────┐    CDC ingest job    ┌─────────────────────────────────────┐
│ customers / orders   │ ───────────────────► │ bronze  → raw landed rows           │
│ products / employees │  (Airflow task 1)    │ silver_t → cleansed 1:1 models (dbt)│
│ stores / order_items │                      │ silver_b → One Big Table      (dbt) │
└──────────────────────┘                      │ gold    → star schema         (dbt) │
                                              └─────────────────────────────────────┘
                                                              ▲
                                              Apache Airflow (Docker, daily 11:00)
```

Every layer below `bronze` is built by **dbt** and orchestrated by the
[`walmart_medallion_pipeline`](../walmart-airflow/dags/orchestrate.py) DAG.

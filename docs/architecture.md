# 🏗️ Architecture

## 1. High-level data flow

```
 ┌─────────────────────┐         ┌─────────────────────────────────────────────────┐
 │  SOURCES            │         │  DATABRICKS (Unity Catalog: walmart)            │
 │                     │         │                                                 │
 │  Ghost Postgres     │   CDC   │  ┌─────────┐   ┌──────────┐   ┌──────────────┐  │
 │  (agentic OLTP DB)  │ ──────► │  │ bronze  │──►│ silver_t │──►│  silver_b    │  │
 │  customers, orders, │  job    │  │ (raw)   │   │ (1:1     │   │  (One Big    │  │
 │  products, employ-  │         │  └─────────┘   │  clean)  │   │   Table)     │  │
 │  ees, stores,       │         │                └──────────┘   └──────┬───────┘  │
 │  order_items        │         │                                      │          │
 │                     │         │                                      ▼          │
 │  Static mapping     │         │                ┌──────────────────────────────┐  │
 │  CSVs (data lake)   │ ──────► │                │ gold (star schema)           │  │
 └─────────────────────┘         │                │  dims: SCD2 snapshots        │  │
                                 │                │  fact: fact_orders           │  │
                                 │                └──────────────────────────────┘  │
                                 └─────────────────────────────────────────────────┘
                                              ▲ orchestrated by
                                 ┌────────────┴────────────┐
                                 │  Apache Airflow (Docker)│
                                 │  walmart_medallion_     │
                                 │  pipeline DAG           │
                                 └─────────────────────────┘
```

## 2. Why this shape (design rationale)

| Concern | Choice | Why |
|---|---|---|
| Ingestion | Databricks Lakeflow CDC job | Production-grade change detection — no hand-written incremental watermarks at the source; new/updated source rows land in `bronze` automatically each run |
| Raw storage | `bronze` schema, Delta tables | Append-optimized landing zone; full history retained for replay |
| Cleansing | dbt `silver_t` models, incremental | Deterministic SQL transforms with type casts, dedup, and renaming; incremental + MERGE keeps runs cheap |
| Analytics-ready grain | dbt `silver_b` One Big Table (OBT) | One wide, denormalized table that joins all silver entities at order-item grain — fast ad-hoc analytics without modeling overhead |
| Dimensional serving | dbt `gold` layer | Classic star schema: conformed dimensions (SCD2 snapshots) + a transaction-grain fact table for BI tools |
| Orchestration | Apache Airflow 3 in Docker | Scheduling, retries, observability, and clear stage boundaries per layer |
| Secrets | Environment variables only | Nothing sensitive in git; single `.env` loaded by docker compose and read by dbt `profiles.yml` |

## 3. Medallion layers in Unity Catalog (`walmart`)

| Layer | Schema | Built by | Materialization | Grain |
|---|---|---|---|---|
| Raw | `bronze` | Databricks CDC job | Delta tables | As-landed source rows |
| Technical silver | `silver_t` | dbt `models/silver_t/*` | Incremental tables | 1 row per business entity (deduped, typed, renamed) |
| Business silver | `silver_b` | dbt `models/silver_b/obt_b.sql` | Table | 1 row per order item, all attributes joined in |
| Gold | `gold` | dbt `models/gold/*` + snapshots | Ephemeral → snapshots (SCD2) → incremental fact | Star schema |

> The `ephimeral` folder name in the repo is a historical typo for "ephemeral" that is
> kept intentionally so the DAG selectors (`gold/ephimeral`) keep working; the models
> themselves are configured `materialized='ephemeral'` in `dbt_project.yml`.

## 4. Gold star schema

```
                    ┌───────────────┐
                    │ dim_customers │◀───┐
                    └───────┬───────┘    │
┌──────────────┐            │            │
│ dim_products │────────┐   │            │
└──────┬───────┘        │   │            │
       │        ┌───────┴───┴────────┐   │
       └───────►│    fact_orders     │───┼──┐
                │  (grain: 1 row per │   │  │
                │   order item)      │◄──┼──┤
                └───────┬────────────┘   │  │
                        │                │  │
                 ┌──────┴───────┐  ┌─────┴──┴──────┐
                 │ dim_employees│  │  dim_stores   │
                 └──────────────┘  └───────────────┘
                 (dim_orders SCD2 snapshot also exists for
                  order-status history)
```

- **Dimensions** are built by dbt **snapshots** (`snapshots/dim_*.yml` + select logic in
  the snapshot SQL), giving Type-2 history via `dbt_valid_from/to` columns.
- **fact_orders** is incremental, keyed on the order-item business key, with measures
  (quantity, price, amounts) and surrogates/keys pointing at the dimensions.

## 5. Orchestration timeline (one DAG run)

```
ingest_cdc ──► clean_workspace ──► source_freshness ──► silver_t run ──► silver_t tests
                                                                        │
gold facts ◄── gold snapshots (SCD2) ◄── gold ephemeral ◄── silver_b run ◄┘
                                                     (OBT)     & tests
```

Each stage is a separate Airflow task: a failure in, say, `silver_t tests` stops the
pipeline before wrong data can propagate into gold.

## 6. Repository layout

```
.
├── walmart_project/          # dbt project (single copy — bind-mounted into Airflow)
│   ├── models/               #   bronze→silver→gold transformations (SQL + YAML)
│   ├── macros/               #   custom schema-name generation
│   ├── snapshots/            #   SCD2 dimension snapshots
│   ├── tests/                #   singular data tests
│   ├── profiles.yml          #   env_var()-driven connection (no secrets)
│   └── dbt_project.yml       #   per-layer materialization config
├── walmart-airflow/          # Airflow platform (renamed from airflow/)
│   ├── dags/orchestrate.py   #   the pipeline DAG
│   ├── docker-compose.yaml   #   CeleryExecutor stack + dbt bind mount
│   ├── Dockerfile            #   airflow + dbt image
│   ├── requirements.txt      #   dbt-core, dbt-databricks, databricks-sdk
│   └── .env.example          #   template for the real .env (git-ignored)
├── docs/                     # this documentation
└── README.md                 # project front page
```

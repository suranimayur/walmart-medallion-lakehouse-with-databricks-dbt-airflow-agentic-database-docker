# 🧱 dbt Project Guide — walmart_project

Reference for every model, macro, test, and snapshot. Start at the top for concepts,
dive into sections as needed.

## Project layout

```
walmart_project/
├── models/
│   ├── source.yml              # bronze sources + freshness config
│   ├── silver_t/               # technical layer: cleansed 1:1 copies of bronze
│   │   ├── orders_t.sql  customers_t.sql  products_t.sql
│   │   ├── employees_t.sql  stores_t.sql  order_items_t.sql
│   │   └── properties.yml      # column tests + schema docs
│   ├── silver_b/
│   │   └── obt_b.sql           # One Big Table (order-item grain, all joins)
│   └── gold/
│       ├── ephimeral/          # ephemeral staging CTEs feeding the fact table
│       │   ├── eph_customers.sql  eph_orders.sql  eph_products.sql
│       │   └── eph_employees.sql  eph_stores.sql
│       └── fact/
│           └── fact_orders.sql # transaction-grain fact table
├── snapshots/                  # SCD2 dimensions (run via `dbt snapshot`)
│   ├── dim_customers.sql/.yml  dim_products.sql/.yml  dim_employees.sql/.yml
│   └── dim_stores.sql/.yml     dim_orders.sql/.yml
├── macros/
│   └── custom_schema.sql       # generate_schema_name override
├── tests/
│   └── test_obt.sql            # singular test on the OBT
├── dbt_project.yml             # materialization strategy per layer
└── profiles.yml                # env_var()-based connection (no secrets)
```

## Concepts used (worth mentioning in interviews)

| Concept | Where |
|---|---|
| Incremental models + `merge` | all `silver_t` models, `fact_orders` |
| Ephemeral materialization | `gold/ephimeral/*` |
| One Big Table (OBT) | `silver_b/obt_b.sql` |
| SCD Type-2 via snapshots | `snapshots/*` |
| Source freshness | `models/source.yml` |
| Custom schema naming | `macros/custom_schema.sql` |
| `env_var()` for secrets | `profiles.yml` |
| Generic + singular tests | `silver_t/properties.yml`, `tests/test_obt.sql` |

## Layer by layer

### 1. Sources — `models/source.yml`

Declares the six bronze tables as dbt sources so models can use `{{ source() }}`
(which builds lineage and enables `dbt source freshness`). Freshness thresholds are
**env-configurable** (`SOURCE_FRESHNESS_WARN_DAYS` / `SOURCE_FRESHNESS_ERROR_DAYS`,
see `.env.example`) and default to warn 7d / error 365d — tuned for this project's
static demo dataset. Tighten them (e.g. warn 1 / error 2 days) for a live source.

### 2. Silver technical — `models/silver_t/`

Each model is **incremental with a unique key and `merge` strategy**: on every run,
new/changed rows from bronze are upserted; nothing is rebuilt. Common pattern:

```sql
{{ config(materialized='incremental', unique_key='...', incremental_strategy='merge') }}
with src as (select * from {{ source('walmart_bronze', '...') }})
...
where ... {% if is_incremental() %} and cursor_col > (select max(cursor_col) from {{ this }}) {% endif %}
```

Cleaning applied consistently: explicit casts, `lower()`/`trim()` on names/emails,
date normalization, dedup on the business key (latest record wins), and renaming to
snake_case business-friendly names.

`properties.yml` attaches generic tests (`unique`, `not_null`, relationships) per
model — the DAG runs `dbt test --select silver_t` right after the run and blocks the
pipeline on failure.

### 3. Silver business — `models/silver_b/obt_b.sql`

The **One Big Table**: joins orders, order_items, customers, products, employees,
stores into one wide table at order-item grain. Built as a plain table because the
fan-out join makes incremental bookkeeping more expensive than a rebuild at this
scale. All references use `{{ ref() }}` (lineage + environment portability — no
hardcoded schema names).

### 4. Gold ephemeral — `models/gold/ephimeral/`

Thin, dimension-shaped projections of the OBT used to stage snapshot/fact inputs.
`materialized='ephemeral'` means dbt **inlines them as CTEs** into dependents —
nothing is persisted, but lineage still shows them.

### 5. Gold dimensions — `snapshots/`

`dbt snapshot` builds Type-2 slowly-changing dimensions: when a tracked attribute
changes, the old row is closed out (`dbt_valid_to` set) and a new row is inserted.
`dim_orders` uses the composite check key `(order_id, order_item_id)` matching the
OBT grain, so order-status history is captured without row loss.

### 6. Gold fact — `models/gold/fact/fact_orders.sql`

Incremental fact at order-item grain, referencing the OBT; measures (quantity,
unit price, gross/net amounts) plus dimension keys. The DAG runs it last, after
snapshots are refreshed.

### 7. Tests

- **Generic** (YAML): `unique`, `not_null`, `accepted_values`, relationships —
  attached in `silver_t/properties.yml` and snapshot YAMLs.
- **Singular** (`tests/test_obt.sql`): cross-model business assertion on the OBT
  (e.g. no orphaned dimension joins / negative amounts — see the file for the exact
  check). Failing rows are returned; zero rows = pass.

## Command cheat sheet

```bash
cd walmart_project
dbt debug                     # validate connection
dbt parse                     # fast syntax/lineage check (no warehouse calls)
dbt run --select silver_t     # technical layer
dbt test  --select silver_t   # its tests
dbt run --select silver_b     # OBT
dbt snapshot                  # SCD2 dimensions
dbt run --select gold/fact    # fact table
dbt build                     # everything, dependency-ordered (local use)
dbt docs generate && dbt docs serve   # lineage + column docs
```

> ⚠️ dbt v1.12 note: `freshness` and `loaded_at_field` must be nested under
> `config:` in source definitions — this project already follows that syntax.

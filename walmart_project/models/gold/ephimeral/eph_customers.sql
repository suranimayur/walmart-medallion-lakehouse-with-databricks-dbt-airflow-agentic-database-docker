{#--
  ==============================================================================
  Model: eph_customers       Layer: gold (staging)      Materialization: ephemeral
  ==============================================================================
  Staging projection for dim_customers (the SCD2 snapshot consumes this model
  via `relation: ref('eph_customers')` in snapshots/dim_customers.yml).

  Why ephemeral
    The model is never built as a table or view — dbt inlines it as a CTE
    inside its dependents (the snapshot query). Keeps the catalog clean and
    avoids a redundant materialization between OBT and dimension.

  Grain / dedup
    `distinct` collapses the OBT's order-item grain back to one row per
    customer, so the snapshot sees customer-level grain (its unique_key).
    The timestamp strategy compares customer_updated_timestamp across runs —
    current_timestamp() columns must NOT feed `updated_at`.
--#}

SELECT DISTINCT
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone_number,
    customer_city,
    customer_province,
    customer_country,
    customer_created_timestamp,
    customer_updated_timestamp,     -- SCD2 change detector (snapshot updated_at)
    customer_is_active,
    customer_processed_at
FROM {{ ref('obt_b') }}

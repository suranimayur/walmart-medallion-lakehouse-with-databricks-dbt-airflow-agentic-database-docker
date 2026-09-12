{#--
  ==============================================================================
  Model: orders_t            Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `orders` table (built by the Databricks
    CDC ingest job), enriched with an audit column.

  Why incremental
    Orders is the largest source table. On each run we only read rows whose
    `updated_timestamp` is newer than the newest row already in this model,
    then dbt MERGEs them on `order_id` (unique_key) — new rows are inserted,
    changed rows are updated in place.

  Contract
    * Grain:        one row per order_id
    * Unique key:   order_id (tested not_null + unique, see properties.yml)
    * Audit column: processed_at — set to the transform time, NOT the source
                    event time; use order timestamps for business logic.
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'orders') }}

{% if is_incremental() %}
  -- Only rows updated since the last successful load are picked up;
  -- COALESCE makes the very first run (empty target) read everything.
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

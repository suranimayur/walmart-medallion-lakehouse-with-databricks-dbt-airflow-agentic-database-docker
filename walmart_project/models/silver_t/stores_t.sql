{#--
  ==============================================================================
  Model: stores_t            Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `stores` table with an audit column.
    Small dimension-style table feeding dim_stores.

  Why incremental
    Store master data rarely changes; the cursor filter means a run with no
    source changes reads zero rows and the MERGE is a no-op.

  Contract
    * Grain:        one row per store_id
    * Unique key:   store_id
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'store_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'stores') }}

{% if is_incremental() %}
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

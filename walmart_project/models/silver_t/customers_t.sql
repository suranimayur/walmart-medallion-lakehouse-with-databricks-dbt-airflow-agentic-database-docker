{#--
  ==============================================================================
  Model: customers_t         Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `customers` table with an audit column.
    Customer master data feeds dim_customers (via obt_b + eph_customers and
    the SCD2 snapshot).

  Why incremental
    Customer updates (name/email changes) arrive as updated rows; the
    cursor-based WHERE clause + MERGE on customer_id keeps this model in sync
    without full-table rewrites.

  Contract
    * Grain:        one row per customer_id
    * Unique key:   customer_id (tested, see properties.yml)
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'customer_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'customers') }}

{% if is_incremental() %}
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

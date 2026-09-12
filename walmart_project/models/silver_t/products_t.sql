{#--
  ==============================================================================
  Model: products_t          Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `products` table with an audit column.
    Feeds dim_products and carries the `price` column guarded by the
    negative-price test in properties.yml.

  Why incremental
    Product catalog changes are sparse; incremental + MERGE on product_id
    avoids re-copying the whole catalog each run.

  Contract
    * Grain:        one row per product_id
    * Unique key:   product_id (tested, see properties.yml)
    * Guard:        price >= 0 (expression test)
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'product_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'products') }}

{% if is_incremental() %}
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

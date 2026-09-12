{#--
  ==============================================================================
  Model: order_items_t       Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `order_items` table (order line items)
    with an audit column. This is the grain-driving table for the whole gold
    star schema: fact_orders is one row per order_item.

  Why incremental
    Line items are the highest-volume table; incremental + MERGE on
    order_item_id keeps runs cheap.

  Contract
    * Grain:        one row per order_item_id
    * Unique key:   order_item_id
    * Note:         order_id is a *foreign* key here (an order has many items),
                    which is why fact grain = order_item, not order.
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'order_item_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'order_items') }}

{% if is_incremental() %}
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

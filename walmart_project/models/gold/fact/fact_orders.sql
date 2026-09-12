{#--
  ==============================================================================
  Model: fact_orders         Layer: gold (star schema)    Type: incremental table
  ==============================================================================
  The fact table of the Walmart star schema. One row per ORDER ITEM.

  Design
    * Measures:     total_amount, order_item_quantity, unit_price, line_amount
    * Dimension FKs: order_id + order_item_id (dim_orders), product_id,
                    store_id, customer_id, employee_id
    * Keys are natural keys carried straight from the OBT — the dimensions
      snapshot the same natural keys, so fact-to-dimension joins work without
      surrogate-key lookups. (For high-cardinality or rotating keys, switch to
      dbt_utils.generate_surrogate_key() and look up keys in dimensions.)

  Why incremental on order_item_id
    The fact inherits the OBT grain (order item); rows touched upstream are
    re-merged. NOTE the grain asymmetry: dim_orders is order-item grain too,
    while eph_orders' distinct makes each (order_id, order_item_id) pair
    unique — join on both keys. See docs/architecture.md.

  Excluded on purpose
    created/updated/processed audit timestamps — kept in dim_orders and the
    silver layers so the fact stays lean for aggregation.
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'order_item_id',
    on_schema_change = 'fail',
) }}

SELECT
    -- dimension foreign keys -------------------------------------------------
    order_id,
    order_item_id,
    product_id,
    store_id,
    customer_id,
    employee_id,

    -- measures ---------------------------------------------------------------
    total_amount,
    order_item_quantity,
    order_item_unit_price,
    order_item_line_amount

FROM {{ ref('obt_b') }}

{% if is_incremental() %}
  -- Re-merge rows whose order changed since the last fact build. Matching on
  -- order-level updated_timestamp is deliberate: an order change can affect
  -- every line item (e.g. total_amount), and order_item rows are picked up
  -- via the same join through the refreshed OBT.
  WHERE order_updated_timestamp >
        (SELECT COALESCE(MAX(order_updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

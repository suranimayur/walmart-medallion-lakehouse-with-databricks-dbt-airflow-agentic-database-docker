{#--
  ==============================================================================
  Model: eph_orders          Layer: gold (staging)      Materialization: ephemeral
  ==============================================================================
  Staging projection for dim_orders (consumed by the SCD2 snapshot in
  snapshots/dim_orders.yml).

  Grain — READ BEFORE JOINING
    This model is at ORDER-ITEM grain, not order grain: the OBT fans out
    through order_items, and order_item_id is kept on purpose. The snapshot
    config sets unique_key: ['order_id', 'order_item_id'] to match, producing
    an order-item-grain dimension.

    When joining dim_orders to fact_orders you MUST join on both keys:
        f.order_id = d.order_id AND f.order_item_id = d.order_item_id
    Joining on order_id alone multiplies rows. See docs/architecture.md.

  Dedup
    `distinct` removes join-fanout duplicates that share the same
    (order_id, order_item_id) pair.
--#}

SELECT DISTINCT
    order_id,
    order_item_id,                  -- part of the snapshot's composite key
    payment_method,
    order_status,
    order_timestamp,
    order_created_timestamp,
    order_updated_timestamp,        -- SCD2 change detector (snapshot updated_at)
    order_is_active,
    order_processed_at,
    obt_b_processed_at
FROM {{ ref('obt_b') }}

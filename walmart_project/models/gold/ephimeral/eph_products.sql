{#--
  ==============================================================================
  Model: eph_products        Layer: gold (staging)      Materialization: ephemeral
  ==============================================================================
  Staging projection for dim_products (consumed by snapshots/dim_products.yml).

  Notes
    * customer_id was removed from this projection: it leaked order-level
      data into a product context and could even corrupt the snapshot's
      change detection (a product row's identity changing per customer).
      Product context = catalog attributes only.
    * product_price is kept deliberately: it is a product attribute (list
      price), not a measure — order-level money stays in fact_orders.
    * `distinct` collapses the OBT grain back to one row per product.
--#}

SELECT DISTINCT
    product_id,
    product_name,
    product_category,
    product_brand,
    product_price,                  -- catalog attribute, not an order measure
    product_created_timestamp,
    product_updated_timestamp,      -- SCD2 change detector (snapshot updated_at)
    product_is_active,
    product_processed_at
FROM {{ ref('obt_b') }}

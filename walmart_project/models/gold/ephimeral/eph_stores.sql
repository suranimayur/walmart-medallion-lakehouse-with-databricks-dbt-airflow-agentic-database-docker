{#--
  ==============================================================================
  Model: eph_stores          Layer: gold (staging)      Materialization: ephemeral
  ==============================================================================
  Staging projection for dim_stores (consumed by snapshots/dim_stores.yml).

  Why ephemeral + distinct
    The snapshot inlines this query as a CTE; `distinct` collapses the OBT's
    order-item grain back to one row per store.
--#}

SELECT DISTINCT
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_created_timestamp,
    store_updated_timestamp,        -- SCD2 change detector (snapshot updated_at)
    store_processed_at
FROM {{ ref('obt_b') }}

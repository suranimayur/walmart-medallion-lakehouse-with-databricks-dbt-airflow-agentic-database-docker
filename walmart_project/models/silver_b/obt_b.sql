{#--
  ==============================================================================
  Model: obt_b               Layer: silver_b (business)       Type: table
  ==============================================================================
  One Big Table (OBT) — the analytical workhorse of the silver layer.

  PURPOSE
    Joins every silver_t table into one wide, denormalized table so analysts
    and the gold layer can query a single relation instead of writing 5-way
    joins. Downstream consumers: eph_* models and fact_orders (via ref()).

  METADATA-DRIVEN DESIGN
    The join topology and column selection are generated from the `configs`
    list below. To extend the OBT (new table / new columns) EDIT ONLY THE
    CONFIG — never the query logic at the bottom:
      * "ref"             dbt model name (passed to ref()) — keeps lineage and
                          environment portability; do NOT hard-code catalog/
                          schema here
      * "alias"           SQL alias used in the join and in the column list
      * "columns"         raw SQL select list (columns already aliased per
                          entity so names never collide across tables)
      * "join_condition"  join predicate; OMIT it for the first (base) table

  GRAIN — IMPORTANT
    The base table is orders_t (1 row per order), but the order_items_t join
    fans the result out to one row per ORDER ITEM. The grain of this model is
    therefore the order item. Consequences:
      * fact_orders shares this grain (one row per order item), and
      * dim_orders (which is at order grain, deduplicated) must be joined to
        the fact on BOTH order_id and order_item_id, otherwise joins multiply
        rows. See docs/architecture.md for the full join-key matrix.

  Jinja notes
    `{% set %}` defines the config; two `{% for %}` loops render (1) the
    comma-separated column list and (2) FROM + LEFT JOINs. `loop.first`
    selects the base table, `loop.last` suppresses the trailing comma.
--#}

{% set configs = [
    {
        "ref": "orders_t",
        "alias": "o",
        "columns": "
            o.order_id,
            o.customer_id AS order_customer_id,
            o.store_id AS order_store_id,
            o.order_timestamp,
            o.payment_method,
            o.order_status,
            o.total_amount,
            o.created_timestamp AS order_created_timestamp,
            o.updated_timestamp AS order_updated_timestamp,
            o.is_active AS order_is_active,
            o.processed_at AS order_processed_at,
            current_timestamp() AS obt_b_processed_at
        "
    },
    {
        "ref": "customers_t",
        "alias": "c",
        "columns": "
            c.customer_id,
            c.first_name AS customer_first_name,
            c.last_name AS customer_last_name,
            c.email AS customer_email,
            c.phone AS customer_phone_number,
            c.city AS customer_city,
            c.province AS customer_province,
            c.country AS customer_country,
            c.created_timestamp AS customer_created_timestamp,
            c.updated_timestamp AS customer_updated_timestamp,
            c.is_active AS customer_is_active,
            c.processed_at AS customer_processed_at
        ",
        "join_condition": "o.customer_id = c.customer_id"
    },
    {
        "ref": "order_items_t",
        "alias": "oi",
        "columns": "
            oi.order_item_id,
            oi.order_id AS order_item_order_id,
            oi.product_id AS order_item_product_id,
            oi.quantity AS order_item_quantity,
            oi.unit_price AS order_item_unit_price,
            oi.line_amount AS order_item_line_amount,
            oi.created_timestamp AS order_item_created_timestamp,
            oi.updated_timestamp AS order_item_updated_timestamp,
            oi.is_active AS order_item_is_active,
            oi.processed_at AS order_item_processed_at
        ",
        "join_condition": "o.order_id = oi.order_id"
    },
    {
        "ref": "products_t",
        "alias": "p",
        "columns": "
            p.product_id,
            p.product_name,
            p.category AS product_category,
            p.brand AS product_brand,
            p.price AS product_price,
            p.created_timestamp AS product_created_timestamp,
            p.updated_timestamp AS product_updated_timestamp,
            p.is_active AS product_is_active,
            p.processed_at AS product_processed_at
        ",
        "join_condition": "oi.product_id = p.product_id"
    },
    {
        "ref": "stores_t",
        "alias": "s",
        "columns": "
            s.store_id,
            s.store_name,
            s.city AS store_city,
            s.province AS store_province,
            s.country AS store_country,
            s.created_timestamp AS store_created_timestamp,
            s.updated_timestamp AS store_updated_timestamp,
            s.processed_at AS store_processed_at
        ",
        "join_condition": "o.store_id = s.store_id"
    },
    {
        "ref": "employees_t",
        "alias": "e",
        "columns": "
            e.employee_id,
            e.store_id AS employee_store_id,
            e.first_name AS employee_first_name,
            e.last_name AS employee_last_name,
            e.email AS employee_email,
            e.job_title AS employee_job_title,
            e.salary AS employee_salary,
            e.created_timestamp AS employee_created_timestamp,
            e.updated_timestamp AS employee_updated_timestamp,
            e.is_active AS employee_is_active,
            e.processed_at AS employee_processed_at
        ",
        "join_condition": "o.store_id = e.store_id"
    }
] %}

SELECT
    {%- for config in configs %}
        {{- config['columns'] }}
        {{- "," if not loop.last }}
    {%- endfor %}

FROM
    {%- for config in configs %}
        {%- if loop.first %}
            {{ ref(config['ref']) }} AS {{ config['alias'] }}
        {%- else %}
            LEFT JOIN {{ ref(config['ref']) }} AS {{ config['alias'] }}
                ON {{ config['join_condition'] }}
        {%- endif %}
    {%- endfor %}

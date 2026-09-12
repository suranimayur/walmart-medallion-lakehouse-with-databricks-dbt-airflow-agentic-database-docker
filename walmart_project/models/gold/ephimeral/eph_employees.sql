{#--
  ==============================================================================
  Model: eph_employees       Layer: gold (staging)      Materialization: ephemeral
  ==============================================================================
  Staging projection for dim_employees (consumed by
  snapshots/dim_employees.yml).

  Design context
    Employee -> order is indirect (an employee belongs to a store; orders are
    booked at a store). Keeping dim_employees lets "which staff belong to the
    store that booked this order?" be answered without a snowflake-style hop
    through dim_stores — a denormalization trade-off documented in
    docs/architecture.md.
--#}

SELECT DISTINCT
    employee_id,
    employee_first_name,
    employee_last_name,
    employee_email,
    employee_job_title,
    employee_salary,
    employee_created_timestamp,
    employee_updated_timestamp,     -- SCD2 change detector (snapshot updated_at)
    employee_is_active,
    employee_processed_at
FROM {{ ref('obt_b') }}

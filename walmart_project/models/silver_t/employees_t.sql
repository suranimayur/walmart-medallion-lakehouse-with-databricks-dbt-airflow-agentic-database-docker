{#--
  ==============================================================================
  Model: employees_t         Layer: silver_t (technical)      Type: incremental
  ==============================================================================
  Purpose
    1:1 cleansed copy of the bronze `employees` table with an audit column.
    Employees are store staff (cashiers etc.) linked to stores via store_id;
    they feed dim_employees, which lets the fact table answer
    "which employee handled this order?".

  Why incremental
    Staff changes (new hires, role/salary updates) are incremental; MERGE on
    employee_id keeps the model current.

  Contract
    * Grain:        one row per employee_id
    * Unique key:   employee_id
--#}

{{ config(
    materialized = 'incremental',
    unique_key   = 'employee_id',
) }}

SELECT
    *,
    current_timestamp() AS processed_at
FROM {{ source('walmart_databricks', 'employees') }}

{% if is_incremental() %}
  WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}

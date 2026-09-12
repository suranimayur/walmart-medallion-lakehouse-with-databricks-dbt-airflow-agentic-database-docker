{#--
  ==============================================================================
  Test: test_obt_b_not_null_keys                     Type: singular (generic SQL)
  ==============================================================================
  Rule for singular tests: the query must return ZERO rows for the test to
  pass — every returned row is a violation.

  This test guards the OBT join integrity: if any dimension FK is null, the
  corresponding LEFT JOIN lost a match, and any fact/dimension built on top
  would silently drop or mis-attribute that order item.

  Severity is `warn` so the pipeline continues while the data team
  investigates (a hard failure here would block every downstream build on a
  single late-arriving dimension record). Switch to 'error' if the business
  requires strict referential integrity.
--#}

{{ config(severity='warn') }}

SELECT order_id
FROM {{ ref('obt_b') }} AS obt
WHERE obt.order_id      IS NULL
   OR obt.product_id    IS NULL
   OR obt.store_id      IS NULL
   OR obt.employee_id   IS NULL
   OR obt.order_item_id IS NULL
   OR obt.customer_id   IS NULL

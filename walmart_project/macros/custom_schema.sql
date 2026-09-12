{%#
  ==============================================================================
  Macro: generate_schema_name
  ------------------------------------------------------------------------------
  Overrides dbt's default behavior. By default dbt names custom schemas
  "<target.schema>_<custom_schema>" (e.g. dbt_schema_silver_t). This override
  uses the custom schema name VERBATIM, so the +schema: values in
  dbt_project.yml map directly to the bronze/silver_t/silver_b/gold layout in
  the Databricks `walmart` catalog.

  Trade-off to know: with this override, two developers running against the
  same target share the same schemas. For isolated dev sandboxes, restore the
  default concatenation or branch on target.name.
  ==============================================================================
%}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}

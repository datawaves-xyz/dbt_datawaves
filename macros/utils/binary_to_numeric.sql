{% macro binary_to_numeric(expression) %}
  {{ return(adapter.dispatch('binary_to_numeric', 'ethereum') (expression)) }}
{% endmacro %}


{% macro default__binary_to_numeric(expression) %}
    cast(conv(hex({{ expression }}), 16, 10) as {{ dbt_utils.type_numeric() }})
{% endmacro %}


{% macro spark__binary_to_numeric(expression) %}
    cast(conv(hex({{ expression }}), 16, 10) as {{ dbt_utils.type_numeric() }})
{% endmacro %}

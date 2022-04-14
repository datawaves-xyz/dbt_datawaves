{% macro binary_to_address(expression) %}
  {{ return(adapter.dispatch('binary_to_address', 'ethereum') (expression)) }}
{% endmacro %}


{% macro default__binary_to_address(expression) -%}
    concat('0x', lower(hex({{ expression }})))
{%- endmacro %}


{% macro spark__binary_to_address(expression) -%}
    concat('0x', lower(hex({{ expression }})))
{%- endmacro %}

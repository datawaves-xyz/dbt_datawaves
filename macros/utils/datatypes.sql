{# Fork from dbt-utils/macros/cross_db_utils/datatypes.sql    #}

{# string  -------------------------------------------------     #}

{%- macro type_string() -%}
  {{ return(adapter.dispatch('type_string', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_string() %}
    string
{% endmacro %}

{% macro redshift__type_string() %}
    varchar
{% endmacro %}

{% macro postgres__type_string() %}
    varchar
{% endmacro %}

{% macro snowflake__type_string() %}
    varchar
{% endmacro %}


{# float  -------------------------------------------------     #}

{%- macro type_float() -%}
  {{ return(adapter.dispatch('type_float', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_float() %}
    float
{% endmacro %}

{% macro bigquery__type_float() %}
    float64
{% endmacro %}

{# numeric  ------------------------------------------------     #}

{%- macro type_numeric() -%}
  {{ return(adapter.dispatch('type_numeric', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_numeric() %}
    numeric(28, 6)
{% endmacro %}

{% macro bigquery__type_numeric() %}
    numeric
{% endmacro %}


{# bigint  -------------------------------------------------     #}

{%- macro type_bigint() -%}
  {{ return(adapter.dispatch('type_bigint', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_bigint() %}
    bigint
{% endmacro %}

{% macro bigquery__type_bigint() %}
    int64
{% endmacro %}

{# int  -------------------------------------------------     #}

{%- macro type_int() -%}
  {{ return(adapter.dispatch('type_int', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_int() %}
    int
{% endmacro %}

{% macro bigquery__type_int() %}
    int64
{% endmacro %}


{# bytes  -------------------------------------------------     #}

{%- macro type_binary() -%}
  {{ return(adapter.dispatch('type_binary', 'ethereum')()) }}
{%- endmacro -%}

{% macro default__type_binary() %}
    binary
{% endmacro %}

{% macro postgres__type_binary() %}
    bytea
{% endmacro %}
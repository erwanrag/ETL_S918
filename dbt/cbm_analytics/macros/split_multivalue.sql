{% macro split_multivalue_numeric(column_name, position) %}
    CASE 
        WHEN TRIM(SPLIT_PART({{ column_name }}::text, ';', {{ position }}))
             ~ '^-?[0-9]+\.?[0-9]*$'
        THEN TRIM(SPLIT_PART({{ column_name }}::text, ';', {{ position }}))::numeric
        ELSE NULL
    END
{% endmacro %}

{% macro split_multivalue_integer(column_name, position) %}
    CASE 
        WHEN TRIM(SPLIT_PART({{ column_name }}::text, ';', {{ position }}))
             ~ '^-?[0-9]+$'
        THEN TRIM(SPLIT_PART({{ column_name }}::text, ';', {{ position }}))::integer
        ELSE NULL
    END
{% endmacro %}

{% macro split_multivalue_text(column_name, position) %}
    NULLIF(TRIM(SPLIT_PART({{ column_name }}::text, ';', {{ position }})), '')
{% endmacro %}

{% macro explode_multivalue(column_name, prefix, max_items=6, type="numeric") %}
    {%- for i in range(1, max_items + 1) %}
        {%- if type == "numeric" %}
            {{ split_multivalue_numeric(column_name, i) }} as {{ prefix }}_{{ i }}
        {%- elif type == "integer" %}
            {{ split_multivalue_integer(column_name, i) }} as {{ prefix }}_{{ i }}
        {%- elif type == "text" %}
            {{ split_multivalue_text(column_name, i) }} as {{ prefix }}_{{ i }}
        {%- endif %}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{% endmacro %}
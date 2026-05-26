{% macro hash_md5(column) -%}
    UPPER(CONVERT(CHAR(32), HASHBYTES('MD5',
        UPPER(TRIM(ISNULL(CAST({{ column }} AS NVARCHAR(MAX)), '')))
    ), 2))
{%- endmacro %}


{% macro hash_md5_concat(columns) -%}
    UPPER(CONVERT(CHAR(32), HASHBYTES('MD5',
        CONCAT(
            {%- for col in columns %}
            UPPER(TRIM(ISNULL(CAST({{ col }} AS NVARCHAR(MAX)), '')))
            {%- if not loop.last %}, '||',{% endif %}
            {%- endfor %}
        )
    ), 2))
{%- endmacro %}

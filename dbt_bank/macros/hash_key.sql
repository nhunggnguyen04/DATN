{% macro hash_key(columns) %}
    lower(
        convert(varchar(64),
            hashbytes(
                'SHA2_256',
                concat(
                    '',
                    {%- for col in columns -%}
                        coalesce(cast({{ col }} as varchar(500)), '')
                        {%- if not loop.last -%}
                            , '|',
                        {%- endif -%}
                    {%- endfor -%}
                )
            ),
            2
        )
    )
{% endmacro %}
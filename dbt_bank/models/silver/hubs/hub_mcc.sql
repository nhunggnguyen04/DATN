{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_mcc',
        schema       = 'silver'
    )
}}

with source_data as (

    select distinct
        mcc_id
    from {{ source('bronze', 'mcc_codes_mns') }}
    where mcc_id is not null
      and operation_flag = 'I'

),

hub as (

    select
        {{ hash_md5('mcc_id') }} as hk_mcc,
        mcc_id,
        SYSUTCDATETIME()          as load_datetime,
        'bronze.mcc_codes_mns'    as record_source
    from source_data

),

final as (

    select h.*
    from hub h

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_mcc = h.hk_mcc
    )
    {% endif %}

)

select * from final

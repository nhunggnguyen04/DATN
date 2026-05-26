{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_customer',
        schema       = 'silver'
    )
}}

with source_data as (

    select distinct
        id as customer_id
    from {{ source('bronze', 'users_mns') }}
    where id is not null
      and operation_flag = 'I'

),

hub as (

    select
        {{ hash_md5('customer_id') }} as hk_customer,
        customer_id,
        SYSUTCDATETIME()              as load_datetime,
        'bronze.users_mns'            as record_source
    from source_data

),

final as (

    select h.*
    from hub h

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_customer = h.hk_customer
    )
    {% endif %}

)

select * from final

{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_merchant',
        schema       = 'silver'
    )
}}

-- merchant_id là BK, lấy từ transactions_tdy cho các record INSERT trong transactions_mns
-- Hub merchant không có bảng master riêng trong Bronze — BK được suy ra từ transactions

with mns_inserts as (

    select distinct
        id
    from {{ source('bronze', 'transactions_mns') }}
    where id is not null
      and operation_flag = 'I'

),

source_data as (

    select distinct
        t.merchant_id
    from {{ source('bronze', 'transactions_tdy') }} t
    inner join mns_inserts m
        on t.id = m.id
    where t.merchant_id is not null

),

hub as (

    select
        {{ hash_md5('merchant_id') }} as hk_merchant,
        merchant_id,
        SYSUTCDATETIME()               as load_datetime,
        'bronze.transactions_tdy'      as record_source
    from source_data

),

final as (

    select h.*
    from hub h

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_merchant = h.hk_merchant
    )
    {% endif %}

)

select * from final

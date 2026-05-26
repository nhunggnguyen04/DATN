{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_transaction',
        schema       = 'silver'
    )
}}

with source_data as (

    select distinct
        id as transaction_id
    from {{ source('bronze', 'transactions_mns') }}
    where id is not null
      and operation_flag = 'I'

),

hub as (

    select
        {{ hash_md5('transaction_id') }} as hk_transaction,
        transaction_id,
        SYSUTCDATETIME()                  as load_datetime,
        'bronze.transactions_mns'         as record_source
    from source_data

),

final as (

    select h.*
    from hub h

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_transaction = h.hk_transaction
    )
    {% endif %}

)

select * from final

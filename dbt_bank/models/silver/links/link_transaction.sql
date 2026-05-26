{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_transaction_link',
        schema       = 'silver'
    )
}}

-- Transactional link: giao dịch kết nối đồng thời customer, card, merchant, mcc
-- Một transaction là một sự kiện kinh doanh duy nhất → gộp 1 link thay vì 4 link riêng

with source_data as (

    select distinct
        t.id,
        t.client_id,
        t.card_id,
        t.merchant_id,
        t.mcc
    from {{ source('bronze', 'transactions_tdy') }} t
    inner join {{ source('bronze', 'transactions_mns') }} m
        on t.id = m.id
    where t.id is not null
      and m.operation_flag = 'I'

),

link as (

    select
        {{ hash_md5_concat(['id', 'client_id', 'card_id', 'merchant_id', 'mcc']) }} as hk_transaction_link,
        {{ hash_md5('id') }}          as hk_transaction,
        {{ hash_md5('client_id') }}   as hk_customer,
        {{ hash_md5('card_id') }}     as hk_card,
        {{ hash_md5('merchant_id') }} as hk_merchant,
        {{ hash_md5('mcc') }}         as hk_mcc,
        SYSUTCDATETIME()              as load_datetime,
        'bronze.transactions_tdy'     as record_source
    from source_data

),

final as (

    select l.*
    from link l

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_transaction_link = l.hk_transaction_link
    )
    {% endif %}

)

select * from final

{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_transaction',
        schema       = 'silver'
    )
}}

-- SCD Type 1: giao dịch bất biến sau khi xảy ra (MERGE sẽ UPDATE nếu có hiệu chỉnh)
-- merchant_city/state/zip đặt ở đây vì Bronze không có bảng merchants riêng;
-- location là context của transaction, không phải attribute cố định của merchant

with source_data as (

    select
        t.id,
        t.[date],
        t.amount,
        t.use_chip,
        t.merchant_city,
        t.merchant_state,
        t.zip,
        t.errors
    from {{ source('bronze', 'transactions_tdy') }} t
    inner join {{ source('bronze', 'transactions_mns') }} m
        on t.id = m.id
    where t.id is not null
      and m.operation_flag in ('I', 'U')

)

select
    {{ hash_md5('id') }}                            as hk_transaction,
    try_convert(datetime, [date])                   as transaction_datetime,
    try_cast(amount       as decimal(10,2))         as amount,
    cast(use_chip         as nvarchar(50))           as use_chip,
    cast(merchant_city    as nvarchar(100))          as merchant_city,
    cast(merchant_state   as nvarchar(50))           as merchant_state,
    cast(zip              as nvarchar(10))           as zip,
    cast(errors           as nvarchar(100))          as errors,
    SYSUTCDATETIME()                                as load_datetime,
    'bronze.transactions_tdy'                       as record_source
from source_data

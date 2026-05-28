{{
    config(
        materialized        = 'incremental',
        unique_key          = 'merchant_id',
        incremental_strategy = 'merge',
        schema              = 'gold',
        tags                = ['gold', 'dim']
    )
}}

-- dim_merchant không có Silver satellite riêng; location được suy ra từ sat_transaction_detail
-- Lấy merchant_city/state/zip từ giao dịch gần nhất (ROW_NUMBER ORDER BY transaction_datetime DESC)

with hub as (

    select
        hk_merchant,
        merchant_id
    from {{ ref('hub_merchant') }}

),

link as (

    select
        hk_merchant,
        hk_transaction
    from {{ ref('link_transaction') }}

),

sat as (

    select
        hk_transaction,
        transaction_datetime,
        merchant_city,
        merchant_state,
        zip
    from {{ ref('sat_transaction_detail') }}

),

ranked as (

    select
        h.merchant_id,
        h.hk_merchant,
        s.merchant_city,
        s.merchant_state,
        s.zip,
        ROW_NUMBER() OVER (
            PARTITION BY h.merchant_id
            ORDER BY s.transaction_datetime DESC
        ) AS rn
    from hub h
    inner join link lt on h.hk_merchant  = lt.hk_merchant
    inner join sat  s  on lt.hk_transaction = s.hk_transaction

),

final as (

    select
        merchant_id,
        hk_merchant,
        merchant_city,
        merchant_state,
        zip,
        SYSUTCDATETIME() AS dbt_updated_at
    from ranked
    where rn = 1

)

select * from final

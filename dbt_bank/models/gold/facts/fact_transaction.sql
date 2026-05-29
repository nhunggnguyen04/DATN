{{
    config(
        materialized        = 'incremental',
        unique_key          = 'transaction_id',
        incremental_strategy = 'delete+insert',
        schema              = 'gold',
        tags                = ['gold', 'fact']
    )
}}

-- Grain: 1 row = 1 giao dịch (157k rows)
-- Incremental: delete+insert theo transaction_datetime = run_date (idempotent)
-- Airflow gọi: dbt run --select tag:fact --vars '{"run_date":"2024-03-15"}'
-- Full refresh (lần đầu hoặc run_date rỗng): load toàn bộ không filter

with link as (

    select
        hk_transaction,
        hk_customer,
        hk_card,
        hk_merchant,
        hk_mcc
    from {{ ref('link_transaction') }}

),

std as (

    select
        hk_transaction,
        transaction_datetime,
        amount,
        use_chip,
        merchant_city,
        merchant_state,
        zip,
        errors
    from {{ ref('sat_transaction_detail') }}

),

-- Hub lookups: hash key → natural key
hub_txn as (
    select hk_transaction, transaction_id
    from {{ ref('hub_transaction') }}
),

hub_cust as (
    select hk_customer, customer_id
    from {{ ref('hub_customer') }}
),

hub_card as (
    select hk_card, card_id
    from {{ ref('hub_card') }}
),

hub_merch as (
    select hk_merchant, merchant_id
    from {{ ref('hub_merchant') }}
),

hub_mcc as (
    select hk_mcc, mcc_id
    from {{ ref('hub_mcc') }}
),

final as (

    select
        ht.transaction_id,
        CONVERT(INT, FORMAT(s.transaction_datetime, 'yyyyMMdd'))     AS date_key,
        hc.customer_id,
        hca.card_id,
        hm.merchant_id,
        hmcc.mcc_id,
        s.transaction_datetime,
        s.amount,
        s.use_chip,
        s.merchant_city,
        s.merchant_state,
        s.zip,
        s.errors,

        -- Computed: 1 = giao dịch có lỗi (chatbot và Power BI đọc cùng cột này)
        CASE
            WHEN s.errors IS NOT NULL AND LTRIM(RTRIM(s.errors)) <> ''
            THEN 1 ELSE 0
        END                                                           AS has_error,

        -- Computed: 1 = dùng chip EMV (CD2 risk analysis)
        CASE
            WHEN s.use_chip = 'Chip Transaction'
            THEN 1 ELSE 0
        END                                                           AS is_chip_used,

        SYSUTCDATETIME()                                              AS dbt_updated_at

    from link lt
    inner join std        s      on lt.hk_transaction = s.hk_transaction
    inner join hub_txn    ht     on lt.hk_transaction = ht.hk_transaction
    inner join hub_cust   hc     on lt.hk_customer    = hc.hk_customer
    inner join hub_card   hca    on lt.hk_card        = hca.hk_card
    inner join hub_merch  hm     on lt.hk_merchant    = hm.hk_merchant
    inner join hub_mcc    hmcc   on lt.hk_mcc         = hmcc.hk_mcc

)

select * from final

{% if is_incremental() %}
{% if var("run_date", "") != "" %}
where CAST(transaction_datetime AS DATE) = CAST('{{ var("run_date", "") }}' AS DATE)
{% endif %}
{% endif %}

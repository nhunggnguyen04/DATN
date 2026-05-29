{{
    config(
        materialized        = 'incremental',
        unique_key          = 'customer_id',
        incremental_strategy = 'merge',
        schema              = 'gold',
        tags                = ['gold', 'dim']
    )
}}

with hub as (

    select
        hk_customer,
        customer_id
    from {{ ref('hub_customer') }}

),

sat as (

    -- SCD Type 2 — chỉ lấy record active hiện tại
    select
        hk_customer,
        gender,
        address,
        yearly_income,
        credit_score,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        latitude,       -- nvarchar(20) trong Silver
        longitude,      -- nvarchar(20) trong Silver
        per_capita_income,
        total_debt,
        num_credit_cards
    from {{ ref('sat_customer_profile') }}
    where effective_to = '9999-12-31 00:00:00.0000000'

),

final as (

    select
        h.hk_customer,
        h.customer_id,
        s.gender,
        s.address,
        s.yearly_income,
        s.credit_score,
        s.current_age,
        s.retirement_age,
        s.birth_year,
        s.birth_month,
        TRY_CAST(s.latitude  AS DECIMAL(10, 6)) AS latitude,
        TRY_CAST(s.longitude AS DECIMAL(10, 6)) AS longitude,
        s.per_capita_income,
        s.total_debt,
        s.num_credit_cards,

        -- Computed: tỷ lệ nợ / thu nhập (core metric cho CD2)
        s.total_debt / NULLIF(s.yearly_income, 0)   AS debt_to_income_ratio,

        -- Computed: phân khúc thu nhập (CD1 marketing)
        CASE
            WHEN s.yearly_income < 30000  THEN 'Low'
            WHEN s.yearly_income < 60000  THEN 'Medium'
            WHEN s.yearly_income < 100000 THEN 'High'
            ELSE 'Premium'
        END                                         AS income_segment,

        -- Computed: hạng rủi ro tín dụng (CD1 up-sell + CD2 hàng rào)
        CASE
            WHEN s.credit_score >= 750 THEN 'Excellent'
            WHEN s.credit_score >= 670 THEN 'Good'
            WHEN s.credit_score >= 580 THEN 'Fair'
            ELSE 'Poor'
        END                                         AS credit_risk_tier,

        -- Computed: số năm đến khi nghỉ hưu (CD1 insurance targeting)
        s.retirement_age - s.current_age           AS years_to_retirement,

        SYSUTCDATETIME()                            AS dbt_updated_at

    from hub h
    inner join sat s on h.hk_customer = s.hk_customer

)

select * from final

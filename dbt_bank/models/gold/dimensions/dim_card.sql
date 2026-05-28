{{
    config(
        materialized        = 'incremental',
        unique_key          = 'card_id',
        incremental_strategy = 'merge',
        schema              = 'gold',
        tags                = ['gold', 'dim']
    )
}}

with hub_card as (

    select
        hk_card,
        card_id
    from {{ ref('hub_card') }}

),

sat as (

    -- SCD Type 2 — chỉ lấy record active hiện tại
    -- card_number và cvv bị loại bỏ (dữ liệu nhạy cảm PCI)
    select
        hk_card,
        card_brand,
        card_type,
        credit_limit,
        expires,
        has_chip,
        num_cards_issued,
        acct_open_date,
        year_pin_last_changed
    from {{ ref('sat_card_detail') }}
    where effective_to = '9999-12-31 00:00:00.0000000'

),

lnk as (

    select
        hk_card,
        hk_customer
    from {{ ref('link_customer_card') }}

),

hub_customer as (

    select
        hk_customer,
        customer_id
    from {{ ref('hub_customer') }}

),

final as (

    select
        hc.hk_card,
        hc.card_id,

        -- Denormalized outrigger: Power BI dùng để link dim_card ↔ dim_customer
        cu.customer_id,

        s.card_brand,
        s.card_type,
        s.credit_limit,
        s.expires,
        s.has_chip,
        s.num_cards_issued,
        s.acct_open_date,
        s.year_pin_last_changed,

        -- Computed: tuổi thẻ tính theo năm
        DATEDIFF(YEAR, s.acct_open_date, GETDATE())  AS card_age_years,

        -- Computed: số năm chưa đổi PIN (CD2 risk indicator)
        YEAR(GETDATE()) - s.year_pin_last_changed    AS pin_age_years,

        SYSUTCDATETIME()                             AS dbt_updated_at

    from hub_card hc
    inner join sat          s  on hc.hk_card      = s.hk_card
    inner join lnk             on hc.hk_card       = lnk.hk_card
    inner join hub_customer cu on lnk.hk_customer  = cu.hk_customer

)

select * from final

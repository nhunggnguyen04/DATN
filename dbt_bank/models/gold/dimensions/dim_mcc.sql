{{
    config(
        materialized        = 'incremental',
        unique_key          = 'mcc_id',
        incremental_strategy = 'merge',
        schema              = 'gold',
        tags                = ['gold', 'dim']
    )
}}

with hub as (

    select
        hk_mcc,
        mcc_id
    from {{ ref('hub_mcc') }}

),

sat as (

    select
        hk_mcc,
        description
    from {{ ref('sat_mcc_detail') }}

),

final as (

    select
        h.mcc_id,
        h.hk_mcc,
        s.description   AS mcc_description,
        SYSUTCDATETIME() AS dbt_updated_at
    from hub h
    inner join sat s on h.hk_mcc = s.hk_mcc

)

select * from final

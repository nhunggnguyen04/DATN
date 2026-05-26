{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_customer_card',
        schema       = 'silver'
    )
}}

-- Link: customer ↔ card
-- BK của card là cards_mns.id (consistent với hub_card)
-- BK của customer là cards_mns.client_id (consistent với hub_customer)

with source_data as (

    select distinct
        client_id,
        id
    from {{ source('bronze', 'cards_mns') }}
    where client_id is not null
      and id is not null
      and operation_flag = 'I'

),

link as (

    select
        {{ hash_md5_concat(['client_id', 'id']) }} as hk_customer_card,
        {{ hash_md5('client_id') }}                 as hk_customer,
        {{ hash_md5('id') }}                        as hk_card,
        SYSUTCDATETIME()                            as load_datetime,
        'bronze.cards_mns'                          as record_source
    from source_data

),

final as (

    select l.*
    from link l

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_customer_card = l.hk_customer_card
    )
    {% endif %}

)

select * from final

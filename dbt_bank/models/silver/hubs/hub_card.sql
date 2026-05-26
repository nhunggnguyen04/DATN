{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_card',
        schema       = 'silver'
    )
}}

-- BK = cards_mns.id (surrogate từ source), không dùng card_number để tránh join TDY

with source_data as (

    select distinct
        id as card_id
    from {{ source('bronze', 'cards_mns') }}
    where id is not null
      and operation_flag = 'I'

),

hub as (

    select
        {{ hash_md5('card_id') }} as hk_card,
        card_id,
        SYSUTCDATETIME()          as load_datetime,
        'bronze.cards_mns'        as record_source
    from source_data

),

final as (

    select h.*
    from hub h

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_card = h.hk_card
    )
    {% endif %}

)

select * from final

{%- set close_records -%}
{%-  if is_incremental() %}
UPDATE t
SET    t.effective_to = SYSUTCDATETIME()
FROM   {{ this }} AS t
INNER JOIN (
    SELECT
        {{ hash_md5('id') }} AS hk_card,
        {{ hash_md5_concat([
            'card_brand', 'card_type', 'card_number', 'expires', 'cvv',
            'has_chip', 'num_cards_issued', 'credit_limit',
            'acct_open_date', 'year_pin_last_changed'
        ]) }} AS new_hashdiff,
        operation_flag
    FROM  {{ source('bronze', 'cards_mns') }}
    WHERE operation_flag IN ('U', 'D')
) AS src
   ON  src.hk_card    = t.hk_card
  AND  t.effective_to = '9999-12-31 00:00:00.0000000'
  AND  (src.operation_flag = 'D' OR src.new_hashdiff <> t.hashdiff)
{%-  else %}
SELECT 1 WHERE 1 = 0
{%-  endif %}
{%- endset -%}

{{
    config(
        materialized = 'incremental',
        unique_key   = ['hk_card', 'effective_from'],
        schema       = 'silver',
        pre_hook     = [close_records]
    )
}}

-- SCD Type 2: lịch sử thay đổi thông tin thẻ (credit_limit, expires, v.v.)
-- LƯU Ý: KHÔNG chứa client_id — quan hệ card-customer nằm trong link_customer_card

with source_data as (

    select
        id,
        card_brand,
        card_type,
        card_number,
        expires,
        cvv,
        has_chip,
        num_cards_issued,
        credit_limit,
        acct_open_date,
        year_pin_last_changed
    from {{ source('bronze', 'cards_mns') }}
    where id is not null
      and operation_flag in ('I', 'U')

),

staged as (

    select
        {{ hash_md5('id') }}                                        as hk_card,
        SYSUTCDATETIME()                                            as effective_from,
        cast('9999-12-31 00:00:00.0000000' as datetime2)            as effective_to,
        {{ hash_md5_concat([
            'card_brand', 'card_type', 'card_number', 'expires', 'cvv',
            'has_chip', 'num_cards_issued', 'credit_limit',
            'acct_open_date', 'year_pin_last_changed'
        ]) }}                                                       as hashdiff,
        cast(card_brand                as nvarchar(50))             as card_brand,
        cast(card_type                 as nvarchar(50))             as card_type,
        cast(card_number               as nvarchar(20))             as card_number,
        try_cast(expires               as date)                     as expires,
        cast(cvv                       as nvarchar(10))             as cvv,
        cast(has_chip                  as nvarchar(10))             as has_chip,
        try_cast(num_cards_issued      as int)                      as num_cards_issued,
        try_cast(credit_limit          as decimal(12,2))            as credit_limit,
        try_cast(acct_open_date        as date)                     as acct_open_date,
        try_cast(year_pin_last_changed as int)                      as year_pin_last_changed,
        'bronze.cards_mns'                                          as record_source
    from source_data

),

final as (

    select s.*
    from staged s

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_card    = s.hk_card
          and t.hashdiff   = s.hashdiff
          and t.effective_to = '9999-12-31 00:00:00.0000000'
    )
    {% endif %}

)

select * from final

{%- set close_records -%}
{%-  if is_incremental() %}
UPDATE t
SET    t.effective_to = SYSUTCDATETIME()
FROM   {{ this }} AS t
INNER JOIN (
    SELECT
        {{ hash_md5('id') }} AS hk_customer,
        {{ hash_md5_concat([
            'current_age', 'retirement_age', 'birth_year', 'birth_month',
            'gender', 'address', 'latitude', 'longitude',
            'per_capita_income', 'yearly_income', 'total_debt',
            'credit_score', 'num_credit_cards'
        ]) }} AS new_hashdiff,
        operation_flag
    FROM  {{ source('bronze', 'users_mns') }}
    WHERE operation_flag IN ('U', 'D')
) AS src
   ON  src.hk_customer = t.hk_customer
  AND  t.effective_to  = '9999-12-31 00:00:00.0000000'
  AND  (src.operation_flag = 'D' OR src.new_hashdiff <> t.hashdiff)
{%-  else %}
SELECT 1 WHERE 1 = 0
{%-  endif %}
{%- endset -%}

{{
    config(
        materialized = 'incremental',
        unique_key   = ['hk_customer', 'effective_from'],
        schema       = 'silver',
        pre_hook     = [close_records]
    )
}}

-- SCD Type 2: lịch sử thay đổi thông tin khách hàng
-- Pre_hook đóng record cũ (effective_to = now) khi flag U có hashdiff thay đổi hoặc flag D
-- Main SELECT chỉ INSERT records mới khi (hk_customer, hashdiff) active chưa tồn tại

with source_data as (

    select
        id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        address,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards
    from {{ source('bronze', 'users_mns') }}
    where id is not null
      and operation_flag in ('I', 'U')

),

staged as (

    select
        {{ hash_md5('id') }}                                        as hk_customer,
        SYSUTCDATETIME()                                            as effective_from,
        cast('9999-12-31 00:00:00.0000000' as datetime2)            as effective_to,
        {{ hash_md5_concat([
            'current_age', 'retirement_age', 'birth_year', 'birth_month',
            'gender', 'address', 'latitude', 'longitude',
            'per_capita_income', 'yearly_income', 'total_debt',
            'credit_score', 'num_credit_cards'
        ]) }}                                                       as hashdiff,
        try_cast(current_age       as int)                          as current_age,
        try_cast(retirement_age    as int)                          as retirement_age,
        try_cast(birth_year        as int)                          as birth_year,
        try_cast(birth_month       as int)                          as birth_month,
        cast(gender                as nvarchar(20))                 as gender,
        cast(address               as nvarchar(255))                as address,
        cast(latitude              as nvarchar(20))                 as latitude,
        cast(longitude             as nvarchar(20))                 as longitude,
        try_cast(per_capita_income as decimal(12,2))                as per_capita_income,
        try_cast(yearly_income     as decimal(12,2))                as yearly_income,
        try_cast(total_debt        as decimal(12,2))                as total_debt,
        try_cast(credit_score      as int)                          as credit_score,
        try_cast(num_credit_cards  as int)                          as num_credit_cards,
        'bronze.users_mns'                                          as record_source
    from source_data

),

final as (

    select s.*
    from staged s

    {% if is_incremental() %}
    -- Bỏ qua nếu đã có record active với cùng (hk_customer, hashdiff)
    -- → tránh duplicate khi pipeline chạy lại, và tránh insert khi không có thay đổi thực sự
    where not exists (
        select 1
        from {{ this }} t
        where t.hk_customer = s.hk_customer
          and t.hashdiff     = s.hashdiff
          and t.effective_to = '9999-12-31 00:00:00.0000000'
    )
    {% endif %}

)

select * from final

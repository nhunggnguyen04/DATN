{{ config(
    materialized = 'incremental',
    unique_key = 'hk_customer'
) }}

with source_data as (

    select distinct
        cast(id as varchar(100)) as customer_id
    from {{ source('bronze', 'users_mns') }}
    where id is not null
      and operation_flag in ('I', 'U')

),

hub_customer as (

    select
        {{ hash_key(['customer_id']) }} as hk_customer,
        customer_id,
        current_timestamp as load_datetime
    from source_data

)

select *
from hub_customer

{% if is_incremental() %}
where hk_customer not in (
    select hk_customer
    from {{ this }}
)
{% endif %}
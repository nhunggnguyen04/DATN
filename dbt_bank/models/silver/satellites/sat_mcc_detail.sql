{{
    config(
        materialized = 'incremental',
        unique_key   = 'hk_mcc',
        schema       = 'silver'
    )
}}

-- SCD Type 1: mô tả MCC code — MERGE sẽ UPDATE description nếu thay đổi
-- MCC codes hiếm khi thay đổi; flag D từ Bronze bị bỏ qua (không xóa ở Silver)

with source_data as (

    select
        m.mcc_id,
        t.[description]
    from {{ source('bronze', 'mcc_codes_mns') }} m
    inner join {{ source('bronze', 'mcc_codes_tdy') }} t on t.mcc_id = m.mcc_id
    where m.mcc_id is not null
      and m.operation_flag in ('I', 'U')

)

select
    {{ hash_md5('mcc_id') }}                        as hk_mcc,
    cast([description] as nvarchar(255))            as description,
    SYSUTCDATETIME()                                as load_datetime,
    'bronze.mcc_codes_mns'                          as record_source
from source_data

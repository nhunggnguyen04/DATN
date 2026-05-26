{{
    config(materialized = 'table')
}}

-- Reference table MCC codes — full refresh mỗi lần chạy
-- Source: bronze.mcc_codes_tdy (complete snapshot từ banking.mcc_codes)

with source_data as (

    select
        try_cast(mcc_id     as int)         as mcc_code,
        cast([description]  as varchar(200)) as description
    from {{ source('bronze', 'mcc_codes_tdy') }}
    where mcc_id is not null

)

select
    mcc_code,
    description,
    -- Phân loại MCC code theo dải số (chuẩn ISO 18245)
    case
        when mcc_code between 1  and 1499 then 'Agriculture & Mining'
        when mcc_code between 1500 and 2999 then 'Contractors & Construction'
        when mcc_code between 3000 and 3499 then 'Airlines & Travel'
        when mcc_code between 3500 and 3999 then 'Car Rental'
        when mcc_code between 4000 and 4799 then 'Transportation'
        when mcc_code between 4800 and 4999 then 'Utilities & Telecom'
        when mcc_code between 5000 and 5199 then 'Wholesale'
        when mcc_code between 5200 and 5999 then 'Retail Stores'
        when mcc_code between 6000 and 6299 then 'Financial & Banking'
        when mcc_code between 6300 and 6399 then 'Insurance'
        when mcc_code between 7000 and 7299 then 'Personal Services'
        when mcc_code between 7300 and 7399 then 'Business Services'
        when mcc_code between 7500 and 7999 then 'Automotive & Recreation'
        when mcc_code between 8000 and 8099 then 'Healthcare'
        when mcc_code between 8100 and 8299 then 'Legal & Educational'
        when mcc_code between 8300 and 8999 then 'Other Services'
        when mcc_code between 9000 and 9999 then 'Government & Non-Profit'
        else 'Uncategorized'
    end                                     as category,
    current_timestamp                       as _loaded_at
from source_data

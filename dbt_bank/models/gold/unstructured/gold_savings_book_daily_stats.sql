{{ config(materialized='table') }}

with s as (

    select
        run_date,
        entity_id,
        file_size_bytes,
        has_ocr_text
    from {{ ref('silver_savings_book_documents') }}

),

agg as (

    select
        run_date,
        count(*) as document_count,
        count(distinct entity_id) as distinct_user_count,
        sum(cast(file_size_bytes as bigint)) as total_bytes,
        avg(cast(file_size_bytes as float)) as avg_bytes,
        min(cast(file_size_bytes as bigint)) as min_bytes,
        max(cast(file_size_bytes as bigint)) as max_bytes,
        sum(cast(has_ocr_text as int)) as with_ocr_text_count
    from s
    group by run_date

)

select *
from agg

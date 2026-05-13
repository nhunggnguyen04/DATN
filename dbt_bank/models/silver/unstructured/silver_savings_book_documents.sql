{{ config(materialized='table') }}

with docs as (

    select
        document_id,
        entity_type,
        entity_id,
        doc_type,
        file_path,
        file_format,
        created_at,
        source,
        sha256,
        file_size_bytes,
        ocr_text,
        run_date
    from {{ ref('bronze_savings_book_documents') }}

),

final as (

    select
        {{ hash_key(["entity_type", "cast(entity_id as varchar(100))", "doc_type"]) }} as hk_document_bk,
        {{ hash_key(["document_id"]) }} as hk_document_id,
        document_id,
        entity_type,
        entity_id,
        doc_type,
        file_path,
        file_format,
        created_at,
        source,
        sha256,
        file_size_bytes,
        cast(file_size_bytes as float) / 1024.0 as file_size_kb,
        ocr_text,
        case when ocr_text is null then 0 else 1 end as has_ocr_text,
        run_date,
        current_timestamp as load_datetime
    from docs

)

select *
from final

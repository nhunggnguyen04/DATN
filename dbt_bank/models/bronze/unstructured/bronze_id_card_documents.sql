{{ config(materialized='table') }}

with src as (

    select
        cast(document_id as varchar(36)) as document_id,
        cast(entity_type as varchar(50)) as entity_type,
        try_cast(entity_id as int) as entity_id,
        cast(doc_type as varchar(50)) as doc_type,
        cast(file_path as varchar(500)) as file_path,
        cast(file_format as varchar(20)) as file_format,
        try_convert(datetime2, replace(cast(created_at as varchar(40)), 'Z', ''), 126) as created_at,
        cast(source as varchar(50)) as source,
        cast(sha256 as varchar(64)) as sha256,
        try_cast(file_size_bytes as bigint) as file_size_bytes,
        nullif(cast(ocr_text as varchar(max)), '') as ocr_text,
        try_cast(run_date as date) as run_date
    from {{ ref('unstructured_documents_manifest') }}

)

select *
from src
where document_id is not null
  and entity_id is not null
  and doc_type is not null
  and file_path is not null
  and lower(doc_type) = 'id_card'

-- Create Bronze tables for unstructured document metadata
-- Run once on TARGET database

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

IF OBJECT_ID('bronze.documents_tdy', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.documents_tdy (
        document_id        VARCHAR(36)    NOT NULL,
        entity_type        VARCHAR(50)    NOT NULL,
        entity_id          BIGINT         NOT NULL,
        doc_type           VARCHAR(50)    NOT NULL,
        file_path          VARCHAR(512)   NOT NULL,
        file_format        VARCHAR(10)    NOT NULL,
        created_at         DATETIME2(0)   NULL,
        source             VARCHAR(50)    NULL,
        sha256             CHAR(64)       NULL,
        file_size_bytes    BIGINT         NULL,
        ocr_text           NVARCHAR(MAX)  NULL,
        run_date           DATE           NULL
    );
END
GO

IF OBJECT_ID('bronze.documents_pdy', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.documents_pdy (
        document_id        VARCHAR(36)    NOT NULL,
        entity_type        VARCHAR(50)    NOT NULL,
        entity_id          BIGINT         NOT NULL,
        doc_type           VARCHAR(50)    NOT NULL,
        file_path          VARCHAR(512)   NOT NULL,
        file_format        VARCHAR(10)    NOT NULL,
        created_at         DATETIME2(0)   NULL,
        source             VARCHAR(50)    NULL,
        sha256             CHAR(64)       NULL,
        file_size_bytes    BIGINT         NULL,
        ocr_text           NVARCHAR(MAX)  NULL,
        run_date           DATE           NULL
    );
END
GO

IF OBJECT_ID('bronze.documents_mns', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.documents_mns (
        document_id      VARCHAR(36) NOT NULL,
        operation_flag   CHAR(1)     NOT NULL
    );
END
GO

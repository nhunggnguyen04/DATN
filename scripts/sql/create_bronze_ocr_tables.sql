-- ============================================================
-- DATABASE: bank_dwh
-- SCHEMA: bronze
-- SQL Server
-- ============================================================

SET NOCOUNT ON;
GO

---------------------------------------------------------------
-- Create schema if not exists
---------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'bronze'
)
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

---------------------------------------------------------------
-- TABLE 1: CCCD OCR RESULTS
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.id_card_results;
GO

CREATE TABLE bronze.id_card_results (
    id BIGINT IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_id_card_results PRIMARY KEY,

    -- Source tracking
    [file] NVARCHAR(200) NULL,
    file_path NVARCHAR(500) NULL,
    run_date DATE NULL,
    user_id INT NULL,

    -- Extracted fields
    full_name NVARCHAR(300) NULL,
    id_number NVARCHAR(50) NULL,
    date_of_birth NVARCHAR(20) NULL,
    sex NVARCHAR(20) NULL,
    nationality NVARCHAR(100) NULL,
    place_of_origin NVARCHAR(300) NULL,
    place_of_residence NVARCHAR(500) NULL,
    issue_date NVARCHAR(20) NULL,
    expiry_date NVARCHAR(20) NULL,

    -- Confidence
    final_confidence FLOAT NULL,
    ocr_confidence FLOAT NULL,
    parse_confidence FLOAT NULL,
    plausible_fields INT NULL,

    -- Audit
    _loaded_at DATETIME2 NOT NULL
        CONSTRAINT DF_id_card_results_loaded_at
        DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_id_card_results_run_date
    ON bronze.id_card_results(run_date);
GO

CREATE INDEX IX_id_card_results_user_id
    ON bronze.id_card_results(user_id);
GO

---------------------------------------------------------------
-- TABLE 2: SAVINGS BOOK OCR RESULTS
-- Source: savings_book_roi_extractions_*.csv
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.savings_book_results;
GO

CREATE TABLE bronze.savings_book_results (
    id                  BIGINT IDENTITY(1,1) NOT NULL
                            CONSTRAINT PK_savings_book_results PRIMARY KEY,

    -- Source tracking
    [file]              NVARCHAR(200)   NULL,   -- tên file ảnh (vd: savings_book_scan.jpg)
    file_path           NVARCHAR(500)   NULL,   -- đường dẫn tuyệt đối đến ảnh gốc
    run_date            DATE            NULL,   -- ngày chạy pipeline
    user_id             INT             NULL,   -- trích từ path user_id=<n>

    -- Thông tin trích xuất từ sổ tiết kiệm
    transaction_date    NVARCHAR(20)    NULL,   -- ngày giao dịch, DD/MM/YYYY
    description         NVARCHAR(300)   NULL,   -- mô tả giao dịch, vd: 'Account opening'
    transaction_code    NVARCHAR(20)    NULL,   -- mã giao dịch, vd: 'OPN'
    transaction_amount  NVARCHAR(50)    NULL,   -- số tiền giao dịch, giữ string (vd: '117,136')
    balance             NVARCHAR(50)    NULL,   -- số dư, giữ string ở bronze
    interest_rate       NVARCHAR(20)    NULL,   -- lãi suất, giữ string ở bronze
    signature           NVARCHAR(200)   NULL,   -- tên người ký

    -- Confidence scores [0.0 – 1.0]
    final_confidence    FLOAT           NULL,
    ocr_confidence      FLOAT           NULL,
    parse_confidence    FLOAT           NULL,
    plausible_fields    INT             NULL,   -- số trường hợp lệ

    -- Audit
    _loaded_at          DATETIME2       NOT NULL
                            CONSTRAINT DF_savings_book_results_loaded_at
                            DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_savings_book_results_run_date
    ON bronze.savings_book_results(run_date);
GO

CREATE INDEX IX_savings_book_results_user_id
    ON bronze.savings_book_results(user_id);
GO
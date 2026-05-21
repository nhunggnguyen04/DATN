-- =====================================================
-- Bronze Unstructured Tables (Simplified)
-- For CCCD and Savings Book OCR results
-- =====================================================

-- -----------------------------------------------------
-- Table: bronze.id_card_results
-- -----------------------------------------------------
CREATE TABLE bronze.id_card_results (
    id INT IDENTITY(1,1) PRIMARY KEY,
    document_id NVARCHAR(100) UNIQUE NOT NULL,
    file_path NVARCHAR(500) NOT NULL,
    run_date DATE NOT NULL,
    user_id INT NULL,

    -- OCR metadata
    ocr_engine NVARCHAR(50) DEFAULT 'paddleocr',
    ocr_lang NVARCHAR(10) DEFAULT 'vi',
    ocr_avg_score DECIMAL(5,4) NULL,
    ocr_raw_text NVARCHAR(MAX) NULL,

    -- Extracted fields (CCCD)
    full_name NVARCHAR(200) NULL,
    id_number NVARCHAR(50) NULL,
    date_of_birth DATE NULL,
    sex NVARCHAR(10) NULL,
    nationality NVARCHAR(50) NULL,
    place_of_origin NVARCHAR(200) NULL,
    place_of_residence NVARCHAR(200) NULL,
    issue_date DATE NULL,
    expiry_date DATE NULL,

    -- Quality metrics
    extraction_confidence DECIMAL(5,4) NULL,
    processed_at DATETIME2 DEFAULT GETDATE(),

    -- Status tracking
    status NVARCHAR(20) DEFAULT 'ok',  -- 'ok', 'error', 'low_confidence'
    error_message NVARCHAR(500) NULL
);

-- Indexes for id_card_results
CREATE INDEX idx_id_card_run_date ON bronze.id_card_results (run_date);
CREATE INDEX idx_id_card_user_id ON bronze.id_card_results (user_id);
CREATE INDEX idx_id_card_status ON bronze.id_card_results (status);
CREATE INDEX idx_id_card_id_number ON bronze.id_card_results (id_number);
CREATE INDEX idx_id_card_document_id ON bronze.id_card_results (document_id);

-- -----------------------------------------------------
-- Table: bronze.savings_book_results
-- -----------------------------------------------------
CREATE TABLE bronze.savings_book_results (
    id INT IDENTITY(1,1) PRIMARY KEY,
    document_id NVARCHAR(100) UNIQUE NOT NULL,
    file_path NVARCHAR(500) NOT NULL,
    run_date DATE NOT NULL,
    user_id INT NULL,

    ocr_engine NVARCHAR(50) DEFAULT 'paddleocr',
    ocr_lang NVARCHAR(10) DEFAULT 'vi',
    ocr_avg_score DECIMAL(5,4) NULL,
    ocr_raw_text NVARCHAR(MAX) NULL,

    -- Extracted fields (Savings Book)
    account_number NVARCHAR(50) NULL,
    account_holder NVARCHAR(200) NULL,
    account_type NVARCHAR(100) NULL,
    opening_date DATE NULL,
    balance DECIMAL(18,2) NULL,
    interest_rate DECIMAL(5,2) NULL,

    extraction_confidence DECIMAL(5,4) NULL,
    processed_at DATETIME2 DEFAULT GETDATE(),
    status NVARCHAR(20) DEFAULT 'ok',
    error_message NVARCHAR(500) NULL
);

-- Indexes for savings_book_results
CREATE INDEX idx_savings_run_date ON bronze.savings_book_results (run_date);
CREATE INDEX idx_savings_account_number ON bronze.savings_book_results (account_number);
CREATE INDEX idx_savings_status ON bronze.savings_book_results (status);
CREATE INDEX idx_savings_document_id ON bronze.savings_book_results (document_id);

-- -----------------------------------------------------
-- Foreign Keys (Optional - only if hub_user exists)
-- -----------------------------------------------------
-- ALTER TABLE bronze.id_card_results
--     ADD CONSTRAINT fk_id_card_user
--     FOREIGN KEY (user_id) REFERENCES bronze.hub_user(user_id);

-- ALTER TABLE bronze.savings_book_results
--     ADD CONSTRAINT fk_savings_user
--     FOREIGN KEY (user_id) REFERENCES bronze.hub_user(user_id);

PRINT 'Bronze unstructured tables created successfully';

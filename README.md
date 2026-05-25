# DATN - Banking Data Pipeline

Dự án xây dựng pipeline dữ liệu ngân hàng, xử lý cả **dữ liệu có cấu trúc** (Bronze → Silver → Gold với Airflow + dbt) và **dữ liệu phi cấu trúc** (OCR extraction đơn giản).

## Mục tiêu

- **Structured Data**: ETL từ SQL Server → Data Vault (Silver) → Star Schema (Gold) với Airflow + dbt
- **Unstructured Data**: OCR 2 loại tài liệu (CCCD, Sổ tiết kiệm) → Bronze tables trực tiếp
- Pipeline đơn giản, dễ hiểu, phù hợp với đồ án sinh viên

## Kiến trúc tổng quan

```
SOURCE (SQL Server + Ảnh)
         │
         ├──────────────┬──────────────┐
         ▼              ▼              ▼
┌─────────────────────────────────────────────┐
│           BRONZE LAYER                       │
├─────────────────────────────────────────────┤
│ Structured:                                │
│   • users_tdy/pdy/mns                      │
│   • cards_tdy/pdy/mns                      │
│   • transactions_tdy/pdy/mns               │
│   • mcc_codes                              │
│                                            │
│ Unstructured (Simple):                     │
│   • id_card_results                        │
│   • savings_book_results                   │
└─────────────────┬──────────┬───────────────┘
                  │          │
                  │          ▼
                  │   Direct Queries (Simple)
                  │   
                  ▼
┌─────────────────────────────────────────────┐
│    SILVER LAYER (Structured Only)           │
│        Data Vault Model                     │
├─────────────────────────────────────────────┤
│         GOLD LAYER                          │
│       Star Schema (Dimensional)             │
├─────────────────────────────────────────────┤
│        CONSUMPTION                          │
│    Power BI / Analytics                     │
└─────────────────────────────────────────────┘
```

**Unstructured Pipeline (Simple - Bronze Only):**

```
Images (CCCD, Savings Book)
         │
         ▼
   PaddleOCR Extraction
         │
         ▼
    Load to Bronze Tables
         │
         ▼
   Direct SQL Queries
         │
    (No Airflow, No dbt,
     No TDY/PDY/MNS)
```

---

## 1. Structured Data Pipeline (Full ETL)

### Kiến trúc

```
SQL Server (source)
     │
     ▼
Bronze (TDY/PDY/MNS) ───→ pd_date, ins_date, del_date flags
     │
     ▼
Silver (Data Vault)
 ├─ hubs (business keys)
 ├─ links (relationships)
 └─ satellites (attributes + history)
     │
     ▼
Gold (Star Schema)
 ├─ dim_user, dim_card, dim_date, dim_mcc
 └─ fact_transactions
     │
     ▼
Power BI / Analytics
```

### Incremental Logic (TDY/PDY/MNS)

```
PDY (Previous Day)  ←  TDY (Today)  →  MNS (Move-New-Stable)

Process:
1. Clear MNS tables
2. Copy TDY → PDY
3. Extract source new → TDY
4. Compare TDY vs PDY → Compute MNS (I/U/D)
5. dbt transforms from MNS → Silver → Gold
```

---

## 2. Unstructured Data Pipeline (Simple - Bronze Only)

### Kiến trúc

```
Input Directory Structure:
data/unstructured/documents/
├── doc_type=id_card/
│   └── run_date=YYYY-MM-DD/
│       └── user_id=1/
│           └── image1.jpg
└── doc_type=savings_book/
    └── run_date=YYYY-MM-DD/
        └── user_id=1/
            └── image1.jpg

Pipeline:
Images → OCR Extraction → CSV → Load Bronze Tables

Tech:
- PaddleOCR (tiếng Việt)
- Python scripts (không dùng Airflow)
- 2 bronze tables:
  1. bronze.id_card_results
  2. bronze.savings_book_results
```

### OCR Extraction Details

#### CCCD Fields

**Label mapping**:
```
FULL NAME → full_name
ID NO → id_number (12 digits)
DATE OF BIRTH → date_of_birth
SEX → sex (Nam/Nữ)
NATIONALITY → nationality
PLACE OF ORIGIN → place_of_origin
PLACE OF RESIDENCE → place_of_residence
ISSUE DATE → issue_date
EXPIRY DATE → expiry_date
```

**Post-processing**:
- Dates: normalize to `YYYY-MM-DD` or `dd/mm/yyyy`
- Sex: normalize to `Nam` / `Nữ`
- ID number: extract digits only (12 digits)
- Strip whitespace, fix OCR errors

#### Savings Book Fields

```
SỐ TÀI KHOẢN → account_number
CHỦ TÀI KHOẢN → account_holder
LOẠI TÀI KHOẢN → account_type
NGÀY MỞ SỔ → opening_date
SỐ DƯ → balance (float)
LÃI SUẤT → interest_rate (float %)
```

**Post-processing**:
- Balance: remove currency symbols (đ, VNĐ, ₫), convert to float
- Interest rate: remove `%`, convert to float

---

## 3. Công nghệ

| Thành phần | Structured Pipeline | Unstructured Pipeline |
|------------|---------------------|------------------------|
| Orchestration | Apache Airflow | Python scripts (simple) |
| Transform | dbt (Silver→Gold) | Direct → Bronze |
| Database | SQL Server (Bronze/Silver/Gold) | SQL Server (Bronze only) |
| OCR | — | PaddleOCR + PaddlePaddle |
| Extract | Python + pyodbc | Python + PaddleOCR |
| Environment | .venv | .venv_ocr (separate) |

---

## 4. Cấu trúc thư mục

```
DATN/
├── dags/                          # Airflow DAGs (chỉ structured)
│   └── banking_pipeline_dag.py
├── scripts/
│   ├── extract/
│   │   ├── ocr_extract_id_card.py          # Simple OCR CCCD
│   │   ├── ocr_extract_savings_book.py    # Simple OCR Sổ tiết kiệm
│   │   ├── load_bronze_simple.py           # Load CSV → Bronze (simple)
│   │   ├── run_simple_pipeline.py          # Orchestrator (simple)
│   │   │
│   │   # Structured pipeline (giữ nguyên)
│   │   ├── load_bronze_users.py
│   │   ├── load_bronze_cards.py
│   │   ├── load_bronze_transactions.py
│   │   ├── load_bronze_mcc_codes.py
│   │   ├── users_mns.py
│   │   ├── cards_mns.py
│   │   ├── transactions_mns.py
│   │   ├── mcc_codes_mns.py
│   │   └── ... (các file structured khác)
│   └── utils/
│       ├── db_connection.py
│       ├── logger.py
│       └── hash_utils.py
├── data/
│   └── unstructured/
│       ├── documents/              # INPUT: Ảnh gốc
│       │   ├── doc_type=id_card/
│       │   │   └── run_date=YYYY-MM-DD/
│       │   │       └── user_id=1/CCCD_001.jpg
│       │   └── doc_type=savings_book/
│       │       └── run_date=YYYY-MM-DD/
│       │           └── user_id=1/savings_001.jpg
│       └── extracted/             # OUTPUT: CSV kết quả OCR
│           ├── id_card_extractions_YYYY-MM-DD.csv
│           └── savings_book_extractions_YYYY-MM-DD.csv
├── dbt_bank/                      # dbt models (chỉ structured)
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── sql/
│   └── create_bronze_unstructured_tables.sql  # Simple bronze tables
├── archive/                       # Old pipeline files (archived)
│   └── ...
├── requirements.txt
├── requirements-ocr.txt
└── README.md
```

---

## 5. Workflow chi tiết

### 5.1. Structured Data Flow (Full Pipeline)

```
SQL Server (users, cards, transactions, mcc)
     ↓
Python Extract → TDY tables
     ↓
Compute MNS (I/U/D flags)
     ↓
dbt: Bronze MNS → Silver (Data Vault)
     ↓
dbt: Silver → Gold (Star Schema)
     ↓
Power BI / Analytics
```

**Orchestration**: Airflow DAG (`dags/banking_pipeline_dag.py`)

### 5.2. Unstructured Data Flow (Simple Pipeline)

```
data/unstructured/documents/
    ├── doc_type=id_card/run_date=YYYY-MM-DD/user_id=*/images
    └── doc_type=savings_book/run_date=YYYY-MM-DD/user_id=*/images
         ↓
[Step 1] OCR Extraction (PaddleOCR)
  - ocr_extract_id_card.py
  - ocr_extract_savings_book.py
         ↓
[Step 2] Output CSV
  data/unstructured/extracted/
    ├── id_card_extractions_YYYY-MM-DD.csv
    └── savings_book_extractions_YYYY-MM-DD.csv
         ↓
[Step 3] Load to Bronze
  load_bronze_simple.py
    ├── bronze.id_card_results
    └── bronze.savings_book_results
         ↓
Direct SQL Queries (không có Silver/Gold)
```

---

## 6. Hướng dẫn chạy pipeline

### 6.1. Chuẩn bị môi trường

**Tạo 2 virtual environments** (do PaddleOCR conflict với Airflow/dbt):

```powershell
# 1. Main environment (core + structured pipeline)
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. OCR environment (PaddleOCR + PaddlePaddle)
deactivate
python -m venv .venv_ocr
.venv_ocr\Scripts\Activate.ps1
pip install -r requirements-ocr.txt
```

**Test PaddleOCR**:

```powershell
.venv_ocr\Scripts\Activate.ps1
python -c "from paddleocr import PaddleOCR; ocr = PaddleOCR(lang='vi'); print('PaddleOCR OK')"
```

### 6.2. Database setup

Tạo database `banking_pipeline` trong SQL Server.

**Chạy SQL script để tạo bronze tables**:

```sql
-- Mở sql/create_bronze_unstructured_tables.sql trong SSMS
-- Hoặc chạy bằng Python:
.venv\Scripts\Activate.ps1
python -c "
from scripts.utils.db_connection import get_target_engine
with open('sql/create_bronze_unstructured_tables.sql', 'r', encoding='utf-8') as f:
    sql = f.read()
engine = get_target_engine()
with engine.begin() as conn:
    conn.execute(sql)
print('Tables created')
"
```

### 6.3. Chạy Unstructured Pipeline (Simple)

**Option A: Chạy full pipeline** (tự động OCR + Load):

```powershell
# Chạy cả CCCD và Sổ tiết kiệm
.venv_ocr\Scripts\Activate.ps1
python scripts/extract/run_simple_pipeline.py --run-date 2026-05-22 --doc-type both

# Chỉ CCCD
python scripts/extract/run_simple_pipeline.py --run-date 2026-05-22 --doc-type id_card

# Chỉ Sổ tiết kiệm
python scripts/extract/run_simple_pipeline.py --run-date 2026-05-22 --doc-type savings_book

# Chỉ OCR, không load DB
python scripts/extract/run_simple_pipeline.py --run-date 2026-05-22 --doc-type both --skip-load
```

**Option B: Chạy từng bước**:

```powershell
# Step 1: OCR CCCD (trong .venv_ocr)
.venv_ocr\Scripts\Activate.ps1
python scripts/extract/ocr_extract_id_card.py `
    --input-dir "data/unstructured/documents/doc_type=id_card/run_date=2026-05-22" `
    --run-date 2026-05-22

# Step 2: OCR Sổ tiết kiệm
python scripts/extract/ocr_extract_savings_book.py `
    --input-dir "data/unstructured/documents/doc_type=savings_book/run_date=2026-05-22" `
    --run-date 2026-05-22

# Step 3: Load to DB (trong .venv)
.venv\Scripts\Activate.ps1
python scripts/extract/load_bronze_simple.py `
    --csv "data/unstructured/extracted/id_card_extractions_2026-05-22.csv" `
    --doc-type id_card

python scripts/extract/load_bronze_simple.py `
    --csv "data/unstructured/extracted/savings_book_extractions_2026-05-22.csv" `
    --doc-type savings_book
```

### 6.4. Chạy Structured Pipeline (Full ETL)

```powershell
.venv\Scripts\Activate.ps1

# 1. Extract structured data
python scripts/extract/load_bronze_users.py
python scripts/extract/load_bronze_cards.py
python scripts/extract/load_bronze_transactions.py
python scripts/extract/load_bronze_mcc_codes.py

# 2. Compute MNS
python scripts/extract/users_mns.py
python scripts/extract/cards_mns.py
python scripts/extract/transactions_mns.py
python scripts/extract/mcc_codes_mns.py

# 3. dbt transform
cd dbt_bank
dbt run
cd ..

# 4. (Optional) Archive TDY to PDY
python scripts/extract/move_tdy_to_pdy.py
```

**Hoặc dùng Airflow**:

```powershell
# Trigger DAG
airflow dags trigger banking_pipeline_dag --conf '{"run_date": "2026-05-22"}'

# Xem logs
airflow logs banking_pipeline_dag <task_id>
```

---

## 7. Query dữ liệu

### Unstructured (Bronze Tables)

**CCCD results**:

```sql
SELECT TOP 10 
    document_id,
    full_name,
    id_number,
    date_of_birth,
    sex,
    extraction_confidence,
    status
FROM bronze.id_card_results
WHERE run_date = '2026-05-22'
ORDER BY extraction_confidence DESC;

-- Summary
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN status = 'ok' THEN 1 END) as success,
    COUNT(CASE WHEN status = 'error' THEN 1 END) as errors,
    AVG(extraction_confidence) as avg_confidence
FROM bronze.id_card_results
WHERE run_date = '2026-05-22';
```

**Sổ tiết kiệm results**:

```sql
SELECT TOP 10 
    document_id,
    account_holder,
    account_number,
    balance,
    interest_rate,
    status
FROM bronze.savings_book_results
WHERE run_date = '2026-05-22';
```

**Structured (Gold Layer)**:

```sql
-- User dimension
SELECT * FROM gold.dim_user LIMIT 10;

-- Transaction facts
SELECT 
    t.transaction_id,
    u.full_name,
    c.card_number,
    t.amount,
    t.transaction_date
FROM gold.fact_transactions t
JOIN gold.dim_user u ON t.user_id = u.user_id
JOIN gold.dim_card c ON t.card_id = c.card_id
WHERE t.transaction_date >= '2026-05-01';
```

---

## 8. Database Schema

> DDL đầy đủ: [scripts/sql/create_bronze_ocr_tables.sql](scripts/sql/create_bronze_ocr_tables.sql)
>
> Script load: [scripts/extract/load_bronze_unstructured.py](scripts/extract/load_bronze_unstructured.py)

---

### `bronze.id_card_results`

Lưu kết quả trích xuất thông tin từ ảnh CCCD (Căn cước công dân). Mỗi dòng tương ứng một ảnh của một user.

#### Nhóm 1 — Source tracking (theo dõi nguồn)

| Cột | Kiểu | Mô tả | Ví dụ |
|-----|------|-------|-------|
| `id` | `BIGINT IDENTITY` | Surrogate key, PK tự tăng | `1` |
| `file` | `NVARCHAR(200)` | Tên file ảnh | `"id_card_scan.jpg"` |
| `file_path` | `NVARCHAR(500)` | Đường dẫn tuyệt đối đến ảnh gốc | `"E:\...\user_id=1\id_card_scan.jpg"` |
| `run_date` | `DATE` | Ngày chạy pipeline — dùng làm partition key | `2026-05-25` |
| `user_id` | `INT` | Trích từ tên thư mục `user_id=<n>` | `1` |

#### Nhóm 2 — Thông tin trích xuất từ CCCD

Các trường này được trích ra từ ảnh bằng OCR + template ROI matching. Lưu dạng `NVARCHAR` để giữ nguyên định dạng gốc (bronze không ép kiểu).

| Cột | Kiểu | Mô tả | Ví dụ |
|-----|------|-------|-------|
| `full_name` | `NVARCHAR(300)` | Họ và tên | `"Nguyen Van An"` |
| `id_number` | `NVARCHAR(50)` | Số CCCD (thường 12 chữ số, dữ liệu test có prefix `DEMO-`) | `"DEMO-33265923"` |
| `date_of_birth` | `NVARCHAR(20)` | Ngày sinh, định dạng `DD/MM/YYYY` | `"10/12/1993"` |
| `sex` | `NVARCHAR(20)` | Giới tính | `"Male"` / `"Female"` |
| `nationality` | `NVARCHAR(100)` | Quốc tịch | `"Vietnam"` |
| `place_of_origin` | `NVARCHAR(300)` | Quê quán | `"South John"` |
| `place_of_residence` | `NVARCHAR(500)` | Nơi thường trú | `"0 Scott Wall, CO 59418"` |
| `issue_date` | `NVARCHAR(20)` | Ngày cấp, định dạng `DD/MM/YYYY` | `"18/06/2021"` |
| `expiry_date` | `NVARCHAR(20)` | Ngày hết hạn, định dạng `DD/MM/YYYY` | `"18/11/2028"` |

> Ngày tháng giữ dạng string `DD/MM/YYYY` ở bronze — silver layer sẽ parse sang `DATE` khi cần.

#### Nhóm 3 — Confidence scores (điểm tin cậy)

Đánh giá chất lượng kết quả OCR ở 3 cấp độ khác nhau, tất cả trong khoảng `[0.0 – 1.0]`.

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `ocr_confidence` | `FLOAT` | Độ tin cậy của engine OCR (PaddleOCR recognition score) |
| `parse_confidence` | `FLOAT` | Độ tin cậy của bước parse/validate từng trường |
| `final_confidence` | `FLOAT` | Điểm tổng hợp cuối = kết hợp `ocr_confidence` và `parse_confidence` |
| `plausible_fields` | `INT` | Số trường có giá trị hợp lệ trong tổng 9 trường (0–9) |

#### Nhóm 4 — Audit

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `_loaded_at` | `DATETIME2 DEFAULT SYSUTCDATETIME()` | Thời điểm load vào DB (UTC) |

**Indexes:** `IX_id_card_results_run_date (run_date)`, `IX_id_card_results_user_id (user_id)`

---

### `bronze.savings_book_results`

Lưu kết quả trích xuất thông tin từ ảnh sổ tiết kiệm. Mỗi dòng là một giao dịch/trang sổ của một user.

#### Nhóm 1 — Source tracking (theo dõi nguồn)

| Cột | Kiểu | Mô tả | Ví dụ |
|-----|------|-------|-------|
| `id` | `BIGINT IDENTITY` | Surrogate key, PK tự tăng | `1` |
| `file` | `NVARCHAR(200)` | Tên file ảnh | `"savings_book_scan.jpg"` |
| `file_path` | `NVARCHAR(500)` | Đường dẫn tuyệt đối đến ảnh gốc | `"E:\...\user_id=1\savings_book_scan.jpg"` |
| `run_date` | `DATE` | Ngày chạy pipeline — dùng làm partition key | `2026-05-25` |
| `user_id` | `INT` | Trích từ tên thư mục `user_id=<n>` | `1` |

#### Nhóm 2 — Thông tin trích xuất từ sổ tiết kiệm

| Cột | Kiểu | Mô tả | Ví dụ |
|-----|------|-------|-------|
| `transaction_date` | `NVARCHAR(20)` | Ngày giao dịch, định dạng `DD/MM/YYYY` | `"20/09/2024"` |
| `description` | `NVARCHAR(300)` | Mô tả loại giao dịch | `"Account opening"` |
| `transaction_code` | `NVARCHAR(20)` | Mã giao dịch viết tắt | `"OPN"` |
| `transaction_amount` | `NVARCHAR(50)` | Số tiền giao dịch, giữ string vì có dấu phẩy | `"117,136"` |
| `balance` | `NVARCHAR(50)` | Số dư sau giao dịch, giữ string ở bronze | `"117,136"` |
| `interest_rate` | `NVARCHAR(20)` | Lãi suất áp dụng, giữ string ở bronze | `"6.5%"` |
| `signature` | `NVARCHAR(200)` | Tên người ký trên sổ | `"Christopher Johnson"` |

> `transaction_amount`, `balance`, `interest_rate` lưu `NVARCHAR` vì OCR trả về chuỗi có dấu phẩy/ký hiệu (`"117,136"`, `"6.5%"`). Silver layer sẽ parse sang số khi cần.

#### Nhóm 3 — Confidence scores (điểm tin cậy)

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `ocr_confidence` | `FLOAT` | Độ tin cậy của engine OCR |
| `parse_confidence` | `FLOAT` | Độ tin cậy của bước parse/validate |
| `final_confidence` | `FLOAT` | Điểm tổng hợp cuối |
| `plausible_fields` | `INT` | Số trường có giá trị hợp lệ (0–7) |

#### Nhóm 4 — Audit

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `_loaded_at` | `DATETIME2 DEFAULT SYSUTCDATETIME()` | Thời điểm load vào DB (UTC) |

**Indexes:** `IX_savings_book_results_run_date (run_date)`, `IX_savings_book_results_user_id (user_id)`

---

## 9. Troubleshooting

### PaddleOCR errors on Windows CPU

```
RuntimeError: PaddlePaddle encountered an error related to PIR/oneDNN
```

**Fix**:
- Sử dụng `paddlepaddle==3.2.2` (không dùng 3.3.x)
- Code đã có workaround disable flags trong `ocr_extract_*.py`
- Kiểm tra `.venv_ocr` đã active

### Database connection fails

- Kiểm tra SQL Server đang chạy
- Kiểm tra connection string trong `scripts/utils/db_connection.py`
- Đảm bảo database `banking_pipeline` tồn tại
- Chạy SQL script để tạo tables trước

### No images found

- Kiểm tra folder structure: `doc_type=*/run_date=*/user_id=*/`
- Sử dụng `--limit 1` để test
- Kiểm tra file extensions (.jpg, .png, .jpeg, .tiff, .bmp)

### Out of memory

- Giảm batch size trong OCR (không có batch processing hiện tại)
- Xử lý ít ảnh mỗi lần
- Đóng ứng dụng khác

---

## 10. Performance Notes

### OCR Speed

- **CPU**: ~2-4s/image (single core)
- **GPU** (nếu có CUDA): ~0.5-1s/image
- 1000 ảnh → ~30-60 phút trên CPU

### Recommendations

- Test với `--limit 10` trước khi chạy full
- Chạy vào ngoài giờ hành chính nếu dataset lớn
- Consider parallel processing (multiprocessing) nếu cần tối ưu

---

## 11. Development Notes

### Design Decisions

1. **Why Bronze-only for unstructured?**
   - Task scope: Chỉ cần OCR extraction, không cần Silver/Gold cho unstructured
   - Simplicity: Dễ hiểu, dễ debug, phù hợp với sinh viên mới
   - Direct query: Query trực tiếp từ Bronze đủ cho use case

2. **Why separate .venv_ocr?**
   - PaddleOCR dependencies conflict với Airflow/dbt stack
   - Isolation: Có thể cập nhật OCR stack độc lập
   - Performance: Tránh bloat core environment

3. **Why no manifest for new pipeline?**
   - Manifest dùng cho incremental logic (TDY/PDY/MNS)
   - Simple pipeline: Mỗi run là independent, không cần track changes
   - Giảm complexity: Scan folder trực tiếp

### Future Improvements

- [ ] Parallel OCR processing (multiprocessing.Pool)
- [ ] Add data validation/quality checks
- [ ] Retry logic for failed OCR
- [ ] Preprocessing (image enhancement, deskewing)
- [ ] Multi-template matching (different CCCD layouts)
- [ ] Dashboard for extraction results review

---

## 12. Testing

### Unit Test (OCR extraction)

```powershell
.venv_ocr\Scripts\Activate.ps1
python scripts/extract/ocr_extract_id_card.py `
    --input-dir "tests/fixtures/id_card_sample/" `
    --run-date 2026-05-22 `
    --limit 5
```

### Integration Test (Full pipeline)

```powershell
# Tạo thư mục test
mkdir test_data/unstructured/documents/doc_type=id_card/run_date=2026-05-22/user_id=999
# Copy ảnh mẫu vào
# Chạy pipeline
python scripts/extract/run_simple_pipeline.py --run-date 2026-05-22 --doc-type id_card
# Kiểm tra database
sqlcmd -Q "SELECT * FROM bronze.id_card_results WHERE user_id = 999"
```

---

## 13. License & Contributing

[Your License Here]

Pull requests welcome. Please open issues first to discuss changes.

---

## 14. Contact

[Your contact info]

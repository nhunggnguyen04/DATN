# DATN — Banking Data Platform

Đồ án tốt nghiệp: nền tảng dữ liệu ngân hàng xử lý cả **dữ liệu có cấu trúc** (Bronze → Silver → Gold) và **dữ liệu phi cấu trúc** (OCR tài liệu ngân hàng), kèm web demo trực quan.

---

## Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────────┐
│                      NGUỒN DỮ LIỆU                              │
│          SQL Server (OLTP)          Ảnh tài liệu                │
│     users · cards · transactions    (CCCD, Sổ tiết kiệm)        │
└──────────────────┬──────────────────────────┬───────────────────┘
                   │                          │
                   ▼                          ▼
┌──────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                              │
│  Có cấu trúc:                    Phi cấu trúc:                   │
│  *_tdy · *_pdy · *_mns           id_card_results                 │
│  (users, cards, transactions,    savings_book_results             │
│   mcc_codes)                                                      │
└──────────────────┬───────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                              │
│                   Data Vault 2.0                                  │
│         Hubs · Links · Satellites                                 │
└──────────────────┬───────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                               │
│                      Star Schema                                  │
│  dim_customer · dim_card · dim_merchant · dim_mcc · dim_date     │
│                      fact_transaction                             │
└──────────────────┬───────────────────────────────────────────────┘
                   │
                   ▼
           Power BI / Analytics
```

---

## Công nghệ sử dụng

| Thành phần     | Công nghệ                                      |
| ---------------- | ------------------------------------------------ |
| Orchestration    | Apache Airflow 2.9.0                             |
| Transformation   | dbt (Data Build Tool)                            |
| Database         | SQL Server (OLTP nguồn + Data Warehouse đích) |
| OCR              | PaddleOCR + PaddlePaddle 3.2.2                   |
| AI (Web demo)    | Google Gemini API · Groq LLaMA                  |
| Web demo         | Flask                                            |
| Containerization | Docker Compose                                   |
| BI               | Power BI                                         |

---

## Cấu trúc thư mục

```
DATN/
├── dags/                             # Airflow DAGs
│   ├── banking_structured_dag.py     # Pipeline chính (@daily 02:00)
│   ├── data_quality_dag.py           # Kiểm tra chất lượng dữ liệu (@daily 04:00)
│   ├── ocr_unstructured_dag.py       # OCR pipeline (trigger thủ công)
│   └── common/                       # operators, constants, callbacks
├── scripts/
│   ├── extract/
│   │   ├── load_bronze_*.py          # Nạp dữ liệu vào *_tdy
│   │   ├── *_mns.py                  # Tính change-set I/U/D
│   │   ├── ocr_extract_id_card.py    # OCR CCCD
│   │   ├── ocr_extract_savings_book.py
│   │   └── load_bronze_unstructured.py
│   ├── utils/
│   │   ├── db_connection.py          # Engine SQL Server dùng chung
│   │   └── audit_logger.py
│   └── setup_audit.py                # Khởi tạo schema audit (chạy 1 lần)
├── dbt_bank/
│   ├── models/
│   │   ├── bronze/                   # source definitions
│   │   ├── silver/                   # hubs · links · satellites · ref
│   │   └── gold/                     # dimensions · facts
│   ├── macros/                       # hash_md5, generate_schema_name
│   ├── profiles.yml
│   └── dbt_project.yml
├── demo_web/                         # Web demo OCR
│   ├── app.py                        # Flask application
│   ├── ocr_engine.py                 # PaddleOCR engine
│   ├── gemini_engine.py              # Google Gemini engine
│   ├── groq_engine.py                # Groq LLaMA engine
│   └── templates/index.html
├── sql/
│   ├── create_audit_tables.sql
│   └── create_bronze_unstructured_tables.sql
├── data/
│   └── unstructured/
│       ├── documents/                # Ảnh đầu vào (doc_type=*/run_date=*/user_id=*/)
│       └── extracted/                # CSV/XLSX kết quả OCR
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

---

## Cài đặt

### 1. Cấu hình môi trường

Sao chép file cấu hình và điền thông tin kết nối:

```powershell
copy .env.example .env
```

Các biến cần điền trong `.env`:

```env
SOURCE_SERVER=<địa chỉ SQL Server nguồn>
SOURCE_DATABASE=<tên database OLTP>
SOURCE_USERNAME=<username>
SOURCE_PASSWORD=<password>

TARGET_SERVER=localhost
TARGET_DATABASE=DATN
TARGET_USERNAME=sa
TARGET_PASSWORD=<password>

ODBC_DRIVER=ODBC Driver 17 for SQL Server
```

### 2. Tạo virtualenv

```powershell
# Môi trường chính (pipeline có cấu trúc + web Gemini/Groq)
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install flask requests

# Môi trường OCR (PaddleOCR — tách riêng do xung đột dependency)
python -m venv .venv_ocr
.venv_ocr\Scripts\Activate.ps1
pip install paddlepaddle==3.2.2 paddleocr flask requests
```

> **Lưu ý:** Dùng `paddlepaddle==3.2.2`, không dùng 3.3.x (lỗi trên Windows CPU).

### 3. Khởi tạo database

```powershell
.venv\Scripts\Activate.ps1

# Kiểm tra kết nối
python scripts/utils/db_connection.py

# Tạo schema và bảng audit (chạy 1 lần)
python scripts/setup_audit.py
```

---

## Chạy pipeline

### Pipeline có cấu trúc (thủ công)

```powershell
.venv\Scripts\Activate.ps1

# 1. Nạp dữ liệu nguồn vào Bronze *_tdy
python scripts/extract/load_bronze_users.py
python scripts/extract/load_bronze_cards.py
python scripts/extract/load_bronze_transactions.py
python scripts/extract/load_bronze_mcc_codes.py

# 2. Tính change-set MNS
python scripts/extract/users_mns.py
python scripts/extract/cards_mns.py
python scripts/extract/transactions_mns.py
python scripts/extract/mcc_codes_mns.py

# 3. Transform bằng dbt
cd dbt_bank
dbt run --select tag:hub
dbt run --select tag:link
dbt run --select tag:satellite
dbt run --select tag:dim
dbt run --select fact_transaction --vars '{"run_date":"2026-05-25"}'
dbt test --select tag:silver
dbt test --select tag:gold
cd ..
```

### Pipeline OCR (thủ công)

```powershell
# Bước 1 — OCR (dùng .venv_ocr)
.venv_ocr\Scripts\Activate.ps1
python scripts/extract/ocr_extract_id_card.py --run-date 2026-05-25
python scripts/extract/ocr_extract_savings_book.py --run-date 2026-05-25

# Bước 2 — Nạp vào Bronze (dùng .venv)
.venv\Scripts\Activate.ps1
python scripts/extract/load_bronze_unstructured.py \
    --csv data/unstructured/extracted/id_card_extractions_2026-05-25.csv \
    --doc-type id_card
python scripts/extract/load_bronze_unstructured.py \
    --csv data/unstructured/extracted/savings_book_roi_extractions_2026-05-25.csv \
    --doc-type savings_book
```

---

## Airflow (Docker)

Airflow orchestrate toàn bộ pipeline có cấu trúc tự động theo lịch.

```powershell
# Build image và khởi động (lần đầu)
docker compose build
docker compose up -d

# Xem log scheduler
docker compose logs -f airflow-scheduler

# Dừng
docker compose down
```

Truy cập Airflow UI: **http://localhost:8080** — tài khoản `admin / admin`

| DAG                        | Lịch             | Mô tả                              |
| -------------------------- | ----------------- | ------------------------------------ |
| `banking_structured_dag` | Hàng ngày 02:00 | ETL chính: Bronze → Silver → Gold |
| `data_quality_dag`       | Hàng ngày 04:00 | Kiểm tra chất lượng dữ liệu    |
| `ocr_unstructured_dag`   | Thủ công        | OCR CCCD và sổ tiết kiệm         |

---

## Web Demo OCR

Web demo cho phép trích xuất thông tin từ ảnh **CCCD** hoặc **Sổ tiết kiệm** với 3 engine AI, kèm chatbot hỏi đáp về dữ liệu đã trích xuất.

| Engine        | Yêu cầu           |
| ------------- | ------------------- |
| PaddleOCR     | Không cần API key |
| Google Gemini | Gemini API key      |
| Groq LLaMA    | Groq API key        |

### Cách 1 — Chạy trên host (Gemini & Groq engine)

```powershell
.venv\Scripts\Activate.ps1
python demo_web/app.py
```

### Cách 2 — Chạy qua Docker (đầy đủ 3 engine, bao gồm PaddleOCR)

```powershell
docker compose up -d demo-web
```

Truy cập: **http://localhost:5000**

> Hai cách dùng chung cổng 5000, không chạy song song.

---

## Mẫu incremental MNS

Mỗi entity có cấu trúc 3 bảng Bronze:

| Bảng     | Ý nghĩa                                             |
| --------- | ----------------------------------------------------- |
| `*_tdy` | Snapshot hôm nay (truncate & load mỗi ngày)        |
| `*_pdy` | Snapshot lũy kế (giữ nguyên, dùng để so sánh) |
| `*_mns` | Change-set:`id` + `operation_flag` (I/U)          |

Script MNS so sánh `*_tdy` với `*_pdy`, sinh cờ `I` (Insert) hoặc `U` (Update), rồi upsert `*_tdy` → `*_pdy`. Không sinh cờ `D` ở chế độ chạy theo ngày.

---

## Troubleshooting

**Lỗi PaddleOCR trên Windows CPU:**

```
RuntimeError: PaddlePaddle encountered an error related to PIR/oneDNN
```

→ Dùng đúng `paddlepaddle==3.2.2`, không dùng 3.3.x.

**Lỗi kết nối database:**

- Kiểm tra SQL Server đang chạy
- Đảm bảo database `DATN` đã tồn tại
- Chạy `python scripts/utils/db_connection.py` để test nhanh

**DAG không xuất hiện trong Airflow UI:**

- Đợi scheduler scan (mặc định 30s)
- Kiểm tra log: `docker compose logs airflow-scheduler`

**Web demo lỗi "Không tìm thấy script":**

- Đảm bảo `scripts/` đã được mount vào container (xem `docker-compose.yaml`)

---

## Tác giả

Nguyễn Hồng Nhung — nguyenhongnhungtxa@gmail.com

Link dữ liệu tham khảo: [www.kaggle.com/datasets/ealtman2019/credit-card-transactions?resource=download&amp;select=User0_credit_card_transactions.csv](https://www.kaggle.com/datasets/ealtman2019/credit-card-transactions?resource=download&select=User0_credit_card_transactions.csv)

# DATN - Banking Data Pipeline

Dự án xây dựng pipeline dữ liệu ngân hàng theo kiến trúc **Bronze → Silver → Gold**, xử lý cả dữ liệu có cấu trúc (SQL Server) và dữ liệu phi cấu trúc (tài liệu quét OCR).

## Tổng quan

Mục tiêu của dự án là xây dựng hệ thống dữ liệu hoàn chỉnh từ tầng dữ liệu gốc đến tầng phục vụ phân tích:

- Kết nối tới database nguồn được cấp quyền truy cập
- Extract dữ liệu từ source database và tài liệu unstructured
- Nạp dữ liệu vào tầng Bronze
- Xử lý incremental theo logic **PDY / TDY / MNS**
- Xây dựng tầng Silver theo mô hình **Data Vault**
- Xây dựng tầng Gold theo mô hình **Dimensional Modeling**
- Sử dụng **dbt** để transform dữ liệu
- Sử dụng **Airflow** để điều phối pipeline
- Phục vụ dashboard **Power BI** hoặc các phân tích dữ liệu sau này

---

## 1. Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCE LAYER                                 │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐      ┌─────────────────────────────────┐  │
│  │   SQL Server DB      │      │    Unstructured Files           │  │
│  │   • users            │      │    • CCCD/CMND scans            │  │
│  │   • cards            │      │    • Savings book images        │  │
│  │   • transactions     │      │    • Other documents            │  │
│  │   • mcc_codes        │      │                                 │  │
│  └──────────┬───────────┘      └──────────────┬──────────────────┘  │
└─────────────┼─────────────────────────────────┼─────────────────────┘
              │                                 │
              ▼                                 ▼
┌─────────────────────────┐     ┌───────────────────────────────────┐
│  Structured Extract     │     │  Unstructured Extract             │
│  • Python + pyodbc      │     │  • PaddleOCR                      │
│  • Hashing (SHA256)     │     │  • Extract CCCD info              │
└─────────────┬───────────┘     └──────────────┬────────────────────┘
              │                                 │
              ▼                                 ▼
┌───────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                               │
│              (Raw data - Incremental PDY/TDY/MNS)                  │
├───────────────────────────────────────────────────────────────────┤
│  Manifest CSV → users, cards, transactions, documents (pdy/tdy)   │
│  Compute MNS (I/U/D flags)                                        │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                                │
│                   (Data Vault - Hubs, Links, Satellites)           │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                                 │
│                   (Star Schema - Facts + Dimensions)               │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                      CONSUMPTION LAYER                             │
│              Power BI / SQL Queries / ML Models                    │
└───────────────────────────────────────────────────────────────────┘
```

---

## 2. Chi tiết các tầng

### 2.1. Source Layer

| Nguồn | Dữ liệu |
|-------|---------|
| SQL Server | `users`, `cards`, `transactions`, `mcc_codes` |
| Unstructured Files | Ảnh CCCD/CMND, sổ tiết kiệm, tài liệu khác |

### 2.2. Bronze Layer

Tầng dữ liệu thô, lưu trữ nguyên bản từ nguồn:

- **Structured Data**: `users_tdy/pdy/mns`, `cards_tdy/pdy/mns`, `transactions_tdy/pdy/mns`, `mcc_codes`
- **Unstructured Data**: `documents_tdy/pdy/mns` + OCR extraction results

#### Incremental Logic (PDY/TDY/MNS)

```
PDY (Previous Day)  ←  TDY (Today)  →  MNS (Move-New-Stable)

Quy trình mỗi run:
1. Xóa MNS cũ
2. Copy TDY hiện tại sang PDY
3. TRUNCATE TDY
4. Extract source mới vào TDY
5. So sánh TDY vs PDY → compute MNS (I/U/D flags)
6. Silver đọc từ MNS
```

#### Manifest Documents

Manifest CSV là file metadata cho tài liệu unstructured:

| Column | Mô tả |
|--------|-------|
| `document_id` | UUID duy nhất |
| `entity_type` | Loại thực thể (user) |
| `entity_id` | ID liên kết (user_id) |
| `doc_type` | Loại tài liệu (id_card, savings_book) |
| `file_path` | Đường dẫn đến file |
| `sha256` | Hash kiểm tra thay đổi |
| `run_date` | Ngày chạy job |

### 2.3. Silver Layer (Data Vault)

Mô hình Data Vault gồm:

- **Hubs**: Business keys (user_id, card_number, document_id)
- **Links**: Relationships (user-card, user-transaction)
- **Satellites**: Attributes + history (SCD Type 2)

### 2.4. Gold Layer (Dimensional Model)

Star schema với Facts và Dimensions:

**Dimensions**:
- `dim_user`: Thông tin khách hàng
- `dim_card`: Thông tin thẻ
- `dim_transaction`: Thông tin giao dịch
- `dim_mcc`: Mã ngành hàng
- `dim_date`: Thời gian

**Facts**:
- `fact_transactions`: Giao dịch (daily/monthly aggregates)
- `fact_card_usage`: Sử dụng thẻ

---

## 3. Công nghệ sử dụng

| Thành phần | Công nghệ |
|------------|-----------|
| Source Database | SQL Server |
| Orchestration | Apache Airflow |
| Transform | dbt (SQL Server adapter) |
| Extract | Python (pandas, pyodbc) |
| OCR | PaddleOCR + PaddlePaddle |
| Target DB | SQL Server |
| Visualization | Power BI |

---

## 4. Cấu trúc dự án

```
DATN/
├── dags/
│   └── banking_pipeline_dag.py    # Airflow DAG orchestration
├── scripts/
│   ├── extract/
│   │   ├── ocr_extract_id_card.py # OCR CCCD
│   │   ├── load_bronze_users.py   # Load users vào Bronze
│   │   ├── load_bronze_cards.py   # Load cards vào Bronze
│   │   ├── load_bronze_transactions.py
│   │   ├── load_bronze_mcc_codes.py
│   │   ├── load_bronze_documents.py
│   │   ├── users_mns.py           # Compute MNS
│   │   ├── cards_mns.py
│   │   ├── transactions_mns.py
│   │   └── documents_mns.py
│   └── utils/
│       ├── db_connection.py
│       ├── logger.py
│       └── hash_utils.py
├── dbt_bank/                        # dbt project
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── macros/
│   ├── seeds/
│   └── dbt_project.yml
├── output/
│   ├── manifests/                   # Manifest CSV files
│   └── unstructured/
│       ├── documents/               # Raw images
│       └── extracted/               # OCR extraction results
└── docs/
```

---

## 5. Workflow chi tiết

### 5.1. Structured Data Flow

```
SQL Server (users, cards, transactions, mcc_codes)
            ↓
     Python Extract
            ↓
     Bronze TDY/PDY/MNS
            ↓
       dbt Silver
   (Data Vault model)
            ↓
       dbt Gold
   (Star Schema)
            ↓
    Power BI / Analytics
```

### 5.2. Unstructured Data Flow

```
Images Folder (CCCD, savings_book)
            ↓
    Scan & Generate Manifest
            ↓
Manifest CSV (documents_YYYY-MM-DD.csv)
            ↓
   Load Bronze Documents
            ↓
    PaddleOCR Processing
            ↓
  Extract Info (name, id, dob, ...)
            ↓
  Silver → Gold (dbt models)
            ↓
    Power BI / Analytics
```

---

## 6. Hướng dẫn chạy pipeline

### 6.1. Chuẩn bị môi trường

```bash
# Tạo virtual environment
python -m venv .venv
.venv\Scripts\activate

# Cài dependencies
pip install -r requirements.txt

# OCR dependencies (nếu cần)
cd .venv_ocr
pip install -r requirements.txt
```

### 6.2. Cấu hình kết nối database

Tạo file `.env` hoặc cấu hình `dbt_bank/profiles.yml`:

```bash
# Copy từ template
cp .env.example .env
```

### 6.3. Chạy pipeline

1. **Load dữ liệu vào Bronze**:
```bash
python scripts/extract/load_bronze_users.py
python scripts/extract/load_bronze_cards.py
python scripts/extract/load_bronze_transactions.py
python scripts/extract/load_bronze_documents.py
```

2. **Compute MNS**:
```bash
python scripts/extract/users_mns.py
python scripts/extract/cards_mns.py
python scripts/extract/transactions_mns.py
python scripts/extract/documents_mns.py
```

3. **Chạy OCR**:
```bash
python scripts/extract/ocr_extract_id_card.py --manifest output/manifests/documents_2026-05-13.csv
```

4. **Chạy dbt**:
```bash
cd dbt_bank
dbt run
```

---

## 7. Tài liệu tham khảo

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Data Vault Modeling](https://www.datavaultmodellierung.de/)
- [PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR)
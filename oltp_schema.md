# OLTP Schema – Hệ thống Ngân hàng (Nguồn dữ liệu cho Pipeline)

Đây là schema hệ thống vận hành (OLTP) nguồn, cung cấp dữ liệu cho toàn bộ pipeline:
- **Nhánh Structured**: `customers`, `cards`, `transactions`, `merchants`, `mcc_codes` → Bronze → Silver (Data Vault) → Gold (Star Schema)
- **Nhánh Unstructured**: `kyc_documents` → OCR Pipeline → `bronze.id_card_results` / `bronze.savings_book_results`

---

## ERD (Entity Relationship Diagram)

```mermaid
erDiagram

    %% ─────────────────────────────────────────────
    %% CORE ENTITIES
    %% ─────────────────────────────────────────────

    customers {
        INT         customer_id         PK
        VARCHAR(100) full_name
        VARCHAR(10) gender
        INT         birth_year
        INT         birth_month
        INT         current_age
        INT         retirement_age
        VARCHAR(500) address
        DECIMAL(9_6) latitude
        DECIMAL(9_6) longitude
        DECIMAL(15_2) yearly_income
        DECIMAL(15_2) per_capita_income
        DECIMAL(15_2) total_debt
        INT         credit_score
        INT         num_credit_cards
        DATETIME2   created_at
        DATETIME2   updated_at
    }

    cards {
        INT         card_id             PK
        INT         customer_id         FK
        VARCHAR(50) card_brand
        VARCHAR(50) card_type
        VARCHAR(20) card_number
        DATE        expires
        VARCHAR(10) cvv
        VARCHAR(10) has_chip
        INT         num_cards_issued
        DECIMAL(15_2) credit_limit
        DATE        acct_open_date
        INT         year_pin_last_changed
        DATETIME2   created_at
        DATETIME2   updated_at
    }

    merchants {
        INT         merchant_id         PK
        VARCHAR(100) merchant_name
        VARCHAR(100) merchant_city
        VARCHAR(50) merchant_state
        VARCHAR(10) zip
        INT         mcc_code            FK
        DATETIME2   created_at
        DATETIME2   updated_at
    }

    mcc_codes {
        INT         mcc_id              PK
        VARCHAR(255) description
        VARCHAR(100) category_group
        DATETIME2   created_at
    }

    transactions {
        INT         transaction_id      PK
        DATETIME2   transaction_date
        INT         customer_id         FK
        INT         card_id             FK
        INT         merchant_id         FK
        INT         mcc_code            FK
        DECIMAL(15_2) amount
        VARCHAR(50) use_chip
        VARCHAR(100) merchant_city
        VARCHAR(50) merchant_state
        VARCHAR(10) zip
        VARCHAR(200) errors
        DATETIME2   created_at
    }

    %% ─────────────────────────────────────────────
    %% CREDIT & PIN HISTORY
    %% ─────────────────────────────────────────────

    credit_score_history {
        INT         history_id          PK
        INT         customer_id         FK
        INT         credit_score
        DECIMAL(15_2) total_debt
        DECIMAL(15_2) yearly_income
        DECIMAL(5_4) debt_to_income_ratio
        DATE        effective_date
        DATETIME2   recorded_at
    }

    card_pin_history {
        INT         pin_history_id      PK
        INT         card_id             FK
        INT         changed_year
        VARCHAR(10) change_reason
        DATETIME2   changed_at
    }

    %% ─────────────────────────────────────────────
    %% KYC / UNSTRUCTURED DOCUMENTS
    %% ─────────────────────────────────────────────

    kyc_documents {
        INT         document_id         PK
        INT         customer_id         FK
        VARCHAR(20) doc_type
        VARCHAR(500) file_path
        VARCHAR(20) status
        DATETIME2   submitted_at
        DATETIME2   verified_at
        VARCHAR(50) verified_by
    }

    id_card_kyc {
        INT         id_card_id          PK
        INT         document_id         FK
        VARCHAR(200) full_name
        VARCHAR(50) id_number
        DATE        date_of_birth
        VARCHAR(20) sex
        VARCHAR(100) nationality
        VARCHAR(300) place_of_origin
        VARCHAR(500) place_of_residence
        DATE        issue_date
        DATE        expiry_date
    }

    savings_accounts {
        INT         account_id          PK
        INT         document_id         FK
        INT         customer_id         FK
        VARCHAR(50) account_number
        VARCHAR(200) account_holder
        VARCHAR(100) account_type
        DATE        opening_date
        DECIMAL(15_2) current_balance
        DECIMAL(5_4) interest_rate
        VARCHAR(20) status
        DATETIME2   created_at
        DATETIME2   updated_at
    }

    savings_transactions {
        INT         stx_id              PK
        INT         account_id          FK
        DATE        transaction_date
        VARCHAR(20) transaction_code
        DECIMAL(15_2) transaction_amount
        DECIMAL(15_2) balance_after
        DECIMAL(5_4) interest_rate
        VARCHAR(200) description
        VARCHAR(100) signature
        DATETIME2   recorded_at
    }

    %% ─────────────────────────────────────────────
    %% RELATIONSHIPS
    %% ─────────────────────────────────────────────

    customers            ||--o{ cards                 : "sở hữu"
    customers            ||--o{ transactions           : "thực hiện"
    customers            ||--o{ credit_score_history   : "lịch sử tín dụng"
    customers            ||--o{ kyc_documents          : "nộp hồ sơ KYC"
    customers            ||--o{ savings_accounts       : "mở tài khoản"

    cards                ||--o{ transactions           : "dùng để giao dịch"
    cards                ||--o{ card_pin_history       : "lịch sử đổi PIN"

    merchants            ||--o{ transactions           : "nhận thanh toán"
    mcc_codes            ||--o{ merchants              : "phân loại"
    mcc_codes            ||--o{ transactions           : "phân loại GD"

    kyc_documents        ||--o| id_card_kyc            : "chi tiết CCCD"
    kyc_documents        ||--o| savings_accounts       : "chi tiết sổ tiết kiệm"

    savings_accounts     ||--o{ savings_transactions   : "lịch sử giao dịch"
```

---

## Ánh xạ OLTP → Pipeline

| Bảng OLTP | Luồng dữ liệu | Đích cuối (Gold) |
|---|---|---|
| `customers` | → `bronze.users_tdy/pdy` → Silver Hub/Sat → | `dim_customer` |
| `cards` | → `bronze.cards_tdy/pdy` → Silver Hub/Sat → | `dim_card` |
| `transactions` | → `bronze.transactions_tdy/pdy` → Silver Link/Sat → | `fact_transaction` |
| `merchants` | → `bronze.transactions_*` (denorm) → Silver → | `dim_merchant` |
| `mcc_codes` | → `bronze.mcc_codes_tdy/pdy` → Silver → | `dim_mcc` |
| `kyc_documents` + `id_card_kyc` | → **OCR Pipeline** → `bronze.id_card_results` | (KYC layer) |
| `kyc_documents` + `savings_accounts` | → **OCR Pipeline** → `bronze.savings_book_results` | (KYC layer) |

---

## Cơ chế đồng bộ (MNS Pattern)

```
OLTP Source ──daily snapshot──► bronze.*_tdy   (today)
                                bronze.*_pdy   (yesterday)
                                     │
                              MNS comparison
                              (I / U / D flags)
                                     │
                              ► Silver Data Vault
                                  hub_*  (business keys)
                                  sat_*  (attributes + SCD2)
                                  lnk_*  (relationships)
                                     │
                              ► Gold Star Schema
                                  dim_* / fact_*
```

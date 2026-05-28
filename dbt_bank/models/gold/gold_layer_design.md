# Gold Layer — Star Schema Design (Final)

## 1. Tổng quan

### Yêu cầu

| Yêu cầu | Giải pháp |
|---|---|
| Power BI dashboard CD1 (phân khúc KH, marketing) | dim_customer + dim_card + dim_mcc + fact_transaction |
| Power BI dashboard CD2 (giao dịch, rủi ro) | dim_card + dim_merchant + dim_date + fact_transaction |
| Chatbot query trực tiếp Gold | computed columns sẵn trong dim/fact → chatbot và dashboard đọc cùng cột, số khớp nhau |
| Pipeline incremental theo ngày giao dịch | fact_transaction incremental theo `transaction_datetime` truyền qua `run_date` |
| Dữ liệu: 157k transactions, 1/1/2022 → 31/10/2024 | 6 bảng dim + fact thuần, không cần agg_* |

### Cấu trúc

```
gold.dim_customer          ← hub_customer + sat_customer_profile
gold.dim_card              ← hub_card + sat_card_detail + link_customer_card
gold.dim_merchant          ← hub_merchant + link_transaction + sat_transaction_detail
gold.dim_mcc               ← hub_mcc + sat_mcc_detail
gold.dim_date              ← seed CSV (1/1/2022 → 31/10/2024)
gold.fact_transaction      ← link_transaction + sat_transaction_detail + lookup dim_*
```

---

## 2. Nguyên tắc thiết kế

### Denormalization

Star Schema denormalize triệt để. Ví dụ `dim_card` chứa cả `customer_id` (lấy qua `link_customer_card`) dù Data Vault cấm điều này trong satellite. Gold không tuân theo Data Vault — mục đích khác nhau.

### Surrogate key

Không dùng hash key CHAR(32) từ Silver làm PK trong Gold. Thay bằng `INT IDENTITY` — nhỏ gọn, Power BI JOIN nhanh hơn. Hash key vẫn giữ để traceability ngược Silver.

### Computed columns phục vụ chatbot

Các chỉ số quan trọng được **pre-compute sẵn** trong Gold thay vì để Power BI DAX hoặc chatbot tự tính. Khi cả hai đọc cùng cột `has_error`, `credit_risk_tier`... → số liệu luôn khớp.

### Latest state only

Dimensions chỉ lấy record active hiện tại từ SCD2 satellites (`effective_to = '9999-12-31'`). Lịch sử nằm trong Silver.

### Incremental theo ngày giao dịch

Airflow truyền `run_date` xuống dbt. `fact_transaction` filter theo `transaction_datetime` trong `sat_transaction_detail`, xử lý giao dịch của đúng ngày đó.

---

## 3. Định nghĩa bảng chi tiết

### 3.1. `gold.dim_customer`

Nguồn: `hub_customer JOIN sat_customer_profile WHERE effective_to = '9999-12-31'`

| Cột | Kiểu | Mô tả | Nguồn Silver | Dashboard |
|---|---|---|---|---|
| `customer_key` | `INT IDENTITY` | **PK** surrogate | auto | — |
| `customer_id` | `INT` | natural key, merge key | `hub_customer.customer_id` | — |
| `hk_customer` | `CHAR(32)` | traceability về Silver | `hub_customer.hk_customer` | — |
| `gender` | `NVARCHAR(20)` | giới tính | `sat_customer_profile.gender` | CD1 |
| `address` | `NVARCHAR(255)` | địa chỉ | `sat_customer_profile.address` | CD1 |
| `yearly_income` | `DECIMAL(12,2)` | thu nhập hàng năm | `sat_customer_profile.yearly_income` | CD1, CD2 |
| `credit_score` | `INT` | điểm tín dụng | `sat_customer_profile.credit_score` | CD1, CD2 |
| `current_age` | `INT` | tuổi hiện tại | `sat_customer_profile.other_attributes→current_age` | CD1 |
| `retirement_age` | `INT` | tuổi nghỉ hưu | `sat_customer_profile.other_attributes→retirement_age` | CD1 |
| `birth_year` | `INT` | năm sinh | `sat_customer_profile.other_attributes→birth_year` | CD1 |
| `birth_month` | `INT` | tháng sinh | `sat_customer_profile.other_attributes→birth_month` | CD1 |
| `latitude` | `DECIMAL(10,6)` | vĩ độ (cast từ string) | `sat_customer_profile.other_attributes→latitude` | CD1 |
| `longitude` | `DECIMAL(10,6)` | kinh độ (cast từ string) | `sat_customer_profile.other_attributes→longitude` | CD1 |
| `per_capita_income` | `DECIMAL(12,2)` | thu nhập bình quân | `sat_customer_profile.other_attributes→per_capita_income` | CD1 |
| `total_debt` | `DECIMAL(12,2)` | tổng nợ | `sat_customer_profile.other_attributes→total_debt` | CD1, CD2 |
| `num_credit_cards` | `INT` | số thẻ tín dụng | `sat_customer_profile.other_attributes→num_credit_cards` | CD1 |
| `debt_to_income_ratio` | `DECIMAL(6,4)` | **computed**: `total_debt / NULLIF(yearly_income, 0)` | tính toán | CD2 |
| `income_segment` | `NVARCHAR(20)` | **computed**: phân khúc thu nhập | tính toán | CD1 |
| `credit_risk_tier` | `NVARCHAR(20)` | **computed**: hạng rủi ro tín dụng | tính toán | CD1, CD2 |
| `years_to_retirement` | `INT` | **computed**: `retirement_age - current_age` | tính toán | CD1 |
| `dbt_updated_at` | `DATETIME2` | audit timestamp | pipeline | — |

**Logic computed columns:**

```sql
-- income_segment: phân 4 nhóm để CD1 phân khúc marketing
CASE
    WHEN yearly_income < 30000  THEN 'Low'
    WHEN yearly_income < 60000  THEN 'Medium'
    WHEN yearly_income < 100000 THEN 'High'
    ELSE 'Premium'
END

-- credit_risk_tier: phân hạng rủi ro cho cả CD1 (up-sell) lẫn CD2 (hàng rào)
CASE
    WHEN credit_score >= 750 THEN 'Excellent'
    WHEN credit_score >= 670 THEN 'Good'
    WHEN credit_score >= 580 THEN 'Fair'
    ELSE 'Poor'
END

-- debt_to_income_ratio: chỉ số cốt lõi CD2 — tỷ lệ > 0.4 = nguy hiểm
total_debt / NULLIF(yearly_income, 0)
```

> **Lưu ý về `other_attributes`**: Trong Silver, `sat_customer_profile` có cột `other_attributes` kiểu STRING chứa các attributes phụ. Gold layer cần **parse/extract** các trường từ đó (nếu lưu dạng JSON thì dùng `JSON_VALUE()`; nếu lưu dạng cột riêng thì map trực tiếp). Danh sách trên dựa trên Bronze schema gốc (`users_mns`). Điều chỉnh mapping tùy cách Silver serialize `other_attributes`.

**Materialization:**

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    schema='gold',
    tags=['gold','dim']
) }}
```

---

### 3.2. `gold.dim_card`

Nguồn: `hub_card JOIN sat_card_detail (active) JOIN link_customer_card JOIN hub_customer`

| Cột | Kiểu | Mô tả | Nguồn Silver | Dashboard |
|---|---|---|---|---|
| `card_key` | `INT IDENTITY` | **PK** surrogate | auto | — |
| `card_id` | `INT` | natural key, merge key | `hub_card.card_id` | — |
| `hk_card` | `CHAR(32)` | traceability | `hub_card.hk_card` | — |
| `customer_id` | `INT` | **denormalized** FK logic | `hub_customer.customer_id` via `link_customer_card` | CD1, CD2 |
| `card_brand` | `NVARCHAR(50)` | thương hiệu thẻ (Visa, MC...) | `sat_card_detail.card_brand` | CD1 |
| `card_type` | `NVARCHAR(50)` | loại thẻ (credit/debit) | `sat_card_detail.card_type` | CD1 |
| `credit_limit` | `DECIMAL(12,2)` | hạn mức tín dụng | `sat_card_detail.credit_limit` | CD1, CD2 |
| `expires` | `DATE` | ngày hết hạn | `sat_card_detail.expires` | CD2 |
| `has_chip` | `NVARCHAR(10)` | có chip không | `sat_card_detail.other_attributes→has_chip` | CD2 |
| `num_cards_issued` | `INT` | số thẻ đã phát hành | `sat_card_detail.other_attributes→num_cards_issued` | CD1 |
| `acct_open_date` | `DATE` | ngày mở tài khoản | `sat_card_detail.other_attributes→acct_open_date` | CD1 |
| `year_pin_last_changed` | `INT` | năm đổi PIN gần nhất | `sat_card_detail.other_attributes→year_pin_last_changed` | CD2 |
| `card_age_years` | `INT` | **computed**: năm từ khi mở thẻ | `DATEDIFF(YEAR, acct_open_date, GETDATE())` | CD1 |
| `pin_age_years` | `INT` | **computed**: năm chưa đổi PIN | `YEAR(GETDATE()) - year_pin_last_changed` | CD2 |
| `dbt_updated_at` | `DATETIME2` | audit | pipeline | — |

> **Loại bỏ khỏi Gold**: `card_number` và `cvv` — dữ liệu nhạy cảm PCI, không cần cho dashboard phân tích.

> **`customer_id` trong dim_card**: Đây là denormalization có chủ đích (outrigger pattern). Power BI cần trường này để tạo relationship dim_card ↔ dim_customer mà không phải đi qua fact. Khác với Data Vault nơi quan hệ này chỉ nằm trong `link_customer_card`.

**Materialization:** `incremental`, `merge` on `card_id`.

---

### 3.3. `gold.dim_merchant`

Nguồn: derived từ `hub_merchant + link_transaction + sat_transaction_detail` (Silver không có bảng/satellite merchants riêng).

| Cột | Kiểu | Mô tả | Nguồn Silver | Dashboard |
|---|---|---|---|---|
| `merchant_key` | `INT IDENTITY` | **PK** surrogate | auto | — |
| `merchant_id` | `INT` | natural key, merge key | `hub_merchant.merchant_id` | — |
| `hk_merchant` | `CHAR(32)` | traceability | `hub_merchant.hk_merchant` | — |
| `merchant_city` | `NVARCHAR(100)` | thành phố (latest) | `sat_transaction_detail.merchant_city` | CD2 |
| `merchant_state` | `NVARCHAR(50)` | tiểu bang (latest) | `sat_transaction_detail.merchant_state` | CD2 |
| `zip` | `NVARCHAR(10)` | zip code (latest) | `sat_transaction_detail.zip` | CD2 |
| `dbt_updated_at` | `DATETIME2` | audit | pipeline | — |

**Logic lấy location mới nhất:**

```sql
-- Mỗi merchant có thể xuất hiện với nhiều location qua các giao dịch
-- Gold lấy location từ giao dịch gần nhất
WITH ranked AS (
    SELECT
        hm.merchant_id,
        hm.hk_merchant,
        std.merchant_city,
        std.merchant_state,
        std.zip,
        ROW_NUMBER() OVER (
            PARTITION BY hm.merchant_id
            ORDER BY std.transaction_datetime DESC
        ) AS rn
    FROM silver.hub_merchant hm
    JOIN silver.link_transaction lt ON hm.hk_merchant = lt.hk_merchant
    JOIN silver.sat_transaction_detail std ON lt.hk_transaction = std.hk_transaction
)
SELECT * FROM ranked WHERE rn = 1
```

> **Trade-off**: Dashboard CD2 cũng có `merchant_city/state/zip` trực tiếp trong `fact_transaction` (degenerate dimension) nếu cần location tại thời điểm giao dịch cụ thể.

**Materialization:** `incremental`, `merge` on `merchant_id`.

---

### 3.4. `gold.dim_mcc`

Nguồn: `hub_mcc JOIN sat_mcc_detail`

| Cột | Kiểu | Mô tả | Nguồn Silver | Dashboard |
|---|---|---|---|---|
| `mcc_key` | `INT IDENTITY` | **PK** surrogate | auto | — |
| `mcc_id` | `INT` | natural key, merge key | `hub_mcc.mcc_id` | — |
| `hk_mcc` | `CHAR(32)` | traceability | `hub_mcc.hk_mcc` | — |
| `mcc_description` | `NVARCHAR(255)` | mô tả ngành hàng | `sat_mcc_detail.description` | CD1, CD2 |
| `dbt_updated_at` | `DATETIME2` | audit | pipeline | — |

**Materialization:** `incremental`, `merge` on `mcc_id`.

---

### 3.5. `gold.dim_date`

Bảng thời gian — **tạo sẵn bằng dbt seed** (file CSV), phạm vi **1/1/2022 → 31/10/2024** khớp với khoảng thời gian dữ liệu transaction.

| Cột | Kiểu | Mô tả | Dashboard |
|---|---|---|---|
| `date_key` | `INT` | **PK**, format YYYYMMDD (vd: 20220101) | — |
| `full_date` | `DATE` | ngày đầy đủ | CD1, CD2 |
| `year` | `INT` | năm (2022, 2023, 2024) | CD1, CD2 |
| `quarter` | `INT` | quý (1-4) | CD1, CD2 |
| `month` | `INT` | tháng (1-12) | CD1, CD2 |
| `month_name` | `NVARCHAR(20)` | tên tháng (January...) | CD1, CD2 |
| `day_of_month` | `INT` | ngày trong tháng (1-31) | CD2 |
| `day_of_week` | `INT` | thứ trong tuần (1=Mon...7=Sun) | CD2 |
| `day_name` | `NVARCHAR(20)` | tên thứ (Monday...) | CD2 |
| `is_weekend` | `BIT` | 1 nếu thứ 7 hoặc CN | CD2 |

**Materialization:** `seed` — tạo file `seeds/dim_date.csv` chứa 1035 dòng (từ 2022-01-01 đến 2024-10-31). Chỉ chạy `dbt seed` 1 lần.

---

### 3.6. `gold.fact_transaction`

Bảng fact chính — grain: **1 row = 1 giao dịch** (157k dòng tổng cộng).

Nguồn: `link_transaction JOIN sat_transaction_detail + lookup surrogate keys từ dim_*`

| Cột | Kiểu | Mô tả | Nguồn Silver | Dashboard |
|---|---|---|---|---|
| `transaction_key` | `INT IDENTITY` | **PK** surrogate | auto | — |
| `transaction_id` | `INT` | natural key (degenerate dim) | `hub_transaction.transaction_id` via `link_transaction.hk_transaction` | — |
| `date_key` | `INT` | **FK** → dim_date | `CONVERT(INT, FORMAT(transaction_datetime, 'yyyyMMdd'))` | CD1, CD2 |
| `customer_key` | `INT` | **FK** → dim_customer | lookup `dim_customer` via `link_transaction.hk_customer` → `hub_customer.customer_id` | CD1, CD2 |
| `card_key` | `INT` | **FK** → dim_card | lookup `dim_card` via `link_transaction.hk_card` → `hub_card.card_id` | CD1, CD2 |
| `merchant_key` | `INT` | **FK** → dim_merchant | lookup `dim_merchant` via `link_transaction.hk_merchant` → `hub_merchant.merchant_id` | CD2 |
| `mcc_key` | `INT` | **FK** → dim_mcc | lookup `dim_mcc` via `link_transaction.hk_mcc` → `hub_mcc.mcc_id` | CD1, CD2 |
| `transaction_datetime` | `DATETIME` | thời điểm giao dịch | `sat_transaction_detail.transaction_datetime` | CD2 |
| `amount` | `DECIMAL(10,2)` | **measure**: số tiền giao dịch | `sat_transaction_detail.amount` | CD1, CD2 |
| `use_chip` | `NVARCHAR(50)` | phương thức giao dịch | `sat_transaction_detail.use_chip` | CD2 |
| `merchant_city` | `NVARCHAR(100)` | thành phố merchant (degenerate) | `sat_transaction_detail.merchant_city` | CD2 |
| `merchant_state` | `NVARCHAR(50)` | tiểu bang (degenerate) | `sat_transaction_detail.merchant_state` | CD2 |
| `zip` | `NVARCHAR(10)` | zip code (degenerate) | `sat_transaction_detail.zip` | CD2 |
| `errors` | `NVARCHAR(100)` | mô tả lỗi gốc | `sat_transaction_detail.errors` | CD2 |
| `has_error` | `BIT` | **computed**: 1 nếu có lỗi | `CASE WHEN errors IS NOT NULL AND errors <> '' THEN 1 ELSE 0 END` | CD2 |
| `is_chip_used` | `BIT` | **computed**: 1 nếu dùng chip | `CASE WHEN use_chip = 'Chip Transaction' THEN 1 ELSE 0 END` | CD2 |
| `dbt_updated_at` | `DATETIME2` | audit | pipeline | — |

> **Degenerate dimensions**: `merchant_city/state/zip` giữ trực tiếp trong fact vì `dim_merchant` chỉ lưu location mới nhất. CD2 cần phân tích lỗi theo location chính xác tại thời điểm giao dịch — dùng cột trong fact, không phải dim_merchant.

> **`has_error` và `is_chip_used`**: Đây là 2 computed columns quan trọng nhất cho chatbot consistency. Khi dashboard Power BI filter theo `has_error = 1` và chatbot cũng filter cùng cột → số liệu khớp. Không ai tự tính lại công thức.

**Materialization — incremental theo ngày giao dịch:**

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    schema='gold',
    tags=['gold','fact']
) }}

-- ... SELECT ...

{% if is_incremental() %}
WHERE CAST(std.transaction_datetime AS DATE) = CAST('{{ var("run_date") }}' AS DATE)
{% endif %}
```

Airflow gọi: `dbt run --select tag:fact --vars '{"run_date":"2024-03-15"}'`

Mỗi run xử lý giao dịch đúng 1 ngày. `delete+insert` đảm bảo idempotent — retry không tạo duplicate.

---

## 4. Pipeline Airflow

### DAG task order

```
extract_bronze (by date)
  → compute_mns (I/U/D flags)
    → dbt_silver (hub → link → sat)
      → dbt_gold_dim (merge current state)
        → dbt_gold_fact (incremental by transaction date)
          → dbt_test (quality gate)
```

### Backfill

Với 157k transactions từ 1/1/2022 → 31/10/2024 (khoảng 1035 ngày):

```bash
# Backfill toàn bộ lịch sử lần đầu
airflow dags backfill banking_pipeline \
    --start-date 2022-01-01 \
    --end-date 2024-10-31
```

Sau backfill xong, DAG chạy `@daily` cho dữ liệu mới (nếu có).

---

## 5. Chatbot Integration

### Cách chatbot query Gold

Chatbot (LLM text-to-SQL) query **trực tiếp dim + fact**. Với 157k dòng, mọi query chạy dưới 1 giây.

**Ví dụ câu hỏi → SQL mà chatbot sinh:**

```sql
-- "Tỷ lệ lỗi giao dịch theo quý năm 2023?"
SELECT d.year, d.quarter,
       COUNT(*) AS total_txn,
       SUM(CAST(f.has_error AS INT)) AS error_count,
       CAST(SUM(CAST(f.has_error AS INT)) AS DECIMAL) / COUNT(*) AS error_rate
FROM gold.fact_transaction f
JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE d.year = 2023
GROUP BY d.year, d.quarter
ORDER BY d.quarter;

-- "Top 5 MCC có nhiều lỗi nhất?"
SELECT TOP 5 m.mcc_description,
       SUM(CAST(f.has_error AS INT)) AS error_count
FROM gold.fact_transaction f
JOIN gold.dim_mcc m ON f.mcc_key = m.mcc_key
GROUP BY m.mcc_description
ORDER BY error_count DESC;

-- "Bao nhiêu KH thuộc nhóm rủi ro Poor có thu nhập Premium?"
SELECT COUNT(*) AS customer_count
FROM gold.dim_customer
WHERE credit_risk_tier = 'Poor' AND income_segment = 'Premium';
```

### Tại sao số liệu chatbot khớp dashboard?

Vì cả hai đều đọc cùng cột `has_error`, `credit_risk_tier`, `income_segment`... — các cột này được tính 1 lần trong dbt khi load Gold, không ai tính lại. Power BI DAX measures chỉ SUM/COUNT trên các cột có sẵn, chatbot SQL cũng vậy.

---

## 6. Dashboard ↔ Bảng Gold Mapping

### CD1: Phân khúc KH & Marketing cá nhân hóa

| Câu hỏi phân tích | Bảng | Cột |
|---|---|---|
| Phân bổ KH theo thu nhập | `dim_customer` | `income_segment` |
| KH tiềm năng up-sell premium | `dim_customer` + `dim_card` | `credit_risk_tier` = Excellent + `credit_limit` thấp |
| KH có khả năng trả nợ tốt | `dim_customer` | `debt_to_income_ratio` < 0.3 |
| Chi tiêu theo nhóm KH | `fact` + `dim_customer` | `amount` group by `income_segment` |
| Chi tiêu theo ngành hàng | `fact` + `dim_mcc` | `amount` group by `mcc_description` |
| KH sắp nghỉ hưu (bảo hiểm) | `dim_customer` | `years_to_retirement` < 5 |

### CD2: Phân tích giao dịch & Quản trị rủi ro

| Câu hỏi phân tích | Bảng | Cột |
|---|---|---|
| Tỷ lệ lỗi theo thời gian | `fact` + `dim_date` | `has_error`, `year`, `month` |
| Lỗi theo thẻ chip / không chip | `fact` + `dim_card` | `has_error`, `has_chip`, `is_chip_used` |
| MCC nhiều lỗi nhất | `fact` + `dim_mcc` | `has_error`, `mcc_description` |
| Lỗi theo khu vực | `fact` | `has_error`, `merchant_state` |
| Tương quan PIN cũ ↔ lỗi | `fact` + `dim_card` | `has_error`, `pin_age_years` |
| KH rủi ro vỡ nợ | `dim_customer` | `debt_to_income_ratio` > 0.4 + `credit_risk_tier` = Poor |
| Giao dịch cuối tuần | `fact` + `dim_date` | `amount`, `is_weekend` |

---

## 7. Thống kê

| Loại | Bảng | Materialization | Rows (ước tính) |
|---|---|---|---|
| Dimension | dim_customer | incremental merge | ~2k |
| Dimension | dim_card | incremental merge | ~6k |
| Dimension | dim_merchant | incremental merge | ~500 |
| Dimension | dim_mcc | incremental merge | ~100 |
| Dimension | dim_date | seed (static) | 1,035 |
| Fact | fact_transaction | incremental delete+insert | 157,000 |
| **Tổng** | **6 bảng** | | |

### Pipeline toàn cảnh

```
SQL Server source (daily)
  → Bronze TDY/PDY/MNS (17 bảng)
    → Silver Data Vault (11 bảng: 5 hub + 2 link + 4 sat)
      → Gold Star Schema (6 bảng: 5 dim + 1 fact)
        ├→ Power BI: CD1 (phân khúc KH) + CD2 (rủi ro)
        └→ Chatbot: query dim + fact trực tiếp
```

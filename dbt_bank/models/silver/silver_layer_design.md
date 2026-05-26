# Silver Layer — Data Vault Design & Mapping Rules

## 1. Tổng quan thiết kế

Silver layer sử dụng **Data Vault 2.0** với cấu trúc:

- **5 Hub tables** — lưu business keys, không thay đổi sau khi insert
- **2 Link tables** — lưu quan hệ giữa các entity
- **5 Satellite tables** — lưu descriptive attributes, phân loại SCD Type 1 hoặc Type 2

Nguồn dữ liệu: các bảng `*_mns` trong Bronze (chứa `operation_flag` = I/U/D).

Lưu ý: các bảng `*_mns` trong đồ án này là **change-set tối giản** (chỉ chứa business key + `operation_flag`). Khi cần descriptive attributes (ví dụ amount, merchant_city...), Silver sẽ **JOIN** `*_mns` với bảng snapshot `*_tdy` theo business key.

Tất cả dữ liệu chỉ đến từ **một source duy nhất** (SQL Server), nên `record_source` luôn là `'bronze.{table_name}'`.

---

## 2. Quy ước đặt tên

| Thành phần | Prefix | Ví dụ |
|---|---|---|
| Hub | `hub_` | `hub_customer` |
| Link | `link_` | `link_transaction` |
| Satellite SCD1 | `sat_` | `sat_transaction_detail` |
| Satellite SCD2 | `sat_` | `sat_customer_profile` |
| Hash key | `hk_` | `hk_customer` |
| Hash diff | `hashdiff` | cột cố định |

**Schema**: tất cả bảng Silver nằm trong schema `silver`.

---

## 3. Quy tắc Hash

### Hash Key (HK)

```sql
-- Công thức chung
hk_<entity> = UPPER(CONVERT(CHAR(32), HASHBYTES('MD5', 
    UPPER(TRIM(CAST(<business_key> AS NVARCHAR(100))))
), 2))
```

| Hash Key | Business Key | Nguồn |
|---|---|---|
| `hk_customer` | `customer_id` | `users_mns.id` |
| `hk_card` | `card_id` | `cards_mns.id` |
| `hk_transaction` | `transaction_id` | `transactions_mns.id` |
| `hk_merchant` | `merchant_id` | `transactions_tdy.merchant_id` (join `transactions_mns` on `id`) |
| `hk_mcc` | `mcc_id` | `mcc_codes_mns.mcc_id` |

### Hash Key cho Link

```sql
-- Link key = HASH(concatenation các business keys thành phần)
hk_customer_card = HASH(customer_id || '||' || card_id)
hk_transaction_link = HASH(transaction_id || '||' || customer_id || '||' || card_id || '||' || merchant_id || '||' || mcc_id)
```

### HASHDIFF (chỉ dùng cho SCD Type 2)

```sql
-- HASHDIFF = HASH(tất cả descriptive attributes, phân cách bằng '||')
-- Dùng để phát hiện thay đổi giữa các lần load
hashdiff = HASH(UPPER(TRIM(col1)) || '||' || UPPER(TRIM(col2)) || '||' || ...)
```

---

## 4. Định nghĩa bảng chi tiết

### 4.1. HUB TABLES

#### `silver.hub_customer`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_customer` | `CHAR(32)` | **PK**. MD5 hash của `customer_id` |
| `customer_id` | `INT` | Business key — từ `users_mns.id` |
| `load_datetime` | `DATETIME2` | Thời điểm record được load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.users_mns'` |

**Load rule**: INSERT nếu `hk_customer` chưa tồn tại. Không bao giờ UPDATE hay DELETE hub.

#### `silver.hub_card`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_card` | `CHAR(32)` | **PK**. MD5 hash của `card_id` |
| `card_id` | `INT` | Business key — từ `cards_mns.id` |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.cards_mns'` |

#### `silver.hub_transaction`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_transaction` | `CHAR(32)` | **PK**. MD5 hash của `transaction_id` |
| `transaction_id` | `INT` | Business key — từ `transactions_mns.id` |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.transactions_mns'` |

#### `silver.hub_merchant`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_merchant` | `CHAR(32)` | **PK**. MD5 hash của `merchant_id` |
| `merchant_id` | `INT` | Business key — từ `transactions_tdy.merchant_id` (join `transactions_mns` on `id`) |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.transactions_tdy'` |

> **Lưu ý**: `hub_merchant` không có bảng master merchants riêng trong Bronze. Business key `merchant_id` được lấy từ `transactions_tdy` cho các transaction có `operation_flag = 'I'` (join với `transactions_mns`).

#### `silver.hub_mcc`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_mcc` | `CHAR(32)` | **PK**. MD5 hash của `mcc_id` |
| `mcc_id` | `INT` | Business key — từ `mcc_codes_mns.mcc_id` |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.mcc_codes_mns'` |

---

### 4.2. LINK TABLES

#### `silver.link_customer_card`

Quan hệ card thuộc về customer. Nguồn: `cards_mns.client_id` → `cards_mns.id`.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_customer_card` | `CHAR(32)` | **PK**. HASH(`customer_id \|\| card_id`) |
| `hk_customer` | `CHAR(32)` | FK → `hub_customer` |
| `hk_card` | `CHAR(32)` | FK → `hub_card` |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.cards_mns'` |

**Load rule**: INSERT nếu `hk_customer_card` chưa tồn tại.

#### `silver.link_transaction`

Transactional link — capture sự kiện giao dịch kết nối đồng thời 4 entity: customer, card, merchant, mcc.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `hk_transaction_link` | `CHAR(32)` | **PK**. HASH(`transaction_id \|\| customer_id \|\| card_id \|\| merchant_id \|\| mcc_id`) |
| `hk_transaction` | `CHAR(32)` | FK → `hub_transaction` |
| `hk_customer` | `CHAR(32)` | FK → `hub_customer` |
| `hk_card` | `CHAR(32)` | FK → `hub_card` |
| `hk_merchant` | `CHAR(32)` | FK → `hub_merchant` |
| `hk_mcc` | `CHAR(32)` | FK → `hub_mcc` |
| `load_datetime` | `DATETIME2` | Thời điểm load lần đầu |
| `record_source` | `NVARCHAR(50)` | `'bronze.transactions_tdy'` |

> **Lý do gộp 1 link thay vì 4 link riêng**: Một transaction là một sự kiện kinh doanh duy nhất, luôn gắn đồng thời với 1 customer, 1 card, 1 merchant, 1 mcc. Tách 4 link riêng tạo overhead không cần thiết (4x JOIN khi query Gold) mà không mang lại lợi ích thực tế cho quy mô đồ án. Trong Data Vault 2.0, multi-key transactional link là cách tiếp cận hợp lệ và phổ biến.

---

### 4.3. SATELLITE TABLES

#### `silver.sat_customer_profile` — SCD Type 2

Track lịch sử thay đổi thông tin khách hàng. Khi attributes thay đổi → đóng record cũ (`effective_to`) → insert record mới.

| Cột | Kiểu | Mô tả | Nguồn Bronze |
|---|---|---|---|
| `hk_customer` | `CHAR(32)` | **PK (part 1)**, FK → `hub_customer` | HASH(`users_mns.id`) |
| `effective_from` | `DATETIME2` | **PK (part 2)**. Thời điểm record có hiệu lực | `load_datetime` |
| `effective_to` | `DATETIME2` | Thời điểm hết hiệu lực. `'9999-12-31'` = active | Set khi có record mới |
| `hashdiff` | `CHAR(32)` | Hash của tất cả attributes bên dưới | Computed |
| `current_age` | `INT` | Tuổi hiện tại | `users_mns.current_age` |
| `retirement_age` | `INT` | Tuổi nghỉ hưu | `users_mns.retirement_age` |
| `birth_year` | `INT` | Năm sinh | `users_mns.birth_year` |
| `birth_month` | `INT` | Tháng sinh | `users_mns.birth_month` |
| `gender` | `NVARCHAR(20)` | Giới tính | `users_mns.gender` |
| `address` | `NVARCHAR(255)` | Địa chỉ | `users_mns.address` |
| `latitude` | `NVARCHAR(20)` | Vĩ độ | `users_mns.latitude` |
| `longitude` | `NVARCHAR(20)` | Kinh độ | `users_mns.longitude` |
| `per_capita_income` | `DECIMAL(12,2)` | Thu nhập bình quân đầu người | `users_mns.per_capita_income` |
| `yearly_income` | `DECIMAL(12,2)` | Thu nhập hàng năm | `users_mns.yearly_income` |
| `total_debt` | `DECIMAL(12,2)` | Tổng nợ | `users_mns.total_debt` |
| `credit_score` | `INT` | Điểm tín dụng | `users_mns.credit_score` |
| `num_credit_cards` | `INT` | Số thẻ tín dụng | `users_mns.num_credit_cards` |
| `record_source` | `NVARCHAR(50)` | | `'bronze.users_mns'` |

**HASHDIFF formula**:
```sql
HASH(current_age || '||' || retirement_age || '||' || birth_year || '||' || 
     birth_month || '||' || gender || '||' || address || '||' || 
     latitude || '||' || longitude || '||' || per_capita_income || '||' || 
     yearly_income || '||' || total_debt || '||' || credit_score || '||' || 
     num_credit_cards)
```

#### `silver.sat_card_detail` — SCD Type 2

Track lịch sử thay đổi thông tin thẻ.

| Cột | Kiểu | Mô tả | Nguồn Bronze |
|---|---|---|---|
| `hk_card` | `CHAR(32)` | **PK (part 1)**, FK → `hub_card` | HASH(`cards_mns.id`) |
| `effective_from` | `DATETIME2` | **PK (part 2)** | `load_datetime` |
| `effective_to` | `DATETIME2` | `'9999-12-31'` = active | Set khi có record mới |
| `hashdiff` | `CHAR(32)` | Hash của tất cả attributes bên dưới | Computed |
| `card_brand` | `NVARCHAR(50)` | Thương hiệu thẻ | `cards_mns.card_brand` |
| `card_type` | `NVARCHAR(50)` | Loại thẻ | `cards_mns.card_type` |
| `card_number` | `NVARCHAR(20)` | Số thẻ | `cards_mns.card_number` |
| `expires` | `DATE` | Ngày hết hạn | `cards_mns.expires` |
| `cvv` | `NVARCHAR(10)` | CVV | `cards_mns.cvv` |
| `has_chip` | `NVARCHAR(10)` | Có chip không | `cards_mns.has_chip` |
| `num_cards_issued` | `INT` | Số thẻ đã phát hành | `cards_mns.num_cards_issued` |
| `credit_limit` | `DECIMAL(12,2)` | Hạn mức tín dụng | `cards_mns.credit_limit` |
| `acct_open_date` | `DATE` | Ngày mở tài khoản | `cards_mns.acct_open_date` |
| `year_pin_last_changed` | `INT` | Năm đổi PIN gần nhất | `cards_mns.year_pin_last_changed` |
| `record_source` | `NVARCHAR(50)` | | `'bronze.cards_mns'` |

> **Lưu ý quan trọng**: `customer_id` (tức `cards_mns.client_id`) **KHÔNG** nằm trong satellite này. Quan hệ card–customer được capture hoàn toàn qua `link_customer_card`. Đây là nguyên tắc Data Vault: satellite chỉ chứa descriptive attributes, không chứa business key của entity khác.

#### `silver.sat_transaction_detail` — SCD Type 1

Transaction là sự kiện bất biến (immutable event) — sau khi xảy ra không thay đổi. Dùng SCD1 (chỉ giữ latest state, không cần history).

| Cột | Kiểu | Mô tả | Nguồn Bronze |
|---|---|---|---|
| `hk_transaction` | `CHAR(32)` | **PK**, FK → `hub_transaction` | HASH(`transactions_mns.id`) |
| `transaction_datetime` | `DATETIME` | Thời điểm giao dịch | `transactions_tdy.date` (join `transactions_mns`) |
| `amount` | `DECIMAL(10,2)` | Số tiền | `transactions_tdy.amount` (join `transactions_mns`) |
| `use_chip` | `NVARCHAR(50)` | Có dùng chip | `transactions_tdy.use_chip` (join `transactions_mns`) |
| `merchant_city` | `NVARCHAR(100)` | Thành phố merchant | `transactions_tdy.merchant_city` (join `transactions_mns`) |
| `merchant_state` | `NVARCHAR(50)` | Tiểu bang merchant | `transactions_tdy.merchant_state` (join `transactions_mns`) |
| `zip` | `NVARCHAR(10)` | Zip code | `transactions_tdy.zip` (join `transactions_mns`) |
| `errors` | `NVARCHAR(100)` | Lỗi giao dịch (nếu có) | `transactions_tdy.errors` (join `transactions_mns`) |
| `load_datetime` | `DATETIME2` | Thời điểm load | Pipeline runtime |
| `record_source` | `NVARCHAR(50)` | | `'bronze.transactions_tdy'` |

> **Quyết định thiết kế**: `merchant_city`, `merchant_state`, `zip` được đặt trong `sat_transaction_detail` thay vì tạo `sat_merchant_profile` riêng. Lý do:
> - Bronze không có bảng merchants riêng — thông tin merchant location nằm trong bảng transactions.
> - Cùng một `merchant_id` có thể xuất hiện với các location khác nhau qua các giao dịch.
> - Đặt trong `sat_transaction_detail` **bảo toàn dữ liệu gốc** mà không cần giả định "1 merchant = 1 location".
> - Gold layer (`dim_merchant`) sẽ aggregate/deduplicate khi cần.

#### `silver.sat_mcc_detail` — SCD Type 1

MCC code descriptions hiếm khi thay đổi, dùng SCD1.

| Cột | Kiểu | Mô tả | Nguồn Bronze |
|---|---|---|---|
| `hk_mcc` | `CHAR(32)` | **PK**, FK → `hub_mcc` | HASH(`mcc_codes_mns.mcc_id`) |
| `description` | `NVARCHAR(255)` | Mô tả MCC | `mcc_codes_mns.description` |
| `load_datetime` | `DATETIME2` | Thời điểm load | Pipeline runtime |
| `record_source` | `NVARCHAR(50)` | | `'bronze.mcc_codes_mns'` |

---

## 5. Xử lý `operation_flag` từ MNS

Bảng MNS có `operation_flag` với 3 giá trị:

| Flag | Ý nghĩa | Xử lý tại Hub | Xử lý tại Link | Xử lý tại Satellite SCD2 | Xử lý tại Satellite SCD1 |
|---|---|---|---|---|---|
| `I` (Insert) | Record mới | INSERT nếu HK chưa tồn tại | INSERT nếu HK chưa tồn tại | INSERT record mới, `effective_to = '9999-12-31'` | INSERT record mới |
| `U` (Update) | Record thay đổi | Bỏ qua (hub không update) | Bỏ qua (link không update) | So sánh `hashdiff`: nếu khác → đóng record cũ (`effective_to = now`) + INSERT record mới | UPDATE trực tiếp |
| `D` (Delete) | Record bị xóa | Bỏ qua (hub không xóa) | Bỏ qua (link không xóa) | Đóng record hiện tại: `effective_to = now` | Đánh dấu soft delete hoặc bỏ qua tùy business rule |

### Xử lý Delete cho SCD Type 2

```sql
-- Khi operation_flag = 'D', đóng record active hiện tại
UPDATE silver.sat_customer_profile
SET effective_to = SYSUTCDATETIME()
WHERE hk_customer = @hk_customer
  AND effective_to = '9999-12-31 00:00:00';
```

### Xử lý Delete cho SCD Type 1

Transactions là sự kiện đã xảy ra → **không xóa** trong Silver. Nếu Bronze gửi flag D cho transaction, Silver bỏ qua (hoặc log warning).

MCC codes bị xóa → UPDATE `is_deleted = 1` nếu cần, hoặc bỏ qua vì MCC codes hiếm khi bị xóa.

---

## 6. Load Pattern — Thứ tự thực hiện

Mỗi lần pipeline chạy, dbt models thực thi theo thứ tự:

```
Bước 1: Load Hubs (song song, không phụ thuộc nhau)
  ├── hub_customer    ← users_mns (flag I)
  ├── hub_card        ← cards_mns (flag I)
  ├── hub_transaction ← transactions_mns (flag I)
  ├── hub_merchant    ← transactions_tdy join transactions_mns (flag I, DISTINCT merchant_id)
  └── hub_mcc         ← mcc_codes_mns (flag I)

Bước 2: Load Links (phụ thuộc Hubs)
  ├── link_customer_card ← cards_mns (flag I)
  └── link_transaction   ← transactions_tdy join transactions_mns (flag I)

Bước 3: Load Satellites (phụ thuộc Hubs)
  ├── sat_customer_profile  ← users_mns (flag I, U, D)
  ├── sat_card_detail       ← cards_mns (flag I, U, D)
  ├── sat_transaction_detail ← transactions_tdy join transactions_mns (flag I, U)
  └── sat_mcc_detail         ← mcc_codes_mns (flag I, U)
```

---

## 7. Mapping tổng hợp Bronze → Silver

### 7.1. `users_mns` → Silver

```
users_mns.id ──────────────────→ hub_customer.customer_id
                                  (HASH → hk_customer)

users_mns.{all attributes} ────→ sat_customer_profile.*
                                  (SCD2: compare hashdiff)
```

### 7.2. `cards_mns` → Silver

```
cards_mns.id ──────────────────→ hub_card.card_id
                                  (HASH → hk_card)

cards_mns.client_id ───────────→ hub_customer.customer_id  (nếu chưa tồn tại)
                                  (HASH → hk_customer)

cards_mns.(client_id, id) ─────→ link_customer_card
                                  (hk_customer, hk_card)

cards_mns.{card attributes} ───→ sat_card_detail.*
                                  (SCD2: compare hashdiff)
                                  ⚠ KHÔNG bao gồm client_id
```

### 7.3. `transactions_mns` → Silver

```
transactions_mns.id ───────────→ hub_transaction.transaction_id
                                  (HASH → hk_transaction)

transactions_tdy.client_id ────→ hub_customer.customer_id  (nếu chưa tồn tại)
  (join transactions_mns on id)
transactions_tdy.card_id ──────→ hub_card.card_id           (nếu chưa tồn tại)
  (join transactions_mns on id)
transactions_tdy.merchant_id ──→ hub_merchant.merchant_id   (nếu chưa tồn tại)
  (join transactions_mns on id)
transactions_tdy.mcc ──────────→ hub_mcc.mcc_id             (nếu chưa tồn tại)
  (join transactions_mns on id)

transactions_tdy.(id, client_id, 
  card_id, merchant_id, mcc) ──→ link_transaction
  (join transactions_mns on id; flag I)
                                  (hk_transaction, hk_customer, 
                                   hk_card, hk_merchant, hk_mcc)

transactions_tdy.(date, amount, 
  use_chip, merchant_city, 
  merchant_state, zip, errors) → sat_transaction_detail.*
  (join transactions_mns on id; flag I/U)
                                  (SCD1: INSERT or UPDATE)
```

### 7.4. `mcc_codes_mns` → Silver

```
mcc_codes_mns.mcc_id ─────────→ hub_mcc.mcc_id
                                  (HASH → hk_mcc)

mcc_codes_mns.description ────→ sat_mcc_detail.description
                                  (SCD1: INSERT or UPDATE)
```

---

## 8. Tổng kết so với thiết kế ban đầu

| Thay đổi | Lý do |
|---|---|
| Xóa `customer_id` khỏi `sat_card_detail` | Vi phạm Data Vault — business key của entity khác thuộc về link, không phải satellite |
| Gộp 4 transaction links → 1 `link_transaction` | Transaction là 1 event duy nhất kết nối 4 entity; gộp giảm JOIN khi query Gold |
| Xóa `sat_merchant_profile`, chuyển merchant location vào `sat_transaction_detail` | Không có bảng merchants riêng trong Bronze; location là context của transaction, không phải attribute của merchant |
| Giữ `hub_merchant` nhưng không tạo satellite riêng | Hub vẫn cần để identify merchant; Gold layer sẽ derive `dim_merchant` từ `sat_transaction_detail` |
| Bổ sung xử lý `operation_flag = D` | Thiết kế ban đầu không thể hiện cách handle soft delete |
| Thêm `record_source` vào tất cả bảng | Bắt buộc trong Data Vault 2.0 — traceability |

---

## 9. Thống kê cuối cùng

| Loại | Số lượng | Bảng |
|---|---|---|
| Hub | 5 | hub_customer, hub_card, hub_transaction, hub_merchant, hub_mcc |
| Link | 2 | link_customer_card, link_transaction |
| Satellite | 4 | sat_customer_profile (SCD2), sat_card_detail (SCD2), sat_transaction_detail (SCD1), sat_mcc_detail (SCD1) |
| **Tổng** | **11** | |

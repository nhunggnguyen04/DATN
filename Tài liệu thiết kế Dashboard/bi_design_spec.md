# Tài Liệu Thiết Kế BI – Power BI Dashboard

## Dự án: Phân tích Giao dịch Thẻ Tín dụng Ngân hàng

**Phiên bản:** 2.2 (giới hạn phạm vi còn CD1, gỡ toàn bộ thiết kế CD2)
**Ngày:** 2026-06-24
**Nguồn dữ liệu:** Gold Layer (Star Schema – dbt, database `DATN`, schema `gold`)
**Công cụ:** Microsoft Power BI Desktop / Power BI Service

> **Changelog v2.2:** Giới hạn chủ đề phân tích chỉ còn **CD1 – Phân khúc Khách hàng & Marketing**. Gỡ bỏ toàn bộ phần thiết kế CD2 (Quản trị Rủi ro / EWS), các measure DAX CD2, vai trò RLS theo rủi ro, các bước build/checklist/hạn chế thuộc CD2 và cột CD2 trong bảng tóm tắt. Đánh số lại các mục.
>
> **Changelog v2.1:** Thêm mục Chuẩn trực quan (bảng màu, lưới bố cục, typography) và bản đánh giá Trang 1 CD1 theo ảnh chụp thực tế kèm bố cục đề xuất.
>
> **Changelog v2.0:** Đối chiếu lại với 6 model gold thực tế (`dim_customer`, `dim_card`, `dim_merchant`, `dim_mcc`, `dim_date`, `fact_transaction`). Sửa: khóa quan hệ là **natural key** (không phải surrogate), `dim_date` là **model động** (không phải seed), gắn cờ khả thi cho từng KPI.

---

## 1. Tổng quan mô hình dữ liệu Gold Layer

```
dim_date ─────┐
dim_customer ──┤
dim_card ──────┼──► fact_transaction (grain: 1 row = 1 giao dịch, ~157k dòng)
dim_merchant ──┤
dim_mcc ───────┘
```

| Bảng                | Loại     | Mô tả                                                     | Khóa nối với fact |
| -------------------- | --------- | ----------------------------------------------------------- | -------------------- |
| `fact_transaction` | Fact      | Bảng sự kiện chính, ~1 row/giao dịch                   | —                   |
| `dim_customer`     | Dimension | Hồ sơ KH, phân khúc thu nhập & rủi ro (current state) | `customer_id`      |
| `dim_card`         | Dimension | Thông tin thẻ, chip, hạn mức, PIN (current state)       | `card_id`          |
| `dim_date`         | Dimension | Bộ lịch (model động, không phải seed)                 | `date_key`         |
| `dim_merchant`     | Dimension | Địa điểm merchant (location mới nhất)                 | `merchant_id`      |
| `dim_mcc`          | Dimension | Danh mục ngành hàng (MCC)                                | `mcc_id`           |

> **Quan trọng (khớp code thật):**
>
> - `fact_transaction` nối với dim bằng **natural key** (`customer_id`, `card_id`, `merchant_id`, `mcc_id`, `date_key`) — **không** dùng surrogate `*_key`. Test `relationships` trong `models/gold/schema.yml` xác nhận điều này.
> - `dim_date` là **model động** (`dim_date.sql`): date-spine từ 2022-01-01 đến 3 năm sau ngày chạy. Cần **Mark as Date Table** theo `full_date` trong Power BI.
> - Các dimension chỉ giữ **trạng thái mới nhất** (SCD2 active). Không có lịch sử của thuộc tính KH ở Gold nên các KPI cần xu hướng theo thời gian của thuộc tính KH không tính trực tiếp được.

### Cột có sẵn để làm KPI (trích từ model thật)

- **dim_customer**: `customer_id, gender, address, yearly_income, credit_score, current_age, retirement_age, birth_year, birth_month, latitude, longitude, per_capita_income, total_debt, num_credit_cards` + computed `debt_to_income_ratio, income_segment, credit_risk_tier, years_to_retirement`.
- **dim_card**: `card_id, customer_id, card_brand, card_type, credit_limit, expires, has_chip, num_cards_issued, acct_open_date, year_pin_last_changed` + computed `card_age_years, pin_age_years`.
- **dim_merchant**: `merchant_id, merchant_city, merchant_state, zip`.
- **dim_mcc**: `mcc_id, mcc_description`.
- **dim_date**: `date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend`.
- **fact_transaction**: `transaction_id, date_key, customer_id, card_id, merchant_id, mcc_id, transaction_datetime, amount, use_chip, merchant_city, merchant_state, zip, errors` + computed `has_error (BIT), is_chip_used (BIT)`.

---

## 2. Chủ đề CD1 – Phân Khúc Khách Hàng & Marketing Cá Nhân Hóa

### 2.1 Mục tiêu nghiệp vụ

> Tìm ra nhóm khách hàng tiềm năng để **up-sell thẻ tín dụng** hoặc **mời chào gói vay phù hợp** dựa trên hành vi chi tiêu, hồ sơ tài chính và phân khúc rủi ro.

### 2.2 Đối tượng sử dụng dashboard

| Vai trò                               | Nhu cầu chính                                                          | Tần suất xem | Trang quan tâm |
| -------------------------------------- | ------------------------------------------------------------------------ | -------------- | --------------- |
| **Product Manager (Thẻ & Vay)** | Xác định segment để thiết kế sản phẩm, định cỡ thị trường | Hàng tuần    | Trang 1         |
| **Marketing Analyst**            | Lọc & export danh sách target campaign (theo `customer_id`)          | Hàng ngày    | Trang 2         |
| **Relationship Manager**         | Xem hành vi nhóm Premium/High để chăm sóc cá nhân hóa           | Theo yêu cầu | Trang 3         |
| **Ban lãnh đạo (C-level)**    | Theo dõi ARPU/giá trị tệp KH theo quý                               | Hàng tháng   | Header trang 1  |

### 2.3 KPI Đề xuất (đã hiệu chỉnh theo cột thật)

Cờ khả thi: ✅ dùng ngay · ⚠️ dùng được nhưng có lưu ý.

#### Nhóm Tổng quan (Header Cards)

| #  | Tên KPI                  | Công thức DAX                                                                                                                      | Ý nghĩa                                                                                              | Cờ  |
| -- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ---- |
| C1 | **KH active**       | `DISTINCTCOUNT('gold fact_transaction'[customer_id])`                                                                              | KH có ≥1 giao dịch trong kỳ lọc                                                                   | ✅   |
| C2 | **Tổng chi tiêu** | `SUM('gold fact_transaction'[amount])`                                                                                             | Doanh số toàn tệp                                                                                   | ✅   |
| C3 | **ARPU**            | `DIVIDE([C2], [C1])`                                                                                                               | Doanh thu bình quân/KH active                                                                        | ✅   |
| C4 | **% KH Premium**    | `DIVIDE(CALCULATE(COUNTROWS('gold dim_customer'), 'gold dim_customer'[income_segment]="Premium"), COUNTROWS('gold dim_customer'))` | Chất lượng tệp KH.**Mẫu số = toàn danh mục**, khác C1 (active) — không so trực tiếp | ⚠️ |
| C5 | **Số thẻ bq/KH**  | `DIVIDE(DISTINCTCOUNT('gold fact_transaction'[card_id]), [C1])`                                                                    | Dư địa up-sell thêm thẻ                                                                           | ✅   |

#### Nhóm Phân khúc & Tiềm năng up-sell

| #   | Tên KPI                                  | Công thức DAX                                                                                                                                                                | Ý nghĩa                                                                                              | Cờ  |
| --- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | ---- |
| C6  | **KH đủ ĐK up-sell thẻ**        | `CALCULATE([Số KH theo Gợi ý], 'Gợi ý Sản phẩm'[Product_Suggestion] = "Up-sell: Thẻ Platinum") + CALCULATE([Số KH theo Gợi ý], 'Gợi ý Sản phẩm'[Product_Suggestion] = "Up-sell: Tăng hạn mức")` | Tái dụng measure `[Số KH theo Gợi ý]` qua bảng phụ `Gợi ý Sản phẩm` — số luôn khớp bar chart | ✅   |
| C7  | **KH đủ ĐK mời vay**            | `CALCULATE([Số KH theo Gợi ý], 'Gợi ý Sản phẩm'[Product_Suggestion] = "Cross-sell: Vay tiêu dùng lãi suất ưu đãi")`                                                  | Tái dụng measure `[Số KH theo Gợi ý]` qua bảng phụ `Gợi ý Sản phẩm` — số luôn khớp bar chart | ✅   |
| C8  | **% KH sắp nghỉ hưu (≤5 năm)** | `DIVIDE(COUNTROWS(FILTER('gold dim_customer', 'gold dim_customer'[years_to_retirement]<=5 && 'gold dim_customer'[years_to_retirement]>=0)), COUNTROWS('gold dim_customer'))` | Segment cho gói tích lũy/bảo hiểm hưu trí                                                       | ✅   |
| C9  | **Tổng dư nợ danh mục**         | `SUM('gold dim_customer'[total_debt])`                                                                                                                                       | Exposure toàn danh mục                                                                               | ✅   |
| C10 | **CLV proxy (12 tháng)**           | `[C3] * 12`                                                                                                                                                                  | **Proxy thô** — ghi rõ giả định; bản tốt hơn: chi tiêu bq tháng × kỳ vọng gắn bó | ⚠️ |

#### Nhóm Hành vi chi tiêu

| #   | Tên KPI                      | Công thức DAX                                                                                                          | Ý nghĩa                                                                                   | Cờ |
| --- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | --- |
| C11 | **Top 3 MCC chi tiêu** | `TOPN(3, VALUES('gold dim_mcc'[mcc_description]), [C2], DESC)`                                                         | Sở thích chi tiêu theo phân khúc                                                       | ✅  |
| C12 | **% GD cuối tuần**    | `DIVIDE(CALCULATE(COUNTROWS('gold fact_transaction'), 'gold dim_date'[is_weekend]=TRUE()), [Total Transactions])`      | Hành vi leisure vs business (lưu ý:`is_weekend` ở **dim_date**, không ở fact) | ✅  |
| C13 | **% dùng chip**        | `DIVIDE(CALCULATE(COUNTROWS('gold fact_transaction'), 'gold fact_transaction'[is_chip_used]=1), [Total Transactions])` | Proxy nhóm KH am hiểu/cẩn trọng                                                         | ✅  |

### 2.4 Thiết kế trang Dashboard CD1

#### Trang 1: "Tổng quan Phân khúc" (Executive View)

```
┌─────────────────────────────────────────────────────────────────┐
│  [SLICER: Năm] [SLICER: Quý] [SLICER: income_segment] [SLICER: credit_risk_tier]  │
├──────────┬──────────┬──────────┬──────────┬────────────────────┤
│  C1      │  C2      │  C3      │  C4      │  C5               │
│ KH Active│ Tổng CT  │ ARPU     │ % Premium│ Thẻ/KH            │
├──────────┴──────────┴──────────┴──────────┴────────────────────┤
│  [Treemap: income_segment × credit_risk_tier]                  │
│   Size = số KH, Màu = tổng chi tiêu                            │
├─────────────────────────┬────────────────────────────────────────┤
│  [Bar: Top 10 MCC theo  │  [Donut: gender × income_segment]     │
│   tổng chi tiêu, phân   │                                        │
│   tách theo segment]    │  [Scatter: yearly_income vs total_debt │
│                         │   bubble = num_credit_cards            │
│                         │   màu = credit_risk_tier]              │
└─────────────────────────┴────────────────────────────────────────┘
```

**Insight mục tiêu:** PM và Marketing xác định được "tứ phân vị" khách hàng ngay từ trang đầu (Treemap + Scatter định vị nhóm up-sell).

#### Trang 2: "Danh sách Mục tiêu Marketing" (Analyst View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: income_segment] [SLICER: credit_risk_tier]           │
│  [SLICER: years_to_retirement range] [SLICER: num_credit_cards]│
├──────────────────────┬──────────────────────┬──────────────────┤
│  KH Up-sell: [C6]    │  KH Mời vay: [C7]    │  CLV tổng        │
├──────────────────────┴──────────────────────┴──────────────────┤
│  [Bar ngang: Số KH theo Gợi ý sản phẩm]                        │
│   Y = Product_Suggestion | X = [Số KH theo Gợi ý]             │
│   Màu xám cho nhãn "Theo dõi – chưa đủ điều kiện"             │
├────────────────────────────────────────────────────────────────┤
│  [Table chi tiết: customer_id | income_segment | credit_risk_tier│
│   | yearly_income | num_credit_cards | debt_to_income_ratio     │
│   | Product_Suggestion]  → Export to Excel cho campaign team    │
├────────────────────────────────────────────────────────────────┤
│  [Map: phân bổ KH theo latitude/longitude]                     │
│   Màu = income_segment, Size = tổng chi tiêu                    │
└────────────────────────────────────────────────────────────────┘
```

> **Luồng logic Trang 2 (quan trọng):**
>
> ```
> Product_Suggestion (Calculated Column trên 'gold dim_customer')
>         │
>         ├── "Up-sell: Thẻ Platinum"             ┐
>         ├── "Up-sell: Tăng hạn mức"             ┘→ C6 (KH Up-sell)
>         │
>         ├── "Cross-sell: Vay tiêu dùng..."      → C7 (KH Mời vay)
>         ├── "Cross-sell: Gói Tích lũy Hưu trí"  → (theo dõi riêng nếu cần)
>         │
>         └── "Theo dõi – chưa đủ điều kiện"      → chưa đủ điều kiện
> ```
>
> - **Bảng phụ `Gợi ý Sản phẩm`** (DATATABLE, không nối quan hệ với fact): chứa 5 nhãn `Product_Suggestion` dùng làm trục Y cho bar chart.
> - **Measure `Số KH theo Gợi ý`**: dùng `SELECTEDVALUE('Gợi ý Sản phẩm'[Product_Suggestion])` + `SWITCH` để đếm KH khớp từng nhãn theo điều kiện gốc.
> - **C6/C7** tái dụng measure trên bằng `CALCULATE([Số KH theo Gợi ý], 'Gợi ý Sản phẩm'[Product_Suggestion] = "...")` → số KPI card luôn khớp bar chart, không viết lại điều kiện.

#### Trang 3: "Hành vi Chi tiêu theo Segment" (Deep-dive)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: income_segment] [SLICER: gender] [SLICER: Tháng]    │
├──────────────────────────┬─────────────────────────────────────┤
│  [Line: xu hướng chi     │  [Heatmap: day_of_week × month     │
│   tiêu theo full_date,   │   vs tổng GD — lọc theo MCC]       │
│   break by segment]      │                                     │
├──────────────────────────┴─────────────────────────────────────┤
│  [Stacked Bar: Top 5 MCC theo từng income_segment]             │
├────────────────────────────────────────────────────────────────┤
│  [Cards] % GD cuối tuần [C12] | % chip [C13]                  │
└────────────────────────────────────────────────────────────────┘
```

### 2.5 Bộ lọc & Slicer CD1

| Slicer                    | Nguồn trường                              | Loại control                       | Ghi chú            |
| ------------------------- | -------------------------------------------- | ----------------------------------- | ------------------- |
| Năm                      | `'gold dim_date'[year]`                    | Dropdown                            | Multi-select        |
| Quý                      | `'gold dim_date'[quarter]`                 | Button (1/2/3/4)                    |                     |
| Tháng                    | `'gold dim_date'[month_name]`              | Dropdown                            |                     |
| Phân khúc thu nhập     | `'gold dim_customer'[income_segment]`      | Checkbox (Low/Medium/High/Premium)  | Mặc định All     |
| Bậc rủi ro tín dụng   | `'gold dim_customer'[credit_risk_tier]`    | Checkbox (Poor/Fair/Good/Excellent) | Màu đỏ→xanh     |
| Giới tính               | `'gold dim_customer'[gender]`              | Toggle                              |                     |
| Số thẻ phát hành      | `'gold dim_card'[num_cards_issued]`        | Range slider                        |                     |
| Năm còn lại đến hưu | `'gold dim_customer'[years_to_retirement]` | Range slider                        |                     |
| Danh mục MCC             | `'gold dim_mcc'[mcc_description]`          | Search dropdown                     |                     |
| Trạng thái giao dịch   | `'gold fact_transaction'[has_error]`       | Toggle (Tất cả/Thành công/Lỗi) | `has_error` = 0/1 |

---

## 3. Thiết kế kỹ thuật Power BI

### 3.1 Kết nối dữ liệu

```
Power BI → Import / DirectQuery → SQL Server (database DATN, schema gold)
```

| Bảng                | Chế độ khuyến nghị  | Lý do                       |
| -------------------- | ------------------------ | ---------------------------- |
| `fact_transaction` | Import (157k dòng nhỏ) | Aggregation nhanh            |
| `dim_*`            | Import                   | Dimension nhỏ, cross-filter |

### 3.2 Quan hệ trong Power BI Data Model (khớp natural key thật)

```
'gold fact_transaction'[date_key]     → 'gold dim_date'[date_key]         (Many-to-One) ✓
'gold fact_transaction'[customer_id]  → 'gold dim_customer'[customer_id]  (Many-to-One) ✓
'gold fact_transaction'[card_id]      → 'gold dim_card'[card_id]          (Many-to-One) ✓
'gold fact_transaction'[merchant_id]  → 'gold dim_merchant'[merchant_id]  (Many-to-One) ✓
'gold fact_transaction'[mcc_id]       → 'gold dim_mcc'[mcc_id]            (Many-to-One) ✓
```

> - **Mark as Date Table**: chọn `dim_date` theo `full_date` để time-intelligence (DATEADD, SAMEPERIODLASTYEAR…) hoạt động. `dim_date` là model động nên tự trải đủ khoảng dữ liệu.
> - `'gold dim_card'[customer_id] → 'gold dim_customer'[customer_id]` tạo vòng lặp với fact → để **inactive**, kích hoạt bằng `USERELATIONSHIP()` khi cần phân tích thẻ theo KH không qua fact.

### 3.3 Các Measure DAX cốt lõi

```dax
-- ===== CHUNG =====
Total Transactions = COUNTROWS('gold fact_transaction')
Total Amount       = SUM('gold fact_transaction'[amount])
Active Customers   = DISTINCTCOUNT('gold fact_transaction'[customer_id])

-- is_chip_used là BIT (0/1) → so sánh = 1, KHÔNG dùng TRUE()
Chip Usage Rate % =
DIVIDE(
    CALCULATE(COUNTROWS('gold fact_transaction'), 'gold fact_transaction'[is_chip_used] = 1),
    [Total Transactions], 0
)

ARPU = DIVIDE([Total Amount], [Active Customers], 0)

-- ===== CD1 =====
-- C6/C7 tái dụng measure [Số KH theo Gợi ý] qua bảng phụ 'Gợi ý Sản phẩm' (DATATABLE)
-- CALCULATE ép filter context → SELECTEDVALUE bên trong trả đúng nhãn → số luôn khớp bar chart
KH Up-sell =
CALCULATE(
    [Số KH theo Gợi ý],
    'Gợi ý Sản phẩm'[Product_Suggestion] = "Up-sell: Thẻ Platinum"
) +
CALCULATE(
    [Số KH theo Gợi ý],
    'Gợi ý Sản phẩm'[Product_Suggestion] = "Up-sell: Tăng hạn mức"
)

KH Mời vay =
CALCULATE(
    [Số KH theo Gợi ý],
    'Gợi ý Sản phẩm'[Product_Suggestion] = "Cross-sell: Vay tiêu dùng lãi suất ưu đãi"
)

Weekend Txn % =
DIVIDE(
    CALCULATE(COUNTROWS('gold fact_transaction'), 'gold dim_date'[is_weekend] = TRUE()),
    [Total Transactions], 0
)
```

### 3.4 Row-Level Security (RLS)

| Role                 | Điều kiện lọc                                                 | Đối tượng                |
| -------------------- | ----------------------------------------------------------------- | ---------------------------- |
| `Marketing_Viewer` | `'gold dim_customer'[credit_risk_tier] IN {"Good","Excellent"}` | Marketing Team               |
| `Executive`        | Toàn bộ dữ liệu                                               | C-level, Product Manager, RM |

---

## 4. Hướng dẫn Triển khai

### 4.1 Thứ tự xây dựng

```
B1: Kết nối SQL Server (schema gold) & kiểm tra quan hệ natural key
B2: Mark dim_date as Date Table (theo full_date)
B3: Tạo Measure table riêng (các measure §3.3)
B4: Trang CD1-P1 (KPI cards + Treemap), đối chiếu số với SQL
B5: Trang CD1-P2 (Danh sách mục tiêu marketing + table export)
B6: Trang CD1-P3 (Hành vi chi tiêu theo segment)
B7: Thiết lập RLS
B8: Publish + cấu hình Scheduled Refresh
```

### 4.2 Refresh Schedule

| Loại                  | Tần suất                                                                     | Phương thức    |
| ---------------------- | ------------------------------------------------------------------------------ | ----------------- |
| Dữ liệu giao dịch   | Hàng ngày ~06:00 (sau khi `banking_structured_dag` chạy 02:00 + DQ 04:00) | Scheduled Refresh |
| Dữ liệu khách hàng | Hàng tuần                                                                    | Scheduled Refresh |

### 4.3 Checklist chất lượng trước go-live

- [ ] Tổng GD & SUM(amount) Power BI khớp query SQL trực tiếp trên `gold.fact_transaction`
- [ ] `dim_date` đã Mark as Date Table; time-intelligence (MoM) chạy đúng
- [ ] `KH Up-sell` / `KH Mời vay` (C6/C7) khớp số với bar chart Gợi ý sản phẩm
- [ ] CLV proxy đã ghi chú rõ giả định
- [ ] RLS đúng theo từng role
- [ ] Slicer cross-filter đúng giữa các visual
- [ ] Report render < 3 giây

---

## 5. Tóm tắt nhanh (CD1)

| Hạng mục              | Nội dung                                                  |
| ----------------------- | ----------------------------------------------------------- |
| **Người dùng chính** | Product Manager, Marketing Analyst, RM, C-level             |
| **KPI cốt lõi**        | ARPU, % Premium, Upsell/Loan Candidates, CLV proxy          |
| **Slicer quan trọng**   | income_segment, credit_risk_tier, years_to_retirement       |
| **Visual nổi bật**     | Treemap, Scatter income/debt, Map lat-long                  |
| **Số trang**            | 3                                                           |

---

## 6. Hạn chế dữ liệu & ghi chú

- Các dimension chỉ giữ **trạng thái mới nhất** (SCD2 active) nên các KPI cần xu hướng theo thời gian của thuộc tính KH (ví dụ thay đổi `income_segment`, `credit_risk_tier`) không tính trực tiếp từ Gold được; muốn có cần snapshot lịch sử từ `silver.sat_customer_profile`.
- **Đồng bộ `gold_layer_design.md`**: phần §3.6 (fact dùng surrogate key) và §3.5 (dim_date là seed) đã lỗi thời so với code — nên cập nhật để tránh nhầm lẫn.

---

## 7. Chuẩn trực quan & đánh giá trang hiện tại

> **Mục đích:** Thống nhất bảng màu, lưới bố cục và typography cho toàn báo cáo, kèm bản đánh giá Trang 1 CD1 theo ảnh thực tế. Áp các quy ước này để mọi trang nhìn nhất quán và bớt rối.

### 7.1 Bảng màu chuẩn (lưu thành Theme JSON, import một lần)

| Vai trò                | Mã màu     | Dùng cho                                        |
| ----------------------- | ------------ | ------------------------------------------------- |
| Nền trang             | `#F5F6F8` | Nền tổng thể (xám rất nhạt, tránh trắng tinh) |
| Nền card/visual       | `#FFFFFF` | Card KPI, khung biểu đồ                         |
| Màu chính             | `#1F4E79` | Cột/đường chủ đạo, tiêu đề                    |
| Màu nhấn              | `#2E9E8F` | Giá trị nổi bật, đường KPI phụ                |
| Văn bản chính        | `#222831` | Số liệu, tiêu đề                                 |
| Văn bản phụ          | `#6B7280` | Nhãn, chú thích                                   |
| Cảnh báo xấu (đỏ)   | `#D64545` | Vượt ngưỡng (Poor, DTI>0.5)                       |
| Đạt mục tiêu (xanh) | `#3FA34D` | Đạt ngưỡng tốt (credit score cao)               |
| Cảnh báo TB (vàng)  | `#E0A82E` | Mức trung bình                                    |

**Quy ước màu theo chiều (bắt buộc giữ nhất quán mọi visual):**

- `income_segment` (có thứ tự Low→Medium→High→Premium): dùng **một thang đơn sắc xanh** nhạt→đậm. Không trộn đỏ/cam/tím.
- `credit_risk_tier` (có thứ tự rủi ro): dùng **thang diverging cố định** Poor `#D64545` → Fair `#E0A82E` → Good `#7FB069` → Excellent `#1F8A4C`. Dùng đúng thang này ở slicer, treemap, scatter, matrix.
- Đỏ/cam/vàng **chỉ** mang ý nghĩa cảnh báo — không dùng trang trí.
- Không dùng cùng một dải màu cho hai chiều khác nhau trong cùng trang (tránh người xem nhầm `income_segment` với `credit_risk_tier`).

### 7.2 Lưới bố cục

- Khổ 16:9 (1280×720). Bật **Snap to grid** + **Gridlines**.
- Lề ngoài 16px, khoảng cách giữa visual (gutter) 12px, đều bốn phía.
- Mọi visual phải bắt đầu/kết thúc trên cùng đường lưới; không lệch vài pixel.
- Thứ tự đọc theo chữ Z: KPI quan trọng nhất ở trên-trái, chi tiết/bảng ở dưới-phải.

### 7.3 Typography

- Một font duy nhất toàn báo cáo (Segoe UI hoặc Inter).
- Số KPI 28–32px đậm · nhãn KPI 11–12px xám · tiêu đề visual 14px nửa đậm · nhãn trục 10–11px.
- Tiêu đề visual viết theo **insight tiếng Việt**, không để nguyên tên cột ("Chi tiêu theo ngành hàng và phân khúc" thay cho "...by mcc_description and income_segment").

### 7.4 Đánh giá Trang 1 CD1 (theo ảnh 2026-06-24) & cách sửa

| # | Vấn đề quan sát                                                                                          | Cách sửa                                                                                           |
| - | ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 1 | Cột slicer trái rời rạc, phí khoảng trắng (month/quarter/year mỗi cái chừa vùng trắng lớn), thứ tự lộn xộn | Gom slicer thành **một dải ngang** dưới header theo thứ tự `Năm → Quý → Tháng → income_segment → credit_risk_tier`; trả chiều rộng còn lại cho vùng biểu đồ |
| 2 | Visual không thẳng lưới, gutter không đều                                                              | Căn lại theo §7.2 (snap to grid, gutter 12px)                                                     |
| 3 | Mảng cam treemap góc dưới-trái quá nặng → lệch trọng lượng thị giác                                | Đổi treemap sang **một gradient xanh** (màu = tổng chi tiêu); bỏ cam                              |
| 4 | Trộn xanh/đỏ/cam/tím, màu không nghĩa, lặp giữa hai chiều                                         | Áp §7.1: `income_segment` thang xanh, `credit_risk_tier` thang đỏ→xanh cố định                |
| 5 | Stacked bar ngang đầy nhãn `0,0xM` chồng chất, lát nhỏ vô nghĩa                                    | Rút **Top 10 MCC**, tắt nhãn lát nhỏ (chỉ tổng dòng), sort giảm dần                              |
| 6 | Scatter income–debt overplot, không thấy quy luật                                                       | Tăng độ trong suốt điểm, giới hạn trục theo phân vị (P1–P99), màu theo thang rủi ro             |
| 7 | Treemap/nhãn bị cắt ("Pr..", "Gr..")                                                                      | Tăng kích thước ô hoặc rút số hạng mục; bật tooltip thay vì nhãn dài                            |
| 8 | Tiêu đề visual để nguyên tên cột tiếng Anh                                                            | Viết lại theo insight tiếng Việt (§7.3)                                                            |

### 7.5 Bố cục Trang 1 đề xuất (sau hiệu chỉnh)

```
┌───────────────────────────────────────────────────────────────────┐
│  TỔNG QUAN PHÂN KHÚC                          (header mảnh, trái) │
├───────────────────────────────────────────────────────────────────┤
│ [Năm][Quý][Tháng][income_segment][credit_risk_tier]  ← dải slicer │
├──────────┬──────────┬──────────┬──────────┬───────────────────────┤
│ C1       │ C2       │ C3       │ C4       │ C5                      │
│ KH Active│ Tổng CT  │ ARPU     │ %Premium │ Thẻ/KH                 │
├──────────┴──────────┴──────────┴─────┬────┴───────────────────────┤
│ [Treemap income_segment×risk         │ [Bar: Top 10 MCC theo chi  │
│  size=số KH, màu=gradient xanh]      │  tiêu, sort desc, 1 màu]   │
├──────────────────────────────────────┼────────────────────────────┤
│ [Donut: KH theo gender, 2 màu       │ [Scatter: thu nhập × dư nợ │
│  trung tính]                         │  màu=thang rủi ro Poor→Exc]│
└──────────────────────────────────────┴────────────────────────────┘
```

# Tài Liệu Thiết Kế BI – Power BI Dashboard

## Dự án: Phân tích Giao dịch Thẻ Tín dụng Ngân hàng

**Phiên bản:** 2.0 (hiệu chỉnh khớp datamart thực tế)
**Ngày:** 2026-06-04
**Nguồn dữ liệu:** Gold Layer (Star Schema – dbt, database `DATN`, schema `gold`)
**Công cụ:** Microsoft Power BI Desktop / Power BI Service

> **Changelog v2.0:** Đối chiếu lại với 6 model gold thực tế (`dim_customer`, `dim_card`, `dim_merchant`, `dim_mcc`, `dim_date`, `fact_transaction`). Sửa: khóa quan hệ là **natural key** (không phải surrogate), `dim_date` là **model động** (không phải seed), bỏ/đánh dấu các KPI EWS không tính được từ Gold, thêm cảnh báo lệch thời gian (`GETDATE()`=2026 vs dữ liệu kết thúc 2024-10), gắn cờ khả thi cho từng KPI.

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
> - Các dimension chỉ giữ **trạng thái mới nhất** (SCD2 active). Không có lịch sử credit_score/DTI ở Gold → các KPI cần xu hướng theo thời gian của thuộc tính KH không tính được trực tiếp (xem §7).

### Cột có sẵn để làm KPI (trích từ model thật)

- **dim_customer**: `customer_id, gender, address, yearly_income, credit_score, current_age, retirement_age, birth_year, birth_month, latitude, longitude, per_capita_income, total_debt, num_credit_cards` + computed `debt_to_income_ratio, income_segment, credit_risk_tier, years_to_retirement`.
- **dim_card**: `card_id, customer_id, card_brand, card_type, credit_limit, expires, has_chip, num_cards_issued, acct_open_date, year_pin_last_changed` + computed `card_age_years, pin_age_years`.
- **dim_merchant**: `merchant_id, merchant_city, merchant_state, zip`.
- **dim_mcc**: `mcc_id, mcc_description`.
- **dim_date**: `date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend`.
- **fact_transaction**: `transaction_id, date_key, customer_id, card_id, merchant_id, mcc_id, transaction_datetime, amount, use_chip, merchant_city, merchant_state, zip, errors` + computed `has_error (BIT), is_chip_used (BIT)`.

> ⚠️ **Cảnh báo lệch thời gian:** `card_age_years`, `pin_age_years` được tính bằng `GETDATE()` (hiện ~2026) trong khi dữ liệu giao dịch kết thúc **2024-10-31**. Mọi KPI "tuổi thẻ / tuổi PIN / sắp hết hạn" lệch ~1.5–2 năm. Khuyến nghị dùng **mốc as-of cố định** = ngày max dữ liệu (2024-10-31) thay cho `TODAY()`/`GETDATE()` (xem §7).

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

```dax

```

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

## 3. Chủ đề CD2 – Phân tích Giao dịch & Quản trị Rủi ro

### 3.1 Mục tiêu nghiệp vụ

> **Tối ưu trải nghiệm người dùng** (giảm lỗi PIN, tăng tỷ lệ chip transaction) và **xây dựng hàng rào ngăn chặn rủi ro vỡ nợ** bằng các tín hiệu cảnh báo sớm (Early Warning Signals – EWS).

### 3.2 Đối tượng sử dụng dashboard

| Vai trò                                | Nhu cầu chính                                       | Tần suất xem | Trang quan tâm         |
| --------------------------------------- | ----------------------------------------------------- | -------------- | ----------------------- |
| **Operations Manager**            | Theo dõi lỗi GD, hiệu suất chip vs swipe          | Hàng ngày    | Trang 1                 |
| **Risk Officer / Credit Analyst** | Giám sát danh mục, phát hiện KH rủi ro vỡ nợ  | Hàng ngày    | Trang 3                 |
| **Fraud Analyst**                 | Phát hiện GD bất thường theo địa lý/giá trị | Ngày          | Trang 2                 |
| **Customer Experience Team**      | Phân tích nguyên nhân lỗi để giảm lỗi PIN    | Hàng tuần    | Trang 1 + 2             |
| **Chief Risk Officer (CRO)**      | Bức tranh rủi ro danh mục tổng                    | Hàng tuần    | Header + Funnel trang 3 |

### 3.3 KPI Đề xuất (đã hiệu chỉnh)

Cờ: ✅ dùng ngay · ⚠️ lưu ý thời gian · ❌ cần bổ sung dữ liệu mới tính được.

#### Nhóm Chất lượng Giao dịch (Transaction Health)

| #  | Tên KPI                             | Công thức DAX                                                                                          | Mục tiêu              | Cờ |
| -- | ------------------------------------ | -------------------------------------------------------------------------------------------------------- | ----------------------- | --- |
| R1 | **Tổng số GD**               | `COUNTROWS('gold fact_transaction')`                                                                   | benchmark               | ✅  |
| R2 | **Tỷ lệ GD lỗi**            | `DIVIDE(CALCULATE(COUNTROWS('gold fact_transaction'), 'gold fact_transaction'[has_error]=1), [R1])`    | < 2% (SLA)              | ✅  |
| R3 | **Tỷ lệ dùng Chip**         | `DIVIDE(CALCULATE(COUNTROWS('gold fact_transaction'), 'gold fact_transaction'[is_chip_used]=1), [R1])` | > 80%                   | ✅  |
| R4 | **Tổng giá trị GD lỗi**    | `CALCULATE(SUM('gold fact_transaction'[amount]), 'gold fact_transaction'[has_error]=1)`                | thiệt hại tiềm năng | ✅  |
| R5 | **Số loại lỗi phân biệt** | `DISTINCTCOUNT('gold fact_transaction'[errors])`                                                       | đa dạng nguyên nhân | ✅  |
| R6 | **Mean Transaction Value**     | `AVERAGE('gold fact_transaction'[amount])`                                                             | phát hiện outlier     | ✅  |

#### Nhóm Rủi ro Danh mục (Portfolio Risk)

| #   | Tên KPI                                   | Công thức DAX                                                                                                                                          | Mục tiêu                 | Cờ                            |
| --- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- | ------------------------------ |
| R7  | **% KH rủi ro cao (Poor)**          | `DIVIDE(CALCULATE(COUNTROWS('gold dim_customer'), 'gold dim_customer'[credit_risk_tier]="Poor"), COUNTROWS('gold dim_customer'))`                      | < 10%                      | ✅                             |
| R8  | **Dư nợ nhóm Poor**               | `CALCULATE(SUM('gold dim_customer'[total_debt]), 'gold dim_customer'[credit_risk_tier]="Poor")`                                                        | exposure tối đa          | ✅                             |
| R9  | **% KH DTI > 0.5**                   | `DIVIDE(CALCULATE(COUNTROWS('gold dim_customer'), 'gold dim_customer'[debt_to_income_ratio]>0.5), COUNTROWS('gold dim_customer'))`                     | < 5%                       | ✅                             |
| R10 | **Credit score bq danh mục**        | `AVERAGE('gold dim_customer'[credit_score])`                                                                                                           | > 680                      | ✅                             |
| R11 | **Tương quan PIN cũ ↔ lỗi**     | GD lỗi (`has_error`=1) phân tích theo `'gold dim_card'[pin_age_years]`                                                                            | cơ sở giảm lỗi PIN     | ⚠️                           |
| R12 | **Số KH PIN cũ (>3 năm)**         | `CALCULATE(COUNTROWS('gold dim_card'), 'gold dim_card'[pin_age_years]>3)`                                                                              | danh sách nhắc đổi PIN | ⚠️ tính theo GETDATE()=2026 |
| R13 | **Thẻ sắp hết hạn (≤60 ngày)** | `CALCULATE(COUNTROWS('gold dim_card'), DATEDIFF(TODAY(), 'gold dim_card'[expires], DAY) >= 0, DATEDIFF(TODAY(), 'gold dim_card'[expires], DAY) <= 60)` | trigger renewal            | ⚠️ lệch do TODAY()=2026     |

#### Nhóm Early Warning Signals (EWS)

| #   | KPI                                             | Logic phát hiện                                                                                                                            | Hành động                     | Cờ                                                    |
| --- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- | ------------------------------------------------------ |
| R14 | **KH chi tiêu đột biến (>200% bq)**   | Chi tiêu tháng hiện tại > 2× bq 6 tháng trước (DAX time-intelligence trên `fact` + `dim_date`)                                  | Xác minh danh tính             | ✅                                                     |
| R15 | **KH lỗi liên tiếp ≥3 lần/tuần**    | Window theo `card_id`/`customer_id` + `dim_date` (DAX nặng → cân nhắc precompute ở dbt)                                           | Gợi ý đổi thẻ / hỗ trợ UX | ✅                                                     |
| R16 | **GD ngoài vùng địa lý quen thuộc** | So `merchant_state` với state của KH — **nhưng `dim_customer` không có cột state sạch** (chỉ `address` text + lat/long) | Flag fraud                       | ❌ không đáng tin tới khi thêm `customer_state` |
| R17 | **KH credit_score giảm + DTI tăng**     | Cần**lịch sử** credit_score/DTI — Gold chỉ có trạng thái mới nhất                                                            | Hạ hạn mức chủ động        | ❌ cần snapshot lịch sử từ Silver                  |

### 3.4 Thiết kế trang Dashboard CD2

#### Trang 1: "Tổng quan Sức khỏe Giao dịch" (Operations View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Ngày/Tuần/Tháng] [SLICER: card_brand] [SLICER: has_error] │
├──────────┬──────────┬──────────┬──────────┬───────────────────┤
│  R1      │  R2      │  R3      │  R4      │  R5               │
│ Tổng GD  │ % Lỗi   │ % Chip   │ GT Lỗi  │ Loại lỗi         │
│          │🔴 nếu>2% │🟢 nếu>80%│          │                   │
├──────────┴──────────┴──────────┴──────────┴───────────────────┤
│  [Line 2 trục: Tổng GD (trái) & Tỷ lệ lỗi % (phải) theo ngày] │
│   → phát hiện ngày đột biến lỗi                               │
├──────────────────────────┬─────────────────────────────────────┤
│  [Bar: Top 10 loại lỗi   │  [Donut: use_chip (Chip/Swipe/     │
│   (errors) theo số GD]   │   Online) break by has_error]      │
│                          │  [Card: MTV (R6) + format outlier]  │
└──────────────────────────┴─────────────────────────────────────┘
```

#### Trang 2: "Bản đồ Rủi ro Giao dịch" (Fraud & Geo Analysis)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Tháng] [SLICER: has_error=1] [SLICER: amount range]  │
├────────────────────────────────────────────────────────────────┤
│  [Map USA: điểm = merchant_city/state (degenerate trong fact)] │
│   Size = số GD lỗi | Màu = tỷ lệ lỗi (red gradient)          │
├──────────────────────────┬─────────────────────────────────────┤
│  [Scatter: amount vs     │  [Heatmap: merchant_state × MCC    │
│   pin_age_years,         │   vs tổng GD lỗi]                  │
│   màu = has_error        │  [Table: Top 20 GD đáng ngờ        │
│   → GD lớn + PIN cũ]    │   amount > P95 AND has_error=1]    │
└──────────────────────────┴─────────────────────────────────────┘
```

> **Lưu ý geo:** dùng `merchant_city/state/zip` **trong `fact_transaction`** (degenerate dimension) để có vị trí đúng tại thời điểm giao dịch. `dim_merchant` chỉ giữ location mới nhất, không phù hợp phân tích lỗi theo địa điểm lịch sử.

#### Trang 3: "Giám sát Rủi ro Danh mục" (Risk Portfolio View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: credit_risk_tier] [SLICER: income_segment]           │
│  [SLICER: DTI threshold slider]                                │
├──────────┬──────────┬──────────┬──────────┬───────────────────┤
│  R7      │  R8      │  R9      │  R10     │  R12              │
│ % Poor   │ Nợ Poor │ % DTI>0.5│ CS bq    │ PIN cũ (>3 năm)   │
├──────────┴──────────┴──────────┴──────────┴───────────────────┤
│  [Funnel/Waterfall: tầng credit_risk_tier Excellent→Poor      │
│   giá trị = tổng total_debt mỗi tầng]                         │
├──────────────────────────┬─────────────────────────────────────┤
│  [Matrix: income_segment │  [Gauge: DTI danh mục avg          │
│   × credit_risk_tier,    │   mục tiêu < 0.35]                 │
│   giá trị = số KH,       │  [Card: Thẻ sắp hết hạn R13        │
│   màu nền = avg(DTI)]    │   ⚠ caveat thời gian]             │
└──────────────────────────┴─────────────────────────────────────┘
```

#### Trang 4: "Cảnh báo Sớm & Hành động" (EWS Action Center)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Mức độ cảnh báo] [SLICER: Ngày]                     │
├────────────────────────────────────────────────────────────────┤
│  [Cards] R14: chi tiêu đột biến | R15: lỗi liên tiếp          │
│  (R16 geo & R17 suy giảm tín nhiệm: ẩn tới khi bổ sung dữ liệu)│
├────────────────────────────────────────────────────────────────┤
│  [Table EWS: customer_id | loại cảnh báo | mức độ | ngày      │
│   | giá trị GD | credit_risk_tier]  → drill-through hồ sơ KH   │
├────────────────────────────────────────────────────────────────┤
│  🔴 Cao: DTI>0.5 AND credit_risk_tier=Poor                    │
│  🟡 TB: lỗi liên tiếp OR pin_age_years>3                       │
│  🟢 Thấp: chi tiêu đột biến nhưng credit_score tốt            │
└────────────────────────────────────────────────────────────────┘
```

### 3.5 Bộ lọc & Slicer CD2

| Slicer               | Nguồn trường                               | Loại control         | Ghi chú            |
| -------------------- | --------------------------------------------- | --------------------- | ------------------- |
| Khoảng thời gian   | `'gold dim_date'[full_date]`                | Date Range Picker     |                     |
| Granularity          | `'gold dim_date'[year/quarter/month/day]`   | Toggle group          | Drill-down          |
| Có lỗi hay không  | `'gold fact_transaction'[has_error]`        | Toggle                | `has_error` = 0/1 |
| Loại lỗi           | `'gold fact_transaction'[errors]`           | Multi-select + search |                     |
| Loại thẻ           | `'gold dim_card'[card_type]`                | Checkbox              |                     |
| Thương hiệu thẻ  | `'gold dim_card'[card_brand]`               | Checkbox              |                     |
| Có chip không      | `'gold dim_card'[has_chip]`                 | Toggle                |                     |
| Khoảng tiền GD     | `'gold fact_transaction'[amount]`           | Range slider          |                     |
| Trạng thái rủi ro | `'gold dim_customer'[credit_risk_tier]`     | Color-coded checkbox  |                     |
| DTI threshold        | `'gold dim_customer'[debt_to_income_ratio]` | Numeric slicer (≥ X) |                     |
| Bang merchant        | `merchant_state` (fact hoặc dim_merchant)  | Dropdown              |                     |
| Danh mục MCC        | `'gold dim_mcc'[mcc_description]`           | Search dropdown       |                     |
| Cuối tuần          | `'gold dim_date'[is_weekend]`               | Toggle                |                     |

---

## 4. Thiết kế kỹ thuật Power BI

### 4.1 Kết nối dữ liệu

```
Power BI → Import / DirectQuery → SQL Server (database DATN, schema gold)
```

| Bảng                | Chế độ khuyến nghị  | Lý do                       |
| -------------------- | ------------------------ | ---------------------------- |
| `fact_transaction` | Import (157k dòng nhỏ) | Aggregation nhanh            |
| `dim_*`            | Import                   | Dimension nhỏ, cross-filter |

### 4.2 Quan hệ trong Power BI Data Model (khớp natural key thật)

```
'gold fact_transaction'[date_key]     → 'gold dim_date'[date_key]         (Many-to-One) ✓
'gold fact_transaction'[customer_id]  → 'gold dim_customer'[customer_id]  (Many-to-One) ✓
'gold fact_transaction'[card_id]      → 'gold dim_card'[card_id]          (Many-to-One) ✓
'gold fact_transaction'[merchant_id]  → 'gold dim_merchant'[merchant_id]  (Many-to-One) ✓
'gold fact_transaction'[mcc_id]       → 'gold dim_mcc'[mcc_id]            (Many-to-One) ✓
```

> - **Mark as Date Table**: chọn `dim_date` theo `full_date` để time-intelligence (DATEADD, SAMEPERIODLASTYEAR…) hoạt động. `dim_date` là model động nên tự trải đủ khoảng dữ liệu.
> - `'gold dim_card'[customer_id] → 'gold dim_customer'[customer_id]` tạo vòng lặp với fact → để **inactive**, kích hoạt bằng `USERELATIONSHIP()` khi cần phân tích thẻ theo KH không qua fact.

### 4.3 Các Measure DAX cốt lõi (đã sửa)

```dax
-- ===== CHUNG =====
Total Transactions = COUNTROWS('gold fact_transaction')
Total Amount       = SUM('gold fact_transaction'[amount])
Active Customers   = DISTINCTCOUNT('gold fact_transaction'[customer_id])

-- has_error / is_chip_used là BIT (0/1) → so sánh = 1, KHÔNG dùng TRUE()
Error Rate % =
DIVIDE(
    CALCULATE(COUNTROWS('gold fact_transaction'), 'gold fact_transaction'[has_error] = 1),
    [Total Transactions], 0
)

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

-- ===== CD2 =====
Error Amount = CALCULATE([Total Amount], 'gold fact_transaction'[has_error] = 1)

High DTI Customer % =
DIVIDE(
    CALCULATE(COUNTROWS('gold dim_customer'), 'gold dim_customer'[debt_to_income_ratio] > 0.5),
    COUNTROWS('gold dim_customer'), 0
)

Poor Tier Debt =
CALCULATE(SUM('gold dim_customer'[total_debt]), 'gold dim_customer'[credit_risk_tier] = "Poor")

MoM Error Rate Change =
VAR CurrentMonth = [Error Rate %]
VAR PrevMonth = CALCULATE([Error Rate %], DATEADD('gold dim_date'[full_date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0)

-- ⚠ Caveat thời gian: TODAY() ~ 2026 nhưng dữ liệu kết thúc 2024-10.
-- Cân nhắc thay TODAY() bằng biến mốc as-of cố định = 2024-10-31.
Old PIN Cards = CALCULATE(COUNTROWS('gold dim_card'), 'gold dim_card'[pin_age_years] > 3)

Cards Expiring Soon =
CALCULATE(
    COUNTROWS('gold dim_card'),
    DATEDIFF(TODAY(), 'gold dim_card'[expires], DAY) >= 0,
    DATEDIFF(TODAY(), 'gold dim_card'[expires], DAY) <= 60
)
```

### 4.4 Row-Level Security (RLS)

| Role                   | Điều kiện lọc                                                                 | Đối tượng  |
| ---------------------- | --------------------------------------------------------------------------------- | -------------- |
| `Credit_Risk_Viewer` | `'gold dim_customer'[credit_risk_tier] IN {"Poor","Fair"}`                      | Risk Analyst   |
| `Marketing_Viewer`   | `'gold dim_customer'[credit_risk_tier] IN {"Good","Excellent"}`                 | Marketing Team |
| `Executive`          | Toàn bộ dữ liệu                                                               | C-level, CRO   |
| `Operations`         | Ẩn `'gold dim_customer'[credit_score]`, `total_debt` (object-level security) | Ops Team       |

---

## 5. Hướng dẫn Triển khai

### 5.1 Thứ tự xây dựng

```
B1: Kết nối SQL Server (schema gold) & kiểm tra quan hệ natural key
B2: Mark dim_date as Date Table (theo full_date)
B3: Tạo Measure table riêng (các measure §4.3)
B4: Trang CD2-P1 (đơn giản nhất, đối chiếu số với SQL)
B5: Trang CD1-P1 (Treemap + KPI cards)
B6: Trang drill-through (EWS → hồ sơ KH)
B7: Thiết lập RLS
B8: Publish + cấu hình Scheduled Refresh
```

### 5.2 Refresh Schedule

| Loại                  | Tần suất                                                                     | Phương thức    |
| ---------------------- | ------------------------------------------------------------------------------ | ----------------- |
| Dữ liệu giao dịch   | Hàng ngày ~06:00 (sau khi `banking_structured_dag` chạy 02:00 + DQ 04:00) | Scheduled Refresh |
| Dữ liệu khách hàng | Hàng tuần                                                                    | Scheduled Refresh |

### 5.3 Checklist chất lượng trước go-live

- [ ] Tổng GD & SUM(amount) Power BI khớp query SQL trực tiếp trên `gold.fact_transaction`
- [ ] `Error Rate %` khớp SQL kiểm tra thủ công (đếm `has_error=1`)
- [ ] `dim_date` đã Mark as Date Table; time-intelligence (MoM) chạy đúng
- [ ] RLS đúng theo từng role
- [ ] Slicer cross-filter đúng giữa các visual
- [ ] Drill-through EWS → hồ sơ KH hoạt động
- [ ] Đã ghi chú caveat thời gian cho R12/R13 (hoặc đã chuyển sang mốc as-of)
- [ ] Report render < 3 giây

---

## 6. Tóm tắt nhanh

|                                | CD1 – Phân khúc KH                                 | CD2 – Quản trị Rủi ro                                         |
| ------------------------------ | ----------------------------------------------------- | ----------------------------------------------------------------- |
| **Người dùng chính** | Product Manager, Marketing Analyst, RM, C-level       | Ops Manager, Risk Officer, Fraud Analyst, CX, CRO                 |
| **KPI cốt lõi**        | ARPU, % Premium, Upsell/Loan Candidates, CLV proxy    | Error Rate %, Chip %, % DTI>0.5, % Poor, Credit Score bq          |
| **Slicer quan trọng**   | income_segment, credit_risk_tier, years_to_retirement | has_error, amount range, credit_risk_tier, date range             |
| **Visual nổi bật**     | Treemap, Scatter income/debt, Map lat-long            | Line 2 trục lỗi, Map merchant_state, Funnel DTI, Matrix rủi ro |
| **Số trang**            | 3                                                     | 4                                                                 |

---

## 7. Hạn chế dữ liệu & khuyến nghị bổ sung datamart

Để bật đầy đủ nhóm EWS và bỏ caveat thời gian, cân nhắc bổ sung ở tầng Gold:

1. **`dim_customer.customer_state`** — parse từ `address` hoặc reverse-geocode `latitude/longitude` → bật lại **R16** (geo-mismatch fraud).
2. **`gold.dim_customer_history`** (snapshot từ `silver.sat_customer_profile` SCD2) → bật lại **R17** (suy giảm credit_score/DTI theo thời gian).
3. **Mốc as-of cố định** (vd biến `analysis_date` = 2024-10-31, ngày max dữ liệu) thay cho `GETDATE()`/`TODAY()` trong `pin_age_years`, `card_age_years`, "thẻ sắp hết hạn" → **R11, R12, R13** hết lệch.
4. **Precompute `is_consecutive_error`** ở dbt fact (window theo card) → **R15** nhẹ hơn trên Power BI.
5. **Đồng bộ `gold_layer_design.md`**: phần §3.6 (fact dùng surrogate key) và §3.5 (dim_date là seed) đã lỗi thời so với code — nên cập nhật để tránh nhầm lẫn.

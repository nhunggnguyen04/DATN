# Tài Liệu Thiết Kế BI – Power BI Dashboard
## Dự án: Phân tích Giao dịch Thẻ Tín dụng Ngân hàng

**Phiên bản:** 1.0  
**Ngày:** 2026-05-30  
**Nguồn dữ liệu:** Gold Layer (Star Schema – dbt)  
**Công cụ:** Microsoft Power BI Desktop / Power BI Service  

---

## 1. Tổng quan mô hình dữ liệu Gold Layer

```
dim_date ─────┐
dim_customer ──┤
dim_card ──────┼──► fact_transaction (grain: 1 row = 1 giao dịch)
dim_merchant ──┤
dim_mcc ───────┘
```

| Bảng | Loại | Mô tả |
|---|---|---|
| `fact_transaction` | Fact | Bảng sự kiện chính, ~1 row/giao dịch |
| `dim_customer` | Dimension | Hồ sơ khách hàng, phân khúc thu nhập & rủi ro |
| `dim_card` | Dimension | Thông tin thẻ, chip, hạn mức |
| `dim_date` | Dimension | Bộ lịch chuẩn (year→day, is_weekend) |
| `dim_merchant` | Dimension | Địa điểm merchant |
| `dim_mcc` | Dimension | Danh mục nghề/loại giao dịch (MCC) |

---

## 2. Chủ đề CD1 – Phân Khúc Khách Hàng & Marketing Cá Nhân Hóa

### 2.1 Mục tiêu nghiệp vụ
> Tìm ra nhóm khách hàng tiềm năng để **up-sell thẻ tín dụng** hoặc **mời chào gói vay phù hợp** dựa trên hành vi chi tiêu, hồ sơ tài chính và phân khúc rủi ro.

### 2.2 Đối tượng sử dụng dashboard

| Vai trò | Nhu cầu chính | Tần suất xem |
|---|---|---|
| **Product Manager (Thẻ & Vay)** | Xác định segment khách hàng để thiết kế sản phẩm mới | Hàng tuần |
| **Marketing Analyst** | Lọc danh sách target cho chiến dịch email/SMS | Hàng ngày |
| **Relationship Manager** | Xem profile khách hàng VIP để chăm sóc cá nhân hóa | Theo yêu cầu |
| **Ban lãnh đạo (C-level)** | Theo dõi tăng trưởng giá trị khách hàng (CLV) theo quý | Hàng tháng |

---

### 2.3 KPI Đề xuất

#### Nhóm KPI Tổng quan (Header Cards)

| # | Tên KPI | Công thức DAX | Ý nghĩa |
|---|---|---|---|
| K1 | **Tổng số khách hàng active** | `DISTINCTCOUNT(fact_transaction[customer_id])` | Khách hàng có ít nhất 1 giao dịch trong kỳ |
| K2 | **Tổng chi tiêu toàn danh mục** | `SUM(fact_transaction[amount])` | Doanh số tổng |
| K3 | **Chi tiêu bình quân/khách hàng** | `[Tổng chi tiêu] / [Tổng KH active]` | Average Revenue Per User (ARPU) |
| K4 | **Tỷ lệ KH Premium** | `DIVIDE(COUNTROWS(FILTER(dim_customer, [income_segment]="Premium")), COUNTROWS(dim_customer))` | Đo lường "chất lượng" tệp khách hàng |
| K5 | **Số thẻ bình quân/KH** | `DIVIDE(DISTINCTCOUNT(fact_transaction[card_id]), DISTINCTCOUNT(fact_transaction[customer_id]))` | Tiềm năng up-sell thẻ thêm |

#### Nhóm KPI Phân khúc & Tiềm năng up-sell

| # | Tên KPI | Công thức DAX | Ý nghĩa |
|---|---|---|---|
| K6 | **KH đủ điều kiện up-sell thẻ** | KH có `credit_risk_tier IN ('Good','Excellent')` AND `num_credit_cards < 3` AND `yearly_income > median` | Danh sách mục tiêu marketing |
| K7 | **KH có rủi ro tăng hạn mức** | `debt_to_income_ratio > 0.4` AND `credit_score < 650` | Cần cảnh báo rủi ro |
| K8 | **Tỷ lệ KH sắp về hưu (<5 năm)** | `DIVIDE(COUNTROWS(FILTER(dim_customer,[years_to_retirement]<=5)), [Total Customers])` | Segment cần sản phẩm tích lũy/bảo hiểm |
| K9 | **Tổng dư nợ danh mục** | `SUM(dim_customer[total_debt])` | Exposure toàn danh mục |
| K10 | **CLV ước tính (12 tháng)** | `[ARPU] * 12` | Customer Lifetime Value (proxy đơn giản) |

#### Nhóm KPI Hành vi chi tiêu

| # | Tên KPI | Công thức DAX | Ý nghĩa |
|---|---|---|---|
| K11 | **Top 3 danh mục chi tiêu (MCC)** | `TOPN(3, dim_mcc, [Tổng chi tiêu], DESC)` | Insight về sở thích chi tiêu theo phân khúc |
| K12 | **Tỷ lệ giao dịch cuối tuần** | `DIVIDE(COUNTROWS(FILTER(fact_transaction,[is_weekend]=TRUE)), [Total Transactions])` | Hành vi chi tiêu leisure vs. business |
| K13 | **Tỷ lệ dùng chip** | `DIVIDE(SUM(fact_transaction[is_chip_used]), [Total Transactions])` | Proxy cho nhóm KH cẩn thận/am hiểu công nghệ |
| K14 | **Số giao dịch bình quân/KH/tháng** | `[Total Transactions] / [Active Months] / [Total Customers]` | Frequency – chỉ số gắn kết |

---

### 2.4 Thiết kế trang Dashboard CD1

#### Trang 1: "Tổng quan Phân khúc" (Executive View)

```
┌─────────────────────────────────────────────────────────────────┐
│  [SLICER: Năm] [SLICER: Quý] [SLICER: income_segment] [SLICER: credit_risk_tier]  │
├──────────┬──────────┬──────────┬──────────┬────────────────────┤
│  K1      │  K2      │  K3      │  K4      │  K5               │
│ KH Active│ Tổng CT  │ ARPU     │ % Premium│ Thẻ/KH            │
├──────────┴──────────┴──────────┴──────────┴────────────────────┤
│                                                                  │
│  [Biểu đồ Treemap: Phân bổ KH theo income_segment x credit_risk_tier]  │
│   Màu sắc = Tổng chi tiêu, Kích thước = Số lượng KH            │
│                                                                  │
├─────────────────────────┬────────────────────────────────────────┤
│  [Bar Chart: Top 10 MCC │  [Donut Chart: Phân bổ gender x       │
│   theo Tổng chi tiêu    │   income_segment]                      │
│   – phân tách theo      │                                        │
│   income_segment]       │  [Scatter: yearly_income vs           │
│                         │   total_debt – bubble = num_credit_cards│
│                         │   màu = credit_risk_tier]              │
└─────────────────────────┴────────────────────────────────────────┘
```

**Insight mục tiêu:** PM và Marketing xác định được "tứ phân vị" khách hàng ngay từ trang đầu.

---

#### Trang 2: "Danh sách Mục tiêu Marketing" (Analyst View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: income_segment] [SLICER: credit_risk_tier]           │
│  [SLICER: years_to_retirement range] [SLICER: num_credit_cards]│
├────────────────────────────────────────────────────────────────┤
│  Số KH đủ điều kiện: [K6]    Tiềm năng doanh thu: [CLV tổng]  │
├─────────────────────────────────────────────────────────────────┤
│  [Bảng chi tiết: customer_id | income_segment | credit_risk_tier│
│   | yearly_income | num_credit_cards | CLV_12m | Gợi ý sản phẩm]│
│   → Hỗ trợ Export to Excel để campaign team lấy danh sách      │
├─────────────────────────────────────────────────────────────────┤
│  [Map: Phân bổ địa lý KH theo latitude/longitude]              │
│   Màu = income_segment, Kích thước = Tổng chi tiêu 12 tháng    │
└─────────────────────────────────────────────────────────────────┘
```

**Logic "Gợi ý sản phẩm" (DAX Calculated Column):**
```dax
Product_Suggestion = 
SWITCH(TRUE(),
    dim_customer[credit_risk_tier] = "Excellent" 
        && dim_customer[num_credit_cards] < 2, "Up-sell: Thẻ Platinum",
    dim_customer[income_segment] = "Premium" 
        && dim_customer[years_to_retirement] <= 5, "Cross-sell: Gói Tích lũy Hưu trí",
    dim_customer[debt_to_income_ratio] < 0.3 
        && dim_customer[credit_risk_tier] IN {"Good","Excellent"}, "Up-sell: Tăng hạn mức",
    dim_customer[income_segment] IN {"Low","Medium"} 
        && dim_customer[credit_score] > 650, "Cross-sell: Vay tiêu dùng lãi suất ưu đãi",
    "Theo dõi – chưa đủ điều kiện"
)
```

---

#### Trang 3: "Hành vi Chi tiêu theo Segment" (Deep-dive)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: income_segment] [SLICER: gender] [SLICER: Tháng]    │
├──────────────────────────┬─────────────────────────────────────┤
│  [Line Chart: Xu hướng   │  [Heatmap: Ngày trong tuần x       │
│   chi tiêu theo tháng    │   Giờ trong ngày vs Tổng GD        │
│   – break by segment]    │   – lọc theo MCC]                   │
├──────────────────────────┴─────────────────────────────────────┤
│  [Stacked Bar: Top 5 MCC theo từng income_segment]             │
│   → Highlight sự khác biệt hành vi giữa các nhóm               │
├────────────────────────────────────────────────────────────────┤
│  [KPI cards] Tỷ lệ GD cuối tuần [K12] | Tỷ lệ chip [K13]     │
│              Số GD/KH/tháng [K14]                               │
└────────────────────────────────────────────────────────────────┘
```

---

### 2.5 Bộ lọc & Slicer CD1

| Slicer | Nguồn trường | Loại control | Ghi chú |
|---|---|---|---|
| Năm | `dim_date[year]` | Dropdown | Multi-select |
| Quý | `dim_date[quarter]` | Button (1/2/3/4) | Tương tác với Line chart |
| Tháng | `dim_date[month_name]` | Dropdown | Chỉ hiện khi chọn Quý |
| Phân khúc thu nhập | `dim_customer[income_segment]` | Checkbox (Low/Medium/High/Premium) | Mặc định All |
| Bậc rủi ro tín dụng | `dim_customer[credit_risk_tier]` | Checkbox (Poor/Fair/Good/Excellent) | Highlight màu đỏ-xanh |
| Giới tính | `dim_customer[gender]` | Toggle (Male/Female/All) | |
| Số thẻ sở hữu | `dim_card[num_cards_issued]` | Range slider (1-10) | |
| Năm còn lại đến hưu | `dim_customer[years_to_retirement]` | Range slider | Lọc KH gần hưu |
| Loại danh mục MCC | `dim_mcc[mcc_description]` | Search dropdown | Cho phép tìm kiếm |
| Trạng thái giao dịch | `fact_transaction[has_error]` | Toggle (Tất cả / Thành công / Lỗi) | |

---

## 3. Chủ đề CD2 – Phân tích Giao dịch & Quản trị Rủi ro

### 3.1 Mục tiêu nghiệp vụ
> **Tối ưu trải nghiệm người dùng** (giảm lỗi PIN, tăng tỷ lệ chip transaction) và **xây dựng hàng rào ngăn chặn rủi ro vỡ nợ** bằng cách theo dõi các tín hiệu cảnh báo sớm (Early Warning Signals – EWS).

### 3.2 Đối tượng sử dụng dashboard

| Vai trò | Nhu cầu chính | Tần suất xem |
|---|---|---|
| **Risk Officer / Credit Risk Analyst** | Giám sát danh mục rủi ro, phát hiện KH có dấu hiệu vỡ nợ | Hàng ngày |
| **Operations Manager** | Theo dõi lỗi giao dịch, hiệu suất kênh (chip vs. swipe) | Hàng ngày |
| **Fraud Analyst** | Phát hiện giao dịch bất thường theo địa lý, thời gian | Thời gian thực / ngày |
| **Chief Risk Officer (CRO)** | Dashboard tổng hợp rủi ro danh mục (Portfolio View) | Hàng tuần |
| **Customer Experience Team** | Phân tích nguyên nhân lỗi giao dịch để cải thiện UX | Hàng tuần |

---

### 3.3 KPI Đề xuất

#### Nhóm KPI Chất lượng Giao dịch (Transaction Health)

| # | Tên KPI | Công thức DAX | Mục tiêu (target) |
|---|---|---|---|
| K15 | **Tổng số giao dịch** | `COUNTROWS(fact_transaction)` | Benchmark so kỳ trước |
| K16 | **Tỷ lệ giao dịch lỗi** | `DIVIDE(SUM(fact_transaction[has_error]), [Total Transactions])` | < 2% (ngưỡng SLA) |
| K17 | **Tỷ lệ dùng Chip** | `DIVIDE(SUM(fact_transaction[is_chip_used]), [Total Transactions])` | > 80% (mục tiêu bảo mật) |
| K18 | **Tổng giá trị GD lỗi** | `SUMX(FILTER(fact_transaction,[has_error]=TRUE),[amount])` | Thiệt hại tiềm năng |
| K19 | **Số loại lỗi phân biệt** | `DISTINCTCOUNT(fact_transaction[errors])` | Đo đa dạng nguyên nhân lỗi |
| K20 | **Mean Transaction Value (MTV)** | `AVERAGE(fact_transaction[amount])` | Phát hiện giao dịch bất thường (outlier) |

#### Nhóm KPI Rủi ro Danh mục (Portfolio Risk)

| # | Tên KPI | Công thức DAX | Mục tiêu |
|---|---|---|---|
| K21 | **Tỷ lệ KH rủi ro cao (Poor tier)** | `DIVIDE(COUNTROWS(FILTER(dim_customer,[credit_risk_tier]="Poor")), [Total Customers])` | < 10% |
| K22 | **Tổng dư nợ nhóm rủi ro cao** | `SUMX(FILTER(dim_customer,[credit_risk_tier]="Poor"),[total_debt])` | Exposure tối đa cho phép |
| K23 | **Tỷ lệ KH DTI > 0.5** | `DIVIDE(COUNTROWS(FILTER(dim_customer,[debt_to_income_ratio]>0.5)),[Total Customers])` | < 5% (warning threshold) |
| K24 | **Credit Score bình quân danh mục** | `AVERAGE(dim_customer[credit_score])` | > 680 (danh mục khỏe mạnh) |
| K25 | **Số KH PIN lỗi thời (pin_age_years > 3)** | `COUNTROWS(FILTER(dim_card,[pin_age_years]>3))` | Cơ sở để gửi nhắc đổi PIN |
| K26 | **Tỷ lệ thẻ sắp hết hạn (≤ 60 ngày)** | `DIVIDE(COUNTROWS(FILTER(dim_card,DATEDIFF(TODAY(),[expires],DAY)<=60)),[Total Cards])` | Trigger renewal campaign |

#### Nhóm KPI Early Warning Signals (EWS)

| # | Tên KPI | Logic phát hiện | Hành động đề xuất |
|---|---|---|---|
| K27 | **KH tăng đột biến chi tiêu (>200% avg)** | Chi tiêu tháng hiện tại > 2× bình quân 6 tháng trước | Xác minh danh tính, gọi điện |
| K28 | **GD bất thường ngoài vùng địa lý quen thuộc** | Merchant state ≠ customer address state, amount > 1M | Flag for fraud review |
| K29 | **KH liên tiếp có GD lỗi ≥ 3 lần/tuần** | Window function: lỗi liên tiếp trên cùng card_id | Gợi ý đổi thẻ hoặc hỗ trợ UX |
| K30 | **KH có credit_score giảm + DTI tăng** | Kết hợp với dữ liệu refresh định kỳ | Hạ hạn mức tín dụng chủ động |

---

### 3.4 Thiết kế trang Dashboard CD2

#### Trang 1: "Tổng quan Sức khỏe Giao dịch" (Operations View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Ngày/Tuần/Tháng] [SLICER: card_brand] [SLICER: has_error] │
├──────────┬──────────┬──────────┬──────────┬───────────────────┤
│  K15     │  K16     │  K17     │  K18     │  K19              │
│ Tổng GD  │ % Lỗi   │ % Chip   │ GT Lỗi  │ Loại lỗi         │
│ [vs PY]  │ 🔴 nếu  │ 🟢 nếu  │          │ phân biệt         │
│          │ > 2%     │ > 80%    │          │                   │
├──────────┴──────────┴──────────┴──────────┴───────────────────┤
│  [Line Chart: Xu hướng Tổng GD & GD Lỗi theo ngày]           │
│   Hai trục Y: Số lượng GD (trái) | Tỷ lệ lỗi % (phải)       │
│   → Phát hiện ngày đột biến lỗi                               │
├──────────────────────────┬─────────────────────────────────────┤
│  [Bar Chart: Top 10      │  [Donut: Chip vs. Swipe vs. Online │
│   Loại lỗi (errors)      │   – break by has_error]            │
│   theo Số lượng GD]      │                                     │
│                          │  [KPI: MTV [K20] với Conditional   │
│                          │   formatting highlight outlier]     │
└──────────────────────────┴─────────────────────────────────────┘
```

---

#### Trang 2: "Bản đồ Rủi ro Giao dịch" (Fraud & Geo Analysis)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Tháng] [SLICER: has_error = TRUE] [SLICER: amount range] │
├────────────────────────────────────────────────────────────────┤
│  [Map USA: Điểm = merchant_city/state]                         │
│   Kích thước = Số GD lỗi | Màu = Tỷ lệ lỗi (red gradient)   │
│   → Phát hiện vùng địa lý có tỷ lệ lỗi cao bất thường        │
├──────────────────────────┬─────────────────────────────────────┤
│  [Scatter: amount vs     │  [Heatmap: Merchant State x        │
│   pin_age_years          │   MCC category vs Tổng GD lỗi]    │
│   – màu = has_error      │                                     │
│   → GD lớn + PIN cũ =   │  [Table: Top 20 GD đáng ngờ       │
│   high risk combo]       │   (amount > 95th percentile        │
│                          │    AND has_error = TRUE)]          │
└──────────────────────────┴─────────────────────────────────────┘
```

---

#### Trang 3: "Giám sát Rủi ro Danh mục" (Risk Portfolio View)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: credit_risk_tier] [SLICER: income_segment]           │
│  [SLICER: DTI threshold slider]                                │
├──────────┬──────────┬──────────┬──────────┬───────────────────┤
│  K21     │  K22     │  K23     │  K24     │  K25              │
│ % KH     │ Tổng nợ │ % DTI>0.5│ CS bình  │ PIN cũ            │
│ Poor tier│ Poor tier│          │ quân     │ (> 3 năm)         │
├──────────┴──────────┴──────────┴──────────┴───────────────────┤
│  [Waterfall/Funnel: Phân tầng KH theo credit_risk_tier        │
│   Excellent → Good → Fair → Poor                              │
│   Giá trị = Tổng dư nợ mỗi tầng]                             │
├──────────────────────────┬─────────────────────────────────────┤
│  [Matrix: income_segment │  [Gauge Chart: DTI danh mục        │
│   × credit_risk_tier     │   Mục tiêu < 0.35]                 │
│   Giá trị = Số KH        │                                     │
│   Màu nền = avg(DTI)     │  [KPI: Thẻ sắp hết hạn K26        │
│   → Cross-tab rủi ro     │   với Alert nếu > 5%]             │
│   two-way]               │                                     │
└──────────────────────────┴─────────────────────────────────────┘
```

---

#### Trang 4: "Cảnh báo Sớm & Hành động" (EWS Action Center)

```
┌────────────────────────────────────────────────────────────────┐
│  [SLICER: Mức độ cảnh báo: Cao/Trung bình/Thấp]               │
│  [SLICER: Ngày cảnh báo]                                       │
├────────────────────────────────────────────────────────────────┤
│  [Summary Cards]                                               │
│  K27: KH chi tiêu đột biến | K28: GD ngoài vùng              │
│  K29: KH lỗi liên tiếp     | K30: KH suy giảm tín nhiệm     │
├────────────────────────────────────────────────────────────────┤
│  [Bảng tổng hợp EWS: customer_id | Loại cảnh báo | Mức độ   │
│   | Ngày phát sinh | Giá trị GD | credit_risk_tier hiện tại  │
│   | Hành động đề xuất]                                         │
│   → Hỗ trợ Click-through sang hồ sơ KH (Page drill-through)  │
├────────────────────────────────────────────────────────────────┤
│  [Conditional formatting rules:]                               │
│  🔴 Đỏ = Cao: DTI>0.5 AND credit_risk_tier=Poor              │
│  🟡 Vàng = Trung bình: GD lỗi liên tiếp OR pin_age>3 năm    │
│  🟢 Xanh = Thấp: Chi tiêu đột biến nhưng credit_score tốt    │
└────────────────────────────────────────────────────────────────┘
```

---

### 3.5 Bộ lọc & Slicer CD2

| Slicer | Nguồn trường | Loại control | Ghi chú |
|---|---|---|---|
| Khoảng thời gian | `dim_date[full_date]` | Date Range Picker | Mặc định: 30 ngày gần nhất |
| Granularity | `dim_date[year/quarter/month/day]` | Toggle group | Drill-down tự động |
| Có lỗi hay không | `fact_transaction[has_error]` | Toggle (Tất cả/Có lỗi/Không lỗi) | Mặc định: Tất cả |
| Loại lỗi | `fact_transaction[errors]` | Multi-select dropdown | Hỗ trợ text search |
| Loại thẻ | `dim_card[card_type]` | Checkbox | |
| Thương hiệu thẻ | `dim_card[card_brand]` | Checkbox | |
| Có chip không | `dim_card[has_chip]` | Toggle | |
| Khoảng tiền GD | `fact_transaction[amount]` | Range slider | Giúp lọc micro/macro transaction |
| Trạng thái rủi ro | `dim_customer[credit_risk_tier]` | Color-coded checkbox | Red=Poor, Orange=Fair |
| DTI threshold | `dim_customer[debt_to_income_ratio]` | Numeric slicer (≥ X) | |
| Bang/Tiểu bang | `dim_merchant[merchant_state]` | Dropdown | Hỗ trợ multi-select |
| Danh mục MCC | `dim_mcc[mcc_description]` | Search dropdown | |
| Ngày cuối tuần | `dim_date[is_weekend]` | Toggle | |

---

## 4. Thiết kế kỹ thuật Power BI

### 4.1 Kết nối dữ liệu

```
Power BI → DirectQuery / Import Mode → PostgreSQL (Gold Layer)
```

| Bảng | Chế độ khuyến nghị | Lý do |
|---|---|---|
| `fact_transaction` | Import (nếu < 50M rows) / DirectQuery | Bảng lớn, cần aggregation |
| `dim_customer` | Import | Dimension nhỏ, cần cross-filter |
| `dim_card` | Import | |
| `dim_date` | Import | Bảng lịch cố định |
| `dim_merchant` | Import | |
| `dim_mcc` | Import | |

### 4.2 Quan hệ trong Power BI Data Model

```
fact_transaction[date_key]      → dim_date[date_key]       (Many-to-One) ✓
fact_transaction[customer_id]   → dim_customer[customer_id] (Many-to-One) ✓
fact_transaction[card_id]       → dim_card[card_id]         (Many-to-One) ✓
fact_transaction[merchant_id]   → dim_merchant[merchant_id] (Many-to-One) ✓
fact_transaction[mcc_id]        → dim_mcc[mcc_id]           (Many-to-One) ✓
```

> **Lưu ý:** `dim_card[customer_id]` → `dim_customer[customer_id]` tạo vòng lặp — dùng inactive relationship, kích hoạt bằng `USERELATIONSHIP()` khi cần.

### 4.3 Các Measure DAX cốt lõi

```dax
-- ===== MEASURES CHUNG =====

Total Transactions = COUNTROWS(fact_transaction)

Total Amount = SUM(fact_transaction[amount])

Active Customers = DISTINCTCOUNT(fact_transaction[customer_id])

Error Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_transaction), fact_transaction[has_error] = TRUE()),
    [Total Transactions],
    0
)

Chip Usage Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_transaction), fact_transaction[is_chip_used] = TRUE()),
    [Total Transactions],
    0
)

ARPU = DIVIDE([Total Amount], [Active Customers], 0)

-- ===== MEASURES CD1 =====

Upsell Candidates = 
CALCULATE(
    COUNTROWS(dim_customer),
    dim_customer[credit_risk_tier] IN {"Good", "Excellent"},
    dim_customer[num_credit_cards] < 3
)

Portfolio Avg Credit Score = AVERAGE(dim_customer[credit_score])

High DTI Customer % = 
DIVIDE(
    CALCULATE(COUNTROWS(dim_customer), dim_customer[debt_to_income_ratio] > 0.5),
    COUNTROWS(dim_customer),
    0
)

-- ===== MEASURES CD2 =====

Error Amount = 
CALCULATE([Total Amount], fact_transaction[has_error] = TRUE())

MoM Error Rate Change = 
VAR CurrentMonth = [Error Rate %]
VAR PrevMonth = CALCULATE([Error Rate %], DATEADD(dim_date[full_date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0)

Cards Expiring Soon = 
CALCULATE(
    COUNTROWS(dim_card),
    DATEDIFF(TODAY(), dim_card[expires], DAY) <= 60,
    DATEDIFF(TODAY(), dim_card[expires], DAY) >= 0
)

Old PIN Cards = CALCULATE(COUNTROWS(dim_card), dim_card[pin_age_years] > 3)
```

### 4.4 Row-Level Security (RLS)

| Role | Điều kiện lọc | Đối tượng |
|---|---|---|
| `Credit_Risk_Viewer` | Chỉ xem KH có `credit_risk_tier IN ('Poor','Fair')` | Risk Analyst |
| `Marketing_Viewer` | Chỉ xem KH có `credit_risk_tier IN ('Good','Excellent')` | Marketing Team |
| `Executive` | Toàn bộ dữ liệu | C-level, CRO |
| `Operations` | Không xem `dim_customer[credit_score]`, `total_debt` | Ops Team |

---

## 5. Hướng dẫn Triển khai

### 5.1 Thứ tự xây dựng

```
Bước 1: Kết nối dữ liệu & kiểm tra quan hệ (Data Model View)
Bước 2: Tạo dim_date nếu chưa có seed (M Query hoặc DAX calendar table)
Bước 3: Tạo các Measure cốt lõi (Measure table riêng)
Bước 4: Xây dựng trang CD2-P1 (đơn giản nhất, kiểm tra data)
Bước 5: Xây dựng trang CD1-P1 (Treemap + KPI cards)
Bước 6: Xây dựng các trang drill-through
Bước 7: Thiết lập RLS
Bước 8: Publish lên Power BI Service + cấu hình refresh
```

### 5.2 Refresh Schedule

| Loại | Tần suất | Phương thức |
|---|---|---|
| Dữ liệu giao dịch | Hàng ngày lúc 06:00 AM | Scheduled Refresh |
| Dữ liệu khách hàng | Hàng tuần (Thứ Hai) | Scheduled Refresh |
| EWS alerts | Thời gian thực (nếu có Premium) | Push Dataset API |

### 5.3 Checklists chất lượng trước khi go-live

- [ ] Tổng GD Power BI khớp với tổng GD trong PostgreSQL (row count + SUM)
- [ ] `Error Rate %` hiển thị đúng với query SQL kiểm tra thủ công
- [ ] RLS hoạt động đúng với từng role
- [ ] Tất cả slicer cross-filter đúng giữa các visual
- [ ] Drill-through từ EWS → hồ sơ khách hàng hoạt động
- [ ] Report render < 3 giây ở kích thước dữ liệu production
- [ ] Thử nghiệm trên mobile layout (Power BI Mobile)

---

## 6. Tóm tắt nhanh

| | CD1 – Phân khúc KH | CD2 – Quản trị Rủi ro |
|---|---|---|
| **Người dùng chính** | Product Manager, Marketing Analyst | Risk Officer, Fraud Analyst, CRO |
| **KPI cốt lõi** | ARPU, CLV, Upsell Candidates, Tỷ lệ Premium | Error Rate%, Chip Rate%, DTI>0.5%, Credit Score |
| **Slicer quan trọng nhất** | income_segment, credit_risk_tier, years_to_retirement | has_error, amount range, credit_risk_tier, date range |
| **Visual nổi bật** | Treemap phân khúc, Scatter CLV, Map địa lý KH | Heatmap lỗi theo ngày/bang, EWS Action Table, Gauge DTI |
| **Số trang** | 3 trang | 4 trang |
| **Tần suất xem** | Hàng tuần / theo chiến dịch | Hàng ngày / thời gian thực |

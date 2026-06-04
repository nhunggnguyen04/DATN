# CLAUDE.md

Tài liệu này hướng dẫn Claude Code (claude.ai/code) khi làm việc với mã nguồn trong repository này.

## Tổng quan

DATN là nền tảng dữ liệu ngân hàng (đồ án tốt nghiệp) gồm hai pipeline độc lập:

1. **Pipeline dữ liệu có cấu trúc** — Nguồn SQL Server → Bronze → Silver (Data Vault) → Gold (Star Schema), được điều phối bởi **Airflow** và biến đổi bằng **dbt**. Đây là pipeline chính.
2. **Pipeline dữ liệu phi cấu trúc** — OCR căn cước công dân (CCCD) và sổ tiết kiệm bằng **PaddleOCR**, đổ trực tiếp vào hai bảng Bronze. Không có Silver/Gold, không có Data Vault.

Comment, docstring và commit message đều viết bằng **tiếng Việt**; hãy giữ đúng phong cách đó khi sửa các file hiện có.

## Kiến trúc

### Cơ sở dữ liệu
- **Data warehouse đích**: SQL Server, database `DATN`, các schema `bronze` / `silver` / `gold` / `audit`. dbt lấy thông tin kết nối từ `dbt_bank/profiles.yml`; các script Python đọc biến môi trường `TARGET_*` (xem `.env`).
- **Nguồn OLTP**: SQL Server, các bảng `banking.*` (users, cards, transactions, mcc). Đọc qua biến môi trường `SOURCE_*`.
- **Metadata Airflow**: Postgres (định nghĩa trong `docker-compose.yaml`), tách biệt với warehouse.

Lưu ý: `.env.example`, README và `dbt_bank/profiles.yml` đặt tên database khác nhau (`bank_dwh` / `banking_pipeline` / `DATN`). **Cấu hình đang chạy thực tế là `DATN`** (xem `profiles.yml` và `models/bronze/sources.yml`).

### Luồng dữ liệu pipeline có cấu trúc (`dags/banking_structured_dag.py`, `@daily 02:00`)
```
precheck_source_db → extract_bronze (4 entity) → compute_mns → validate_mns_change_ratio
  → dbt silver hubs → links → satellites → test
  → dbt gold dims → fact → test → notify
```

**Mẫu incremental MNS** (quy ước cốt lõi): mỗi entity có ba bảng Bronze — `*_tdy` (hôm nay), `*_pdy` (snapshot lũy kế mới nhất), `*_mns` (change-set gồm `id` + `operation_flag` I/U/D).
- `scripts/extract/load_bronze_<entity>.py` truncate `*_tdy` và nạp batch hôm nay từ nguồn. Nó **không** đụng tới `*_pdy`.
- `scripts/extract/<entity>_mns.py` so sánh `*_tdy` với `*_pdy` để tính cờ I/U, sau đó upsert `*_tdy` vào `*_pdy` để giữ `*_pdy` là snapshot lũy kế. **Không sinh cờ `D`** ở chế độ chạy theo ngày (một user vắng mặt hôm nay không có nghĩa bị xóa) — xem comment trong `users_mns.py`.
- `--run-date YYYY-MM-DD` bật chế độ theo ngày (lọc theo ngày giao dịch); bỏ flag này thì nạp toàn bộ nguồn (backfill). `users`/`cards`/`mcc_codes` không có cột ngày nên được lọc bằng cách join với `transactions`.

### dbt (`dbt_bank/`)
- **Medallion + Data Vault**: `models/silver/{hubs,links,satellites,ref}` dựng Data Vault; `models/gold/{dimensions,facts}` dựng star schema.
- **Hash key**: business/hash key dùng MD5 qua các macro trong `macros/hash.sql` (`hash_md5`, `hash_md5_concat`). Hãy tái sử dụng chúng — đừng tự viết `HASHBYTES`.
- **Satellite là SCD Type 2**: incremental với `pre_hook` đóng record cũ (`effective_to = now`) và chèn phiên bản mới, key theo `(hk_*, effective_from)` + `hashdiff`. Xem `models/silver/satellites/sat_customer_profile.sql` làm mẫu chuẩn.
- **Gold fact** (`fact_transaction.sql`) là `incremental` với `delete+insert` key theo `run_date`; truyền `--vars '{"run_date":"YYYY-MM-DD"}'` để nạp theo ngày idempotent, hoặc để trống để full refresh.
- **Tag điều khiển điều phối**, không phải để chọn file: `tag:hub`, `tag:link`, `tag:satellite`, `tag:silver`, `tag:dim`, `tag:gold`. Materialization và schema được gán theo thư mục trong `dbt_project.yml`.
- `macros/generate_schema_name.sql` khiến custom schema giữ nguyên tên (không thêm tiền tố `target.schema_`), nên `+schema: silver` đổ vào `silver` chứ không phải `dbo_silver`.

### Audit & kiểm tra chất lượng dữ liệu
- Mọi task Airflow đều được bọc bởi `AuditedBashOperator` / `AuditedPythonOperator` (`dags/common/operators.py`), ghi một dòng vào `audit.pipeline_run_log` khi bắt đầu và cập nhật khi thành công/thất bại. Lỗi audit không bao giờ che lỗi thật của task.
- Script Python có thể tự báo số dòng qua context manager `audit_run(...)` trong `scripts/utils/audit_logger.py`.
- `dags/data_quality_dag.py` (`@daily 04:00`) chạy các DQ check (row-count drift, freshness, null FK, duplicate PK, OCR confidence). Phần lớn check **chỉ cảnh báo**; `check_duplicate_pk` và freshness khi dữ liệu cũ thì **raise**.
- Chạy `python scripts/setup_audit.py` một lần để tạo schema/bảng `audit` từ `sql/create_audit_tables.sql`.

### Quy ước xuyên suốt
- `scripts/utils/db_connection.py` là nguồn duy nhất tạo engine DB: `get_source_engine()` / `get_target_engine()`, dựng từ biến môi trường `{SOURCE,TARGET}_*` với `fast_executemany=True`.
- Đường dẫn DAG, pool, default args và template `SCRIPT_ENV` (inject các biến `AIRFLOW_*` để script ghi được audit) nằm trong `dags/common/constants.py`. Đường dẫn là tuyệt đối trong container (`/opt/airflow/...`).
- Pool Airflow giới hạn concurrency: `source_db_pool` (2), `dbt_pool` (4), `ocr_pool` (1).

## Môi trường

Hai virtualenv Python riêng biệt (PaddleOCR xung đột với stack Airflow/dbt):
- `.venv` — core + pipeline có cấu trúc (`requirements.txt`).
- `.venv_ocr` — chỉ PaddleOCR (`paddlepaddle==3.2.2` + `paddleocr`; **không** dùng 3.3.x vì lỗi trên Windows CPU).

Trong Docker (`Dockerfile`) chúng trở thành `/opt/airflow/.venv_ocr` và một `/opt/dbt_venv` riêng cho dbt; `dbt` được symlink vào PATH.

## Các lệnh thường dùng

Tất cả lệnh dùng **PowerShell** (host Windows).

```powershell
# --- Cài đặt ---
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/utils/db_connection.py     # kiểm tra nhanh kết nối DB source+target
python scripts/setup_audit.py             # chạy 1 lần: tạo schema/bảng audit

# --- Pipeline có cấu trúc (chạy tay, không qua Airflow) ---
# 1. Extract nguồn → bronze.*_tdy  (thêm --run-date YYYY-MM-DD cho chế độ theo ngày)
python scripts/extract/load_bronze_users.py
python scripts/extract/load_bronze_cards.py
python scripts/extract/load_bronze_transactions.py
python scripts/extract/load_bronze_mcc_codes.py
# 2. Tính change-set MNS
python scripts/extract/users_mns.py
python scripts/extract/cards_mns.py
python scripts/extract/transactions_mns.py
python scripts/extract/mcc_codes_mns.py
# 3. Biến đổi bằng dbt
cd dbt_bank
dbt run --select tag:hub
dbt run --select tag:link
dbt run --select tag:satellite
dbt run --select tag:dim
dbt run --select fact_transaction --vars '{"run_date":"2026-05-25"}'
dbt test --select tag:silver
dbt test --select tag:gold
cd ..

# --- Chạy một model / test dbt đơn lẻ ---
cd dbt_bank
dbt run  --select sat_customer_profile
dbt test --select fact_transaction        # chỉ chạy test của model này
dbt run  --select sat_customer_profile --full-refresh   # dựng lại SCD2 từ đầu

# --- Pipeline phi cấu trúc / OCR ---
.venv_ocr\Scripts\Activate.ps1
python scripts/extract/ocr_extract_id_card.py --run-date 2026-05-25
python scripts/extract/ocr_extract_savings_book.py --run-date 2026-05-25
.venv\Scripts\Activate.ps1
python scripts/extract/load_bronze_unstructured.py --csv data/unstructured/extracted/id_card_extractions_2026-05-25.csv --doc-type id_card
python scripts/extract/load_bronze_unstructured.py --csv data/unstructured/extracted/savings_book_roi_extractions_2026-05-25.csv --doc-type savings_book
```

### Airflow (Docker)
```powershell
docker compose build
docker compose up -d          # Postgres + airflow-init + webserver (:8080) + scheduler
# Web UI: http://localhost:8080  (admin / admin)
docker compose logs -f airflow-scheduler
docker compose down
```
SQL Server chạy trên **host**, không nằm trong Compose; các container kết nối qua `host.docker.internal` (xem `profiles.yml`) / biến `TARGET_SERVER` trong `.env`.

DAG: `banking_structured_dag` (hằng ngày 02:00), `data_quality_dag` (hằng ngày 04:00), `ocr_unstructured_dag` (trigger thủ công). Hành vi được tinh chỉnh qua Airflow Variable — `skip_mns_validation`, `ocr_conf_threshold`, `freshness_max_hours`, `row_count_drift_pct` (tên định nghĩa trong `dags/common/constants.py`).

## Lưu ý quan trọng
- README gọi `dags/` là "placeholder" — điều này đã **lỗi thời**. Các DAG là thật và chính là điểm vào điều phối.
- `.env`, `docker-compose.yaml` và `dbt_bank/profiles.yml` chứa credential hardcode (mật khẩu app SMTP, mật khẩu SQL `sa`). Hãy coi đó là thực tế của dự án nhưng đừng nhân bản chúng vào code mới hay log.
- Bronze giữ mọi thứ ở dạng chuỗi (ngày dạng `DD/MM/YYYY`, số tiền có dấu phẩy); việc ép kiểu diễn ra ở Silver. Đừng thêm ép kiểu trong các loader Bronze.

"""
Load OCR extraction results vào bronze tables.

  bronze.id_card_results       - CCCD (ocr_extract_id_card.py / generate_data.py)
  bronze.savings_book_results  - Sổ tiết kiệm (ocr_extract_savings_book.py)

DDL tham khảo: scripts/sql/create_bronze_ocr_tables.sql

Usage:
    python scripts/extract/load_bronze_unstructured.py --csv data/unstructured/extracted/id_card_extractions_2026-05-25.csv --doc-type id_card
    python scripts/extract/load_bronze_unstructured.py --csv data/unstructured/extracted/savings_book_extractions_2026-05-25.csv --doc-type savings_book
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine

SCHEMA = "bronze"
ID_CARD_TABLE = "id_card_results"
SAVINGS_BOOK_TABLE = "savings_book_results"

# -----------------------------------------------------------------------
# DDL — id_card_results (schema khớp với CSV từ generate_data.py)
# -----------------------------------------------------------------------

_DDL_SCHEMA = "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze')"

_DDL_ID_CARD = """
CREATE TABLE bronze.id_card_results (
    id                  BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_id_card_results PRIMARY KEY,
    file                NVARCHAR(200)   NULL,
    file_path           NVARCHAR(500)   NULL,
    run_date            DATE            NULL,
    user_id             INT             NULL,
    full_name           NVARCHAR(300)   NULL,
    id_number           NVARCHAR(50)    NULL,
    date_of_birth       NVARCHAR(20)    NULL,
    sex                 NVARCHAR(20)    NULL,
    nationality         NVARCHAR(100)   NULL,
    place_of_origin     NVARCHAR(300)   NULL,
    place_of_residence  NVARCHAR(500)   NULL,
    issue_date          NVARCHAR(20)    NULL,
    expiry_date         NVARCHAR(20)    NULL,
    final_confidence    FLOAT           NULL,
    ocr_confidence      FLOAT           NULL,
    parse_confidence    FLOAT           NULL,
    plausible_fields    INT             NULL,
    _loaded_at          DATETIME2       NOT NULL DEFAULT GETDATE()
)
"""

_DDL_SAVINGS_BOOK = """
CREATE TABLE bronze.savings_book_results (
    id                  BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_savings_book_results PRIMARY KEY,
    file                NVARCHAR(200)   NULL,
    file_path           NVARCHAR(500)   NULL,
    run_date            DATE            NULL,
    user_id             INT             NULL,
    transaction_date    NVARCHAR(20)    NULL,
    description         NVARCHAR(300)   NULL,
    transaction_code    NVARCHAR(20)    NULL,
    transaction_amount  NVARCHAR(50)    NULL,
    balance             NVARCHAR(50)    NULL,
    interest_rate       NVARCHAR(20)    NULL,
    signature           NVARCHAR(200)   NULL,
    final_confidence    FLOAT           NULL,
    ocr_confidence      FLOAT           NULL,
    parse_confidence    FLOAT           NULL,
    plausible_fields    INT             NULL,
    _loaded_at          DATETIME2       NOT NULL DEFAULT GETDATE()
)
"""

# Cột dùng để phát hiện schema lệch (nếu cột này không có trong bảng → DROP+CREATE)
_ID_CARD_SIGNATURE_COL = "file"
_SAVINGS_BOOK_SIGNATURE_COL = "file"

_DDL_IDX_ID_CARD = [
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_id_card_results_run_date' AND object_id=OBJECT_ID('bronze.id_card_results')) CREATE INDEX IX_id_card_results_run_date ON bronze.id_card_results (run_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_id_card_results_user_id'  AND object_id=OBJECT_ID('bronze.id_card_results')) CREATE INDEX IX_id_card_results_user_id  ON bronze.id_card_results (user_id)",
]
_DDL_IDX_SAVINGS_BOOK = [
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_savings_book_results_run_date' AND object_id=OBJECT_ID('bronze.savings_book_results')) CREATE INDEX IX_savings_book_results_run_date ON bronze.savings_book_results (run_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_savings_book_results_user_id'  AND object_id=OBJECT_ID('bronze.savings_book_results')) CREATE INDEX IX_savings_book_results_user_id  ON bronze.savings_book_results (user_id)",
]


# -----------------------------------------------------------------------
# Schema management
# -----------------------------------------------------------------------

def _table_has_column(engine, schema: str, table: str, column: str) -> bool:
    """Return True nếu bảng tồn tại VÀ có cột column."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t AND COLUMN_NAME = :c
        """), {"s": schema, "t": table, "c": column}).scalar()
    return bool(row)


def _recreate_table(engine, schema: str, table: str, create_ddl: str) -> None:
    """DROP TABLE nếu tồn tại, rồi CREATE lại."""
    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {schema}.{table}"))
    with engine.begin() as conn:
        conn.execute(text(create_ddl))
    print(f"  Recreated {schema}.{table}")


def _ensure_tables(engine) -> None:
    """Tạo schema bronze, đảm bảo cả 2 bảng tồn tại với schema đúng."""
    with engine.begin() as conn:
        conn.execute(text(_DDL_SCHEMA))

    # id_card_results: kiểm tra cột 'file' — nếu thiếu nghĩa là schema cũ, cần recreate
    if not _table_has_column(engine, SCHEMA, ID_CARD_TABLE, _ID_CARD_SIGNATURE_COL):
        print(f"  Schema mismatch hoặc bảng chưa tồn tại: {SCHEMA}.{ID_CARD_TABLE} → recreating...")
        _recreate_table(engine, SCHEMA, ID_CARD_TABLE, _DDL_ID_CARD)
    else:
        print(f"  {SCHEMA}.{ID_CARD_TABLE} schema OK")

    # savings_book_results
    if not _table_has_column(engine, SCHEMA, SAVINGS_BOOK_TABLE, _SAVINGS_BOOK_SIGNATURE_COL):
        print(f"  Schema mismatch hoặc bảng chưa tồn tại: {SCHEMA}.{SAVINGS_BOOK_TABLE} → recreating...")
        _recreate_table(engine, SCHEMA, SAVINGS_BOOK_TABLE, _DDL_SAVINGS_BOOK)
    else:
        print(f"  {SCHEMA}.{SAVINGS_BOOK_TABLE} schema OK")

    for stmt in _DDL_IDX_ID_CARD + _DDL_IDX_SAVINGS_BOOK:
        with engine.begin() as conn:
            conn.execute(text(stmt))


# -----------------------------------------------------------------------
# File reading
# -----------------------------------------------------------------------

def _read_file(path: Path) -> pd.DataFrame:
    """Đọc CSV hoặc XLSX; fallback sang .xlsx nếu .csv không tồn tại."""
    if path.exists():
        if path.suffix.lower() in (".xlsx", ".xls"):
            return pd.read_excel(path)
        return pd.read_csv(path)

    xlsx = path.with_suffix(".xlsx")
    if xlsx.exists():
        print(f"  Note: {path.name} không tồn tại, đọc {xlsx.name}")
        return pd.read_excel(xlsx)

    raise FileNotFoundError(f"File không tìm thấy: {path}  (cũng thử {xlsx})")


# -----------------------------------------------------------------------
# Type coercion
# -----------------------------------------------------------------------

def _coerce_id_card(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "run_date" in df.columns:
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.date
    if "user_id" in df.columns:
        df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").astype(pd.Int64Dtype())
    if "plausible_fields" in df.columns:
        df["plausible_fields"] = pd.to_numeric(df["plausible_fields"], errors="coerce").astype(pd.Int64Dtype())
    for col in ("final_confidence", "ocr_confidence", "parse_confidence"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _coerce_savings_book(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "run_date" in df.columns:
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.date
    if "user_id" in df.columns:
        df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").astype(pd.Int64Dtype())
    if "plausible_fields" in df.columns:
        df["plausible_fields"] = pd.to_numeric(df["plausible_fields"], errors="coerce").astype(pd.Int64Dtype())
    for col in ("final_confidence", "ocr_confidence", "parse_confidence"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# -----------------------------------------------------------------------
# Main load function
# -----------------------------------------------------------------------

def load_to_bronze(csv_path: Path, doc_type: str, clear_existing: bool = True) -> int:
    if doc_type == "id_card":
        table_name, coerce_fn = ID_CARD_TABLE, _coerce_id_card
    elif doc_type == "savings_book":
        table_name, coerce_fn = SAVINGS_BOOK_TABLE, _coerce_savings_book
    else:
        raise ValueError(f"Unknown doc_type: {doc_type}")

    df = _read_file(csv_path)
    print(f"  Đọc {len(df)} dòng từ {csv_path.name}")

    df = coerce_fn(df)
    df["_loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    engine = get_target_engine()
    full_table = f"{SCHEMA}.{table_name}"

    _ensure_tables(engine)

    if clear_existing and "run_date" in df.columns and len(df) > 0:
        run_date_val = df["run_date"].iloc[0]
        if run_date_val is not None and str(run_date_val) not in ("NaT", "None", "nan"):
            with engine.begin() as conn:
                deleted = conn.execute(
                    text(f"DELETE FROM {full_table} WHERE run_date = :rd"),
                    {"rd": str(run_date_val)},
                ).rowcount
                print(f"  Xóa {deleted} dòng cũ cho run_date={run_date_val}")

    print(f"  Đang load {len(df)} dòng → {full_table} ...")
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        chunksize=500,
    )

    # Verify
    with engine.begin() as conn:
        total = conn.execute(text(
            f"SELECT COUNT(*) FROM {full_table} WHERE run_date = :rd",
        ), {"rd": str(df["run_date"].iloc[0]) if "run_date" in df.columns else None}).scalar()
        print(f"  Tổng dòng trong {full_table} cho run_date này: {total}")

    return len(df)


load_csv_to_bronze = load_to_bronze


def main():
    parser = argparse.ArgumentParser(description="Load OCR extraction results to bronze tables")
    parser.add_argument("--csv", required=True, help="Đường dẫn file CSV hoặc XLSX")
    parser.add_argument("--doc-type", required=True, choices=["id_card", "savings_book"])
    parser.add_argument("--no-clear", action="store_true",
                        help="Bỏ qua bước xóa dòng cũ cùng run_date trước khi load")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    print(f"Loading {csv_path.name} → bronze.{args.doc_type}_results")
    try:
        count = load_to_bronze(csv_path, args.doc_type, clear_existing=not args.no_clear)
        print(f"\n✓ Loaded {count} records")
        return 0
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

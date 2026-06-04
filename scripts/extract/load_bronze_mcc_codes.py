import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from scripts.utils.db_connection import get_source_engine, get_target_engine


SOURCE_QUERY_ALL = """
SELECT
    mcc_id,
    description
FROM banking.mcc_codes;
"""

# Load-theo-ngày: chỉ lấy mcc_codes xuất hiện trong giao dịch ngày run_date
# (mcc_codes không có cột ngày → JOIN gián tiếp qua transactions.mcc).
SOURCE_QUERY_BY_DATE = """
SELECT
    mcc_id,
    description
FROM banking.mcc_codes
WHERE mcc_id IN (
    SELECT DISTINCT mcc
    FROM banking.transactions
    WHERE CAST([date] AS DATE) = :run_date
);
"""


# PDY giờ là snapshot LŨY KẾ (trạng thái mới nhất của mọi mcc đã từng thấy), được
# duy trì bởi mcc_codes_mns.py SAU khi tính diff. Load script chỉ dọn TDY rồi nạp batch.
TRUNCATE_TDY_SQL = "TRUNCATE TABLE bronze.mcc_codes_tdy;"


def main():
    parser = argparse.ArgumentParser(description="Load bronze mcc_codes")
    parser.add_argument(
        "--run-date",
        default=None,
        help="Lọc mcc_codes theo ngày giao dịch (YYYY-MM-DD). Để trống = load toàn bộ.",
    )
    args = parser.parse_args()
    run_date = args.run_date or None

    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if run_date:
        print(f"Reading mcc_codes used on run_date={run_date} from source...")
        with source_engine.connect() as conn:
            df = pd.read_sql(text(SOURCE_QUERY_BY_DATE), conn, params={"run_date": run_date})
    else:
        print("Reading ALL mcc_codes from source (no date filter)...")
        df = pd.read_sql(SOURCE_QUERY_ALL, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Clearing bronze.mcc_codes_tdy (PDY giữ nguyên — snapshot lũy kế)...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(TRUNCATE_TDY_SQL)

    print("Loading new source data into bronze.mcc_codes_tdy...")
    df.to_sql(
        name="mcc_codes_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Loaded mcc_codes into bronze.mcc_codes_tdy successfully.")


if __name__ == "__main__":
    main()
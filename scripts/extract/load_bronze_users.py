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
    id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards
FROM banking.users;
"""

# Load-theo-ngày: chỉ lấy users có phát sinh giao dịch trong ngày run_date
# (users không có cột ngày → JOIN gián tiếp qua transactions.client_id).
SOURCE_QUERY_BY_DATE = """
SELECT
    id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards
FROM banking.users
WHERE id IN (
    SELECT DISTINCT client_id
    FROM banking.transactions
    WHERE CAST([date] AS DATE) = :run_date
);
"""


# PDY giờ là snapshot LŨY KẾ (trạng thái mới nhất của mọi user đã từng thấy), được
# duy trì bởi users_mns.py SAU khi tính diff. Load script chỉ cần dọn TDY rồi nạp
# batch hôm nay; KHÔNG đụng tới PDY ở đây.
TRUNCATE_TDY_SQL = "TRUNCATE TABLE bronze.users_tdy;"


def main():
    parser = argparse.ArgumentParser(description="Load bronze users")
    parser.add_argument(
        "--run-date",
        default=None,
        help="Lọc users theo ngày giao dịch (YYYY-MM-DD). Để trống = load toàn bộ.",
    )
    args = parser.parse_args()
    run_date = args.run_date or None

    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if run_date:
        print(f"Reading users transacting on run_date={run_date} from source...")
        with source_engine.connect() as conn:
            df = pd.read_sql(text(SOURCE_QUERY_BY_DATE), conn, params={"run_date": run_date})
    else:
        print("Reading ALL users from source (no date filter)...")
        df = pd.read_sql(SOURCE_QUERY_ALL, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Clearing bronze.users_tdy (PDY giữ nguyên — snapshot lũy kế)...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(TRUNCATE_TDY_SQL)

    print("Loading new source data into bronze.users_tdy...")
    df.to_sql(
        name="users_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Loaded users into bronze.users_tdy successfully.")


if __name__ == "__main__":
    main()
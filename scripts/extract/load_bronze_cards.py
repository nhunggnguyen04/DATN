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
    client_id,
    card_brand,
    card_type,
    card_number,
    expires,
    cvv,
    has_chip,
    num_cards_issued,
    credit_limit,
    acct_open_date,
    year_pin_last_changed
FROM banking.cards;
"""

# Load-theo-ngày: chỉ lấy cards có phát sinh giao dịch trong ngày run_date
# (cards không có cột ngày → JOIN gián tiếp qua transactions.card_id).
SOURCE_QUERY_BY_DATE = """
SELECT
    id,
    client_id,
    card_brand,
    card_type,
    card_number,
    expires,
    cvv,
    has_chip,
    num_cards_issued,
    credit_limit,
    acct_open_date,
    year_pin_last_changed
FROM banking.cards
WHERE id IN (
    SELECT DISTINCT card_id
    FROM banking.transactions
    WHERE CAST([date] AS DATE) = :run_date
);
"""


# PDY giờ là snapshot LŨY KẾ (trạng thái mới nhất của mọi card đã từng thấy), được
# duy trì bởi cards_mns.py SAU khi tính diff. Load script chỉ dọn TDY rồi nạp batch hôm nay.
TRUNCATE_TDY_SQL = "TRUNCATE TABLE bronze.cards_tdy;"


def main():
    parser = argparse.ArgumentParser(description="Load bronze cards")
    parser.add_argument(
        "--run-date",
        default=None,
        help="Lọc cards theo ngày giao dịch (YYYY-MM-DD). Để trống = load toàn bộ.",
    )
    args = parser.parse_args()
    run_date = args.run_date or None

    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if run_date:
        print(f"Reading cards transacting on run_date={run_date} from source...")
        with source_engine.connect() as conn:
            df = pd.read_sql(text(SOURCE_QUERY_BY_DATE), conn, params={"run_date": run_date})
    else:
        print("Reading ALL cards from source (no date filter)...")
        df = pd.read_sql(SOURCE_QUERY_ALL, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Clearing bronze.cards_tdy (PDY giữ nguyên — snapshot lũy kế)...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(TRUNCATE_TDY_SQL)

    print("Loading new source data into bronze.cards_tdy...")
    df.to_sql(
        name="cards_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Loaded cards into bronze.cards_tdy successfully.")


if __name__ == "__main__":
    main()
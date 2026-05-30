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
    [date],
    client_id,
    card_id,
    amount,
    use_chip,
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    errors
FROM banking.transactions;
"""

SOURCE_QUERY_BY_DATE = """
SELECT
    id,
    [date],
    client_id,
    card_id,
    amount,
    use_chip,
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    errors
FROM banking.transactions
WHERE CAST([date] AS DATE) = :run_date;
"""

MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.transactions_pdy;

INSERT INTO bronze.transactions_pdy (
    id,
    [date],
    client_id,
    card_id,
    amount,
    use_chip,
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    errors
)
SELECT
    id,
    [date],
    client_id,
    card_id,
    amount,
    use_chip,
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    errors
FROM bronze.transactions_tdy;

TRUNCATE TABLE bronze.transactions_tdy;
"""


def main():
    parser = argparse.ArgumentParser(description="Load bronze transactions")
    parser.add_argument(
        "--run-date",
        default=None,
        help="Lọc theo ngày giao dịch (YYYY-MM-DD). Không truyền = load toàn bộ.",
    )
    args = parser.parse_args()
    run_date = args.run_date

    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if run_date:
        print(f"Reading transactions for run_date={run_date} from source...")
        with source_engine.connect() as conn:
            df = pd.read_sql(
                text(SOURCE_QUERY_BY_DATE),
                conn,
                params={"run_date": run_date},
            )
    else:
        print("Reading ALL transactions from source (no date filter)...")
        df = pd.read_sql(SOURCE_QUERY_ALL, source_engine)

    print(f"Read {len(df)} rows from source.")

    print("Moving current transactions_tdy → transactions_pdy ...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

    print("Loading into bronze.transactions_tdy ...")
    df.to_sql(
        name="transactions_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print(f"Loaded {len(df)} transactions into bronze.transactions_tdy successfully.")


if __name__ == "__main__":
    main()

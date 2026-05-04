import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd

from scripts.utils.db_connection import get_source_engine, get_target_engine


SOURCE_QUERY = """
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
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    print("Reading transactions from source...")
    df = pd.read_sql(SOURCE_QUERY, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Moving current transactions TDY to PDY and clearing TDY...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

    print("Loading new source data into bronze.transactions_tdy...")
    df.to_sql(
        name="transactions_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Loaded transactions into bronze.transactions_tdy successfully.")


if __name__ == "__main__":
    main()
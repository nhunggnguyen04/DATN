import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd

from scripts.utils.db_connection import get_source_engine, get_target_engine


SOURCE_QUERY = """
SELECT
    mcc_id,
    description
FROM banking.mcc_codes;
"""


MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.mcc_codes_pdy;

INSERT INTO bronze.mcc_codes_pdy (
    mcc_id,
    description
)
SELECT
    mcc_id,
    description
FROM bronze.mcc_codes_tdy;

TRUNCATE TABLE bronze.mcc_codes_tdy;
"""


def main():
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    print("Reading mcc_codes from source...")
    df = pd.read_sql(SOURCE_QUERY, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Moving current TDY to PDY and clearing TDY...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

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
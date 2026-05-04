import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd

from scripts.utils.db_connection import get_source_engine, get_target_engine


SOURCE_QUERY = """
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


MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.users_pdy;

INSERT INTO bronze.users_pdy (
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
)
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
FROM bronze.users_tdy;

TRUNCATE TABLE bronze.users_tdy;
"""


def main():
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    print("Reading users from source...")
    df = pd.read_sql(SOURCE_QUERY, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Moving current users TDY to PDY and clearing TDY...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

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
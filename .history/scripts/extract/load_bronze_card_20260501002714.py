import pandas as pd

from scripts.utils.db_connection import get_source_engine, get_target_engine


SOURCE_QUERY = """
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


MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.cards_pdy;

INSERT INTO bronze.cards_pdy (
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
)
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
FROM bronze.cards_tdy;

TRUNCATE TABLE bronze.cards_tdy;
"""


def main():
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    print("Reading cards from source...")
    df = pd.read_sql(SOURCE_QUERY, source_engine)
    print(f"Read {len(df)} rows from source.")

    print("Moving current TDY to PDY and clearing TDY...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

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
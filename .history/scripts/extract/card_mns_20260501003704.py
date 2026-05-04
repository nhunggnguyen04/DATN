from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.cards_mns;

-- I: Có trong TDY nhưng không có trong PDY
INSERT INTO bronze.cards_mns (id, operation_flag)
SELECT
    t.id,
    'I' AS operation_flag
FROM bronze.cards_tdy t
LEFT JOIN bronze.cards_pdy p
    ON t.id = p.id
WHERE p.id IS NULL;

-- U: Có trong cả TDY và PDY nhưng dữ liệu thay đổi
INSERT INTO bronze.cards_mns (id, operation_flag)
SELECT
    t.id,
    'U' AS operation_flag
FROM bronze.cards_tdy t
INNER JOIN bronze.cards_pdy p
    ON t.id = p.id
WHERE
    ISNULL(t.client_id, -1) <> ISNULL(p.client_id, -1)
    OR ISNULL(t.card_brand, '') <> ISNULL(p.card_brand, '')
    OR ISNULL(t.card_type, '') <> ISNULL(p.card_type, '')
    OR ISNULL(t.card_number, '') <> ISNULL(p.card_number, '')
    OR ISNULL(t.expires, CONVERT(DATE, '1900-01-01')) <> ISNULL(p.expires, CONVERT(DATE, '1900-01-01'))
    OR ISNULL(t.cvv, '') <> ISNULL(p.cvv, '')
    OR ISNULL(t.has_chip, '') <> ISNULL(p.has_chip, '')
    OR ISNULL(t.num_cards_issued, -1) <> ISNULL(p.num_cards_issued, -1)
    OR ISNULL(t.credit_limit, -1) <> ISNULL(p.credit_limit, -1)
    OR ISNULL(t.acct_open_date, CONVERT(DATE, '1900-01-01')) <> ISNULL(p.acct_open_date, CONVERT(DATE, '1900-01-01'))
    OR ISNULL(t.year_pin_last_changed, -1) <> ISNULL(p.year_pin_last_changed, -1);

-- D: Có trong PDY nhưng không có trong TDY
INSERT INTO bronze.cards_mns (id, operation_flag)
SELECT
    p.id,
    'D' AS operation_flag
FROM bronze.cards_pdy p
LEFT JOIN bronze.cards_tdy t
    ON p.id = t.id
WHERE t.id IS NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.cards_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing cards MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
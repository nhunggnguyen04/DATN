import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.transactions_mns;

-- I: Có trong TDY nhưng không có trong PDY
INSERT INTO bronze.transactions_mns (id, operation_flag)
SELECT
    t.id,
    'I' AS operation_flag
FROM bronze.transactions_tdy t
LEFT JOIN bronze.transactions_pdy p
    ON t.id = p.id
WHERE p.id IS NULL;

-- U: Có trong cả TDY và PDY nhưng dữ liệu thay đổi
INSERT INTO bronze.transactions_mns (id, operation_flag)
SELECT
    t.id,
    'U' AS operation_flag
FROM bronze.transactions_tdy t
INNER JOIN bronze.transactions_pdy p
    ON t.id = p.id
WHERE
    ISNULL(t.[date], CONVERT(DATETIME, '1900-01-01')) <> ISNULL(p.[date], CONVERT(DATETIME, '1900-01-01'))
    OR ISNULL(t.client_id, -1) <> ISNULL(p.client_id, -1)
    OR ISNULL(t.card_id, -1) <> ISNULL(p.card_id, -1)
    OR ISNULL(t.amount, -1) <> ISNULL(p.amount, -1)
    OR ISNULL(t.use_chip, '') <> ISNULL(p.use_chip, '')
    OR ISNULL(t.merchant_id, -1) <> ISNULL(p.merchant_id, -1)
    OR ISNULL(t.merchant_city, '') <> ISNULL(p.merchant_city, '')
    OR ISNULL(t.merchant_state, '') <> ISNULL(p.merchant_state, '')
    OR ISNULL(t.zip, '') <> ISNULL(p.zip, '')
    OR ISNULL(t.mcc, -1) <> ISNULL(p.mcc, -1)
    OR ISNULL(t.errors, '') <> ISNULL(p.errors, '');

-- D: Có trong PDY nhưng không có trong TDY
INSERT INTO bronze.transactions_mns (id, operation_flag)
SELECT
    p.id,
    'D' AS operation_flag
FROM bronze.transactions_pdy p
LEFT JOIN bronze.transactions_tdy t
    ON p.id = t.id
WHERE t.id IS NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.transactions_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing transactions MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.users_mns;

-- I: Có trong TDY nhưng không có trong PDY
INSERT INTO bronze.users_mns (id, operation_flag)
SELECT
    t.id,
    'I' AS operation_flag
FROM bronze.users_tdy t
LEFT JOIN bronze.users_pdy p
    ON t.id = p.id
WHERE p.id IS NULL;

-- U: Có trong cả TDY và PDY nhưng dữ liệu thay đổi
INSERT INTO bronze.users_mns (id, operation_flag)
SELECT
    t.id,
    'U' AS operation_flag
FROM bronze.users_tdy t
INNER JOIN bronze.users_pdy p
    ON t.id = p.id
WHERE
    ISNULL(t.current_age, -1) <> ISNULL(p.current_age, -1)
    OR ISNULL(t.retirement_age, -1) <> ISNULL(p.retirement_age, -1)
    OR ISNULL(t.birth_year, -1) <> ISNULL(p.birth_year, -1)
    OR ISNULL(t.birth_month, -1) <> ISNULL(p.birth_month, -1)
    OR ISNULL(t.gender, '') <> ISNULL(p.gender, '')
    OR ISNULL(t.address, '') <> ISNULL(p.address, '')
    OR ISNULL(t.latitude, '') <> ISNULL(p.latitude, '')
    OR ISNULL(t.longitude, '') <> ISNULL(p.longitude, '')
    OR ISNULL(t.per_capita_income, -1) <> ISNULL(p.per_capita_income, -1)
    OR ISNULL(t.yearly_income, -1) <> ISNULL(p.yearly_income, -1)
    OR ISNULL(t.total_debt, -1) <> ISNULL(p.total_debt, -1)
    OR ISNULL(t.credit_score, -1) <> ISNULL(p.credit_score, -1)
    OR ISNULL(t.num_credit_cards, -1) <> ISNULL(p.num_credit_cards, -1);

-- D: Có trong PDY nhưng không có trong TDY
INSERT INTO bronze.users_mns (id, operation_flag)
SELECT
    p.id,
    'D' AS operation_flag
FROM bronze.users_pdy p
LEFT JOIN bronze.users_tdy t
    ON p.id = t.id
WHERE t.id IS NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.users_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing users MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
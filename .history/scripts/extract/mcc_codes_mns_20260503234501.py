import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.mcc_codes_mns;

-- I: Có trong TDY nhưng không có trong PDY
INSERT INTO bronze.mcc_codes_mns (mcc_id, operation_flag)
SELECT
    t.mcc_id,
    'I' AS operation_flag
FROM bronze.mcc_codes_tdy t
LEFT JOIN bronze.mcc_codes_pdy p
    ON t.mcc_id = p.mcc_id
WHERE p.mcc_id IS NULL;

-- U: Có trong cả TDY và PDY nhưng dữ liệu thay đổi
INSERT INTO bronze.mcc_codes_mns (mcc_id, operation_flag)
SELECT
    t.mcc_id,
    'U' AS operation_flag
FROM bronze.mcc_codes_tdy t
INNER JOIN bronze.mcc_codes_pdy p
    ON t.mcc_id = p.mcc_id
WHERE
    ISNULL(t.description, '') <> ISNULL(p.description, '');

-- D: Có trong PDY nhưng không có trong TDY
INSERT INTO bronze.mcc_codes_mns (mcc_id, operation_flag)
SELECT
    p.mcc_id,
    'D' AS operation_flag
FROM bronze.mcc_codes_pdy p
LEFT JOIN bronze.mcc_codes_tdy t
    ON p.mcc_id = t.mcc_id
WHERE t.mcc_id IS NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.mcc_codes_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing mcc_codes MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
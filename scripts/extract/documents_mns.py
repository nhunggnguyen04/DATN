import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.documents_mns;

-- I: In TDY but not in PDY
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    t.document_id,
    'I' AS operation_flag
FROM bronze.documents_tdy t
LEFT JOIN bronze.documents_pdy p
    ON t.document_id = p.document_id
WHERE p.document_id IS NULL;

-- U: In both but changed
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    t.document_id,
    'U' AS operation_flag
FROM bronze.documents_tdy t
INNER JOIN bronze.documents_pdy p
    ON t.document_id = p.document_id
WHERE
    ISNULL(t.entity_type, '') <> ISNULL(p.entity_type, '')
    OR ISNULL(t.entity_id, -1) <> ISNULL(p.entity_id, -1)
    OR ISNULL(t.doc_type, '') <> ISNULL(p.doc_type, '')
    OR ISNULL(t.file_path, '') <> ISNULL(p.file_path, '')
    OR ISNULL(t.file_format, '') <> ISNULL(p.file_format, '')
    OR ISNULL(t.source, '') <> ISNULL(p.source, '')
    OR ISNULL(t.sha256, '') <> ISNULL(p.sha256, '')
    OR ISNULL(t.file_size_bytes, -1) <> ISNULL(p.file_size_bytes, -1)
    OR ISNULL(CONVERT(VARCHAR(19), t.created_at, 120), '') <> ISNULL(CONVERT(VARCHAR(19), p.created_at, 120), '')
    OR ISNULL(CONVERT(VARCHAR(10), t.run_date, 120), '') <> ISNULL(CONVERT(VARCHAR(10), p.run_date, 120), '')
    OR ISNULL(t.ocr_text, '') <> ISNULL(p.ocr_text, '');

-- D: In PDY but not in TDY
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    p.document_id,
    'D' AS operation_flag
FROM bronze.documents_pdy p
LEFT JOIN bronze.documents_tdy t
    ON p.document_id = t.document_id
WHERE t.document_id IS NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.documents_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing documents MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()

"""
Compute MNS (Move-New-Stable) cho bảng documents.

Logic:
- I (Insert): Document mới xuất hiện trong TDY (không có trong PDY)
- U (Update): Document tồn tại nhưng file_path hoặc sha256 thay đổi
- D (Delete): Document có trong PDY nhưng không còn trong TDY

LƯU Ý: Script này chạy TRƯỚC khi load dữ liệu mới vào TDY,
để compute MNS dựa trên dữ liệu hiện tại (TDY = run trước, PDY = older).

Sau khi chạy OCR và load mới vào TDY, chạy lại để compute MNS thực sự.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine

# Compute MNS: So sánh TDY (dữ liệu mới) với PDY (dữ liệu cũ)
COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.documents_mns;

-- I: New documents (in TDY but not in PDY)
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    t.document_id,
    'I' AS operation_flag
FROM bronze.documents_tdy t
LEFT JOIN bronze.documents_pdy p
    ON t.document_id = p.document_id
WHERE p.document_id IS NULL;

-- U: Documents where metadata changed (sha256 or file_path differs)
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    t.document_id,
    'U' AS operation_flag
FROM bronze.documents_tdy t
INNER JOIN bronze.documents_pdy p
    ON t.document_id = p.document_id
WHERE
    COALESCE(t.sha256, '') <> COALESCE(p.sha256, '')
    OR COALESCE(t.file_path, '') <> COALESCE(p.file_path, '');

-- D: Deleted documents (in PDY but not in TDY)
INSERT INTO bronze.documents_mns (document_id, operation_flag)
SELECT
    p.document_id,
    'D' AS operation_flag
FROM bronze.documents_pdy p
LEFT JOIN bronze.documents_tdy t
    ON p.document_id = t.document_id
WHERE t.document_id IS NULL;
"""

# Check MNS results
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

    print("=" * 60)
    print("DOCUMENTS MNS COMPUTATION")
    print("=" * 60)

    # Check if there's data in TDY
    check_tdy_sql = "SELECT COUNT(*) AS cnt FROM bronze.documents_tdy"
    with target_engine.begin() as conn:
        result = conn.exec_driver_sql(check_tdy_sql).fetchone()
        tdy_count = result[0] if result else 0

    print(f"\nDocuments in TDY: {tdy_count}")

    if tdy_count == 0:
        print("Warning: No data in bronze.documents_tdy")
        print("Run 'load_bronze_documents.py' first to load new manifest.")
        return

    with target_engine.begin() as conn:
        # Compute MNS
        print("\nComputing MNS (TDY vs PDY)...")
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        print("MNS computation complete.")

        # Show results
        print("\n" + "=" * 60)
        print("MNS RESULTS")
        print("=" * 60)

        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

        if not rows:
            print("No changes detected.")
        else:
            for row in rows:
                flag, count = row
                desc = {"I": "Insert", "U": "Update", "D": "Delete"}.get(flag, flag)
                print(f"  {desc} ({flag}): {count} records")

    print("\n" + "=" * 60)
    print("Next steps:")
    print("  1. Run OCR extraction (if needed)")
    print("  2. Run Silver dbt models (reads from MNS)")
    print("  3. After successful Silver/Gold, run move_tdy_to_pdy.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
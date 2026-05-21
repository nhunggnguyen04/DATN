"""
Compute MNS (Move-New-Stable) cho bảng ocr_results.

Logic:
- I (Insert): Document mới xuất hiện trong TDY (không có trong PDY)
- U (Update): Document tồn tại nhưng ocr_text_hash thay đổi (content khác)
- D (Delete): Document có trong PDY nhưng không còn trong TDY

LƯU Ý: Script này chạy SAU khi load mới OCR results vào TDY.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine

COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.ocr_results_mns;

-- I: New content (in TDY but not in PDY)
INSERT INTO bronze.ocr_results_mns (document_id, operation_flag, change_type)
SELECT
    t.document_id,
    'I' AS operation_flag,
    'new_content' AS change_type
FROM bronze.ocr_results_tdy t
LEFT JOIN bronze.ocr_results_pdy p
    ON t.document_id = p.document_id
WHERE p.document_id IS NULL;

-- U: Content changed (ocr_text_hash differs)
INSERT INTO bronze.ocr_results_mns (document_id, operation_flag, change_type)
SELECT
    t.document_id,
    'U' AS operation_flag,
    'content_changed' AS change_type
FROM bronze.ocr_results_tdy t
INNER JOIN bronze.ocr_results_pdy p
    ON t.document_id = p.document_id
WHERE
    COALESCE(t.ocr_text_hash, '') <> COALESCE(p.ocr_text_hash, '');

-- D: Deleted content (in PDY but not in TDY)
INSERT INTO bronze.ocr_results_mns (document_id, operation_flag, change_type)
SELECT
    p.document_id,
    'D' AS operation_flag,
    'deleted_content' AS change_type
FROM bronze.ocr_results_pdy p
LEFT JOIN bronze.ocr_results_tdy t
    ON p.document_id = t.document_id
WHERE t.document_id IS NULL;
"""

# Move TDY to PDY (chạy trước khi load dữ liệu mới)
MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.ocr_results_pdy;

INSERT INTO bronze.ocr_results_pdy (
    document_id,
    run_date,
    ocr_engine,
    ocr_lang,
    ocr_avg_score,
    ocr_text,
    ocr_text_hash,
    full_name,
    demo_id_no,
    date_of_birth,
    sex,
    nationality,
    place_of_origin,
    place_of_residence,
    issue_date,
    expiry_date,
    extraction_confidence,
    status,
    error,
    processed_at
)
SELECT
    document_id,
    run_date,
    ocr_engine,
    ocr_lang,
    ocr_avg_score,
    ocr_text,
    ocr_text_hash,
    full_name,
    demo_id_no,
    date_of_birth,
    sex,
    nationality,
    place_of_origin,
    place_of_residence,
    issue_date,
    expiry_date,
    extraction_confidence,
    status,
    error,
    processed_at
FROM bronze.ocr_results_tdy;

TRUNCATE TABLE bronze.ocr_results_tdy;
"""

CHECK_MNS_SQL = """
SELECT
    operation_flag,
    change_type,
    COUNT(*) AS total_records
FROM bronze.ocr_results_mns
GROUP BY operation_flag, change_type
ORDER BY operation_flag, change_type;
"""


def main():
    target_engine = get_target_engine()

    print("=" * 60)
    print("OCR_RESULTS MNS COMPUTATION")
    print("=" * 60)

    # Check if there's data in TDY
    check_tdy_sql = "SELECT COUNT(*) AS cnt FROM bronze.ocr_results_tdy"
    with target_engine.begin() as conn:
        result = conn.exec_driver_sql(check_tdy_sql).fetchone()
        tdy_count = result[0] if result else 0

    if tdy_count == 0:
        print("\nWarning: No data in bronze.ocr_results_tdy")
        print("Run 'load_bronze_ocr_results.py' first to load new OCR results.")
        print("\nProceeding to move TDY to PDY anyway...")

    # CORRECT FLOW:
    # This script is called AFTER loading new OCR data into TDY.
    # We assume:
    # - PDY already has previous day's OCR data (from earlier archival)
    # - TDY has new OCR data (just loaded)
    # - We ONLY compute MNS (compare TDY vs PDY)
    #
    # DO NOT move TDY to PDY here - that should happen at the END of the pipeline
    # AFTER dbt transforms succeed.

    print("\n" + "=" * 60)
    print("Re-computing MNS (TDY vs PDY)...")

    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        print("MNS computation complete.")

        # Step 3: Show results
        print("\n" + "=" * 60)
        print("MNS RESULTS")
        print("=" * 60)

        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

        if not rows:
            print("No changes detected.")
        else:
            for row in rows:
                print(f"  {row[0]} ({row[1]}): {row[2]} records")

    print("\n" + "=" * 60)
    print("Next steps:")
    print("  1. Run Silver dbt models (reads from MNS)")
    print("  2. After successful Silver/Gold, run move to PDY:")
    print("     DELETE FROM bronze.ocr_results_pdy;")
    print("     INSERT INTO bronze.ocr_results_pdy SELECT * FROM bronze.ocr_results_tdy;")
    print("     TRUNCATE TABLE bronze.ocr_results_tdy;")
    print("=" * 60)


if __name__ == "__main__":
    main()
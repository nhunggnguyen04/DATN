"""
Move TDY to PDY cho cả documents và ocr_results tables.

Chạy script này sau khi:
1. Đã compute MNS thành công
2. Đã run Silver/Gold dbt models thành công

Điều này đảm bảo dữ liệu được archival đúng cách sau khi xử lý hoàn tất.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine

# Move documents TDY to PDY (chỉ chạy nếu bảng tồn tại)
MOVE_DOCUMENTS_TDY_TO_PDY = """
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze' AND t.name = 'documents_pdy')
BEGIN
    TRUNCATE TABLE bronze.documents_pdy;

    INSERT INTO bronze.documents_pdy (
        document_id, entity_type, entity_id, doc_type, file_path,
        file_format, created_at, source, sha256, file_size_bytes, ocr_text, run_date
    )
    SELECT
        document_id, entity_type, entity_id, doc_type, file_path,
        file_format, created_at, source, sha256, file_size_bytes, ocr_text, run_date
    FROM bronze.documents_tdy;

    TRUNCATE TABLE bronze.documents_tdy;
END
"""

MOVE_OCR_RESULTS_TDY_TO_PDY = """
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze' AND t.name = 'ocr_results_pdy')
BEGIN
    TRUNCATE TABLE bronze.ocr_results_pdy;

    INSERT INTO bronze.ocr_results_pdy (
        document_id, run_date, ocr_engine, ocr_lang, ocr_avg_score,
        ocr_text, ocr_text_hash, full_name, demo_id_no, date_of_birth,
        sex, nationality, place_of_origin, place_of_residence,
        issue_date, expiry_date, extraction_confidence, status, error, processed_at
    )
    SELECT
        document_id, run_date, ocr_engine, ocr_lang, ocr_avg_score,
        ocr_text, ocr_text_hash, full_name, demo_id_no, date_of_birth,
        sex, nationality, place_of_origin, place_of_residence,
        issue_date, expiry_date, extraction_confidence, status, error, processed_at
    FROM bronze.ocr_results_tdy;

    TRUNCATE TABLE bronze.ocr_results_tdy;
END
"""

TRUNCATE_MNS = """
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze' AND t.name = 'documents_mns')
    TRUNCATE TABLE bronze.documents_mns;

IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze' AND t.name = 'ocr_results_mns')
    TRUNCATE TABLE bronze.ocr_results_mns;
"""


def main():
    target_engine = get_target_engine()

    print("=" * 60)
    print("MOVE TDY TO PDY (Archival)")
    print("=" * 60)

    with target_engine.begin() as conn:
        # Move documents
        print("\n[1] Moving documents_tdy -> documents_pdy...")
        conn.exec_driver_sql(MOVE_DOCUMENTS_TDY_TO_PDY)
        print("    Done.")

        # Move ocr_results
        print("\n[2] Moving ocr_results_tdy -> ocr_results_pdy...")
        conn.exec_driver_sql(MOVE_OCR_RESULTS_TDY_TO_PDY)
        print("    Done.")

        # Clear MNS
        print("\n[3] Clearing MNS tables...")
        conn.exec_driver_sql(TRUNCATE_MNS)
        print("    Done.")

    print("\n" + "=" * 60)
    print("Archival complete. Pipeline ready for next run.")
    print("=" * 60)


if __name__ == "__main__":
    main()
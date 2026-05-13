import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import pandas as pd

from scripts.utils.db_connection import get_target_engine


MOVE_TDY_TO_PDY_SQL = """
TRUNCATE TABLE bronze.documents_pdy;

INSERT INTO bronze.documents_pdy (
    document_id,
    entity_type,
    entity_id,
    doc_type,
    file_path,
    file_format,
    created_at,
    source,
    sha256,
    file_size_bytes,
    ocr_text,
    run_date
)
SELECT
    document_id,
    entity_type,
    entity_id,
    doc_type,
    file_path,
    file_format,
    created_at,
    source,
    sha256,
    file_size_bytes,
    ocr_text,
    run_date
FROM bronze.documents_tdy;

TRUNCATE TABLE bronze.documents_tdy;
"""


def _find_latest_manifest(manifest_dir: Path) -> Path:
    candidates = sorted(manifest_dir.glob("documents_*.csv"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No manifest files found in {manifest_dir}")
    return candidates[0]


def main():
    manifest_dir = PROJECT_ROOT / "output" / "manifests"
    manifest_path = _find_latest_manifest(manifest_dir)

    if len(sys.argv) > 1:
        manifest_path = Path(sys.argv[1])

    print(f"Reading manifest: {manifest_path.as_posix()}")
    df = pd.read_csv(manifest_path)

    # Normalize types
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce").dt.tz_convert(None)
    if "run_date" in df.columns:
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.date

    target_engine = get_target_engine()

    print("Moving current documents TDY to PDY and clearing TDY...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(MOVE_TDY_TO_PDY_SQL)

    print("Loading new manifest data into bronze.documents_tdy...")
    df.to_sql(
        name="documents_tdy",
        con=target_engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=1000,
    )

    print("Loaded documents into bronze.documents_tdy successfully.")


if __name__ == "__main__":
    main()

import argparse
import hashlib
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from scripts.utils.db_connection import get_target_engine


def compute_sha256_hash(text: str) -> str:
    """Compute SHA256 hash of text for change detection."""
    if not text or pd.isna(text):
        return ""
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Load OCR extraction results into bronze.ocr_results_tdy"
    )
    p.add_argument(
        "--input",
        required=True,
        help="Path to OCR extraction CSV file (e.g., id_card_extractions_YYYY-MM-DD.csv)",
    )
    p.add_argument(
        "--run-date",
        default="",
        help="Override run_date (default: extracted from filename or 'unknown')",
    )
    args = p.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Extraction file not found: {input_path}")

    # Determine run_date from filename if not provided
    run_date_override = args.run_date or None
    if not run_date_override:
        # Try to extract from filename: id_card_extractions_2026-05-13.csv
        import re

        match = re.search(r"(\d{4}-\d{2}-\d{2})", input_path.name)
        if match:
            run_date_override = match.group(1)

    print(f"Reading extraction file: {input_path.as_posix()}")
    df = pd.read_csv(input_path)

    # Compute ocr_text_hash for change detection
    print("Computing OCR text hashes...")
    df["ocr_text_hash"] = df["ocr_text"].apply(compute_sha256_hash)

    # Normalize data types to match SQL Server schema
    # - run_date: DATE
    # - processed_at: DATETIME
    # - ocr_avg_score/extraction_confidence: DECIMAL(5,4)
    # - date_of_birth/issue_date/expiry_date: DATE (often emitted as dd/mm/yyyy)
    def normalize_run_date(val):
        if pd.isna(val) or val == "":
            return None
        if isinstance(val, date):
            return val
        try:
            return pd.to_datetime(val).date()
        except Exception:
            return None

    if "run_date" in df.columns:
        df["run_date"] = df["run_date"].apply(normalize_run_date)

    # Coerce decimal-like columns
    for col in ["ocr_avg_score", "extraction_confidence"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Coerce date fields that the OCR extractor outputs as dd/mm/yyyy
    for col in ["date_of_birth", "issue_date", "expiry_date"]:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            # store as Python date to bind cleanly into SQL DATE
            df[col] = parsed.dt.date

    # Coerce processed_at (ISO8601 with 'Z') -> naive datetime
    if "processed_at" in df.columns:
        parsed = pd.to_datetime(df["processed_at"], utc=True, errors="coerce")
        df["processed_at"] = parsed.dt.tz_convert(None)

    # Override run_date if provided
    if run_date_override:
        df["run_date"] = pd.to_datetime(run_date_override).date()

    # Ensure status column exists
    if "status" not in df.columns:
        df["status"] = df["error"].apply(lambda x: "error" if pd.notna(x) and str(x).strip() != "" else "ok")

    target_engine = get_target_engine()

    # Expected columns for ocr_results_tdy
    expected_columns = [
        "document_id",
        "run_date",
        "ocr_engine",
        "ocr_lang",
        "ocr_avg_score",
        "ocr_text",
        "ocr_text_hash",
        "full_name",
        "demo_id_no",
        "date_of_birth",
        "sex",
        "nationality",
        "place_of_origin",
        "place_of_residence",
        "issue_date",
        "expiry_date",
        "extraction_confidence",
        "status",
        "error",
        "processed_at",
    ]

    # Select only expected columns that exist in the dataframe
    available_columns = [col for col in expected_columns if col in df.columns]
    df_load = df[available_columns].copy()

    print(f"Loading {len(df_load)} records into bronze.ocr_results_tdy...")

    with target_engine.begin() as conn:
        df_load.to_sql(
            name="ocr_results_tdy",
            con=target_engine,
            schema="bronze",
            if_exists="append",
            index=False,
            chunksize=500,
        )

    # Print summary
    ok_count = len(df_load[df_load["status"] == "ok"])
    error_count = len(df_load[df_load["status"] == "error"])

    print(f"Loaded successfully: {ok_count}")
    print(f"Errors: {error_count}")
    print("Done.")


if __name__ == "__main__":
    main()
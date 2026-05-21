"""
Simple OCR Pipeline - Không có Airflow, không có dbt, không có MNS.

Chỉ xử lý:
1. OCR extraction cho CCCD và/hoặc Savings Book
2. Load kết quả vào bronze tables

Usage:
    python run_simple_pipeline.py --run-date 2026-05-22 --doc-type id_card
    python run_simple_pipeline.py --run-date 2026-05-22 --doc-type savings_book
    python run_simple_pipeline.py --run-date 2026-05-22 --doc-type both
"""
import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_command(cmd: list[str], description: str, use_ocr_venv: bool = False) -> bool:
    """Run a command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"[{description}]")
    print(f"{'=' * 60}")

    if use_ocr_venv:
        venv_ocr_python = PROJECT_ROOT / ".venv_ocr" / "Scripts" / "python.exe"
        if venv_ocr_python.exists():
            cmd[0] = str(venv_ocr_python)
            print(f"Using OCR venv: {venv_ocr_python}")
        else:
            print(f"WARNING: .venv_ocr not found at {venv_ocr_python}")
            print("Make sure to create .venv_ocr and install requirements-ocr.txt")

    print(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=False)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description='Simple OCR pipeline (no Airflow, no dbt)')
    parser.add_argument("--run-date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--doc-type", choices=['id_card', 'savings_book', 'both'], default='both',
                        help="Document type to process")
    parser.add_argument("--skip-load", action='store_true', help="Skip loading to database (only extract)")
    parser.add_argument("--input-dir", default="data/unstructured/documents",
                        help="Base directory containing documents")
    args = parser.parse_args()

    run_date = args.run_date
    doc_types = []
    if args.doc_type == 'both':
        doc_types = ['id_card', 'savings_book']
    else:
        doc_types = [args.doc_type]

    print("=" * 60)
    print("SIMPLE OCR PIPELINE")
    print(f"Run date: {run_date}")
    print(f"Doc types: {', '.join(doc_types)}")
    print("=" * 60)

    success = True

    for doc_type in doc_types:
        print(f"\n{'#'*60}")
        print(f"Processing: {doc_type}")
        print(f"{'#'*60}")

        # Define paths
        input_dir = PROJECT_ROOT / args.input_dir / f"doc_type={doc_type}" / f"run_date={run_date}"
        output_csv = PROJECT_ROOT / "data" / "unstructured" / "extracted" / f"{doc_type}_extractions_{run_date}.csv"

        # Step 1: OCR Extraction
        extraction_script = f"scripts/extract/ocr_extract_{doc_type}.py"
        if not run_command(
            [sys.executable, extraction_script,
             "--input-dir", str(input_dir),
             "--run-date", run_date,
             "--out", str(output_csv)],
            f"OCR Extraction: {doc_type}",
            use_ocr_venv=True
        ):
            print(f"ERROR: OCR extraction failed for {doc_type}")
            success = False
            continue

        # Step 2: Load to Bronze (optional)
        if not args.skip_load:
            if not run_command(
                [sys.executable, "scripts/extract/load_bronze_simple.py",
                 "--csv", str(output_csv),
                 "--doc-type", doc_type],
                f"Load to Bronze: {doc_type}"
            ):
                print(f"ERROR: Load to bronze failed for {doc_type}")
                success = False
                continue

    if success:
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        return 0
    else:
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED WITH ERRORS")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())

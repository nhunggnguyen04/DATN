"""
Run full pipeline cho unstructured data (Hybrid approach).

Các bước:
1. Move TDY -> PDY (archival từ run trước)
2. Build manifest từ documents folder
3. Load metadata vào documents_tdy
4. Chạy OCR extraction
5. Load OCR results vào ocr_results_tdy
6. Compute MNS cho cả documents và ocr_results
7. (Optional) Run dbt Silver/Gold
8. Move TDY -> PDY (archival)
"""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"[{description}]")
    print(f"{'=' * 60}")
    print(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=False)
    return result.returncode == 0


def main():
    p = argparse.ArgumentParser(description="Run full unstructured data pipeline")
    p.add_argument("--run-date", required=True, help="Run date (YYYY-MM-DD)")
    p.add_argument("--doc-type", default="id_card", choices=["id_card", "savings_book"], help="Document type")
    p.add_argument("--skip-db-update", action="store_true", help="Skip database updates (manifest build only)")
    p.add_argument("--skip-ocr", action="store_true", help="Skip OCR extraction")
    p.add_argument("--skip-dbt", action="store_true", help="Skip dbt transform")
    args = p.parse_args()

    run_date = args.run_date
    doc_type = args.doc_type

    print("=" * 60)
    print("UNSTRUCTURED DATA PIPELINE (Hybrid)")
    print(f"Run date: {run_date}")
    print(f"Doc type: {doc_type}")
    print("=" * 60)

    # Step 1: Move TDY to PDY (cleanup from previous run)
    if not args.skip_db_update:
        if not run_command(
            [sys.executable, "scripts/extract/move_tdy_to_pdy.py"],
            "Step 1: Archive previous run (TDY -> PDY)"
        ):
            print("Warning: Previous archival failed, continuing anyway...")

    # Step 2: Build manifest
    manifest_dir = PROJECT_ROOT / "data" / "unstructured" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifest_dir / f"documents_{run_date}_{doc_type}.csv"

    if not run_command(
        [
            sys.executable, "scripts/build_unstructured_manifest_from_files.py",
            "--run-date", run_date,
            "--doc-type", doc_type,
            "--unstructured-root", "data/unstructured",
            "--out", str(manifest_path)
        ],
        f"Step 2: Build manifest for {doc_type}"
    ):
        print("ERROR: Manifest build failed. Stopping.")
        return 1

    # Step 3: Load metadata into documents_tdy
    if not args.skip_db_update:
        if not run_command(
            [sys.executable, "scripts/extract/load_bronze_documents.py", str(manifest_path)],
            "Step 3: Load metadata into documents_tdy"
        ):
            print("ERROR: Load documents failed. Stopping.")
            return 1

    # Step 4: Run OCR extraction
    if not args.skip_ocr:
        extraction_path = PROJECT_ROOT / "data" / "unstructured" / "extracted" / f"{doc_type}_extractions_{run_date}.csv"

        if not run_command(
            [
                sys.executable, "scripts/extract/ocr_extract_id_card.py",
                "--manifest", str(manifest_path),
                "--run-date", run_date,
                "--out", str(extraction_path)
            ],
            f"Step 4: OCR extraction for {doc_type}"
        ):
            print("WARNING: OCR extraction failed. Continuing with partial data...")

        # Step 5: Load OCR results
        if extraction_path.exists():
            if not run_command(
                [
                    sys.executable, "scripts/extract/load_bronze_ocr_results.py",
                    "--input", str(extraction_path)
                ],
                "Step 5: Load OCR results into ocr_results_tdy"
            ):
                print("ERROR: Load OCR results failed. Stopping.")
                return 1

    # Step 6: Compute MNS
    if not args.skip_db_update:
        if not run_command(
            [sys.executable, "scripts/extract/documents_mns.py"],
            "Step 6a: Compute documents MNS"
        ):
            print("WARNING: Documents MNS computation failed.")

        if not args.skip_ocr:
            if not run_command(
                [sys.executable, "scripts/extract/ocr_results_mns.py"],
                "Step 6b: Compute ocr_results MNS"
            ):
                print("WARNING: OCR results MNS computation failed.")

    # Step 7: Run dbt (optional)
    if not args.skip_dbt:
        if not run_command(
            [sys.executable, "-m", "dbt", "run"],
            "Step 7: Run dbt transform"
        ):
            print("WARNING: dbt transform failed.")

    # Step 8: Move TDY to PDY (archive)
    if not args.skip_db_update:
        if not run_command(
            [sys.executable, "scripts/extract/move_tdy_to_pdy.py"],
            "Step 8: Archive to PDY (final)"
        ):
            print("ERROR: Final archival failed.")
            return 1

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
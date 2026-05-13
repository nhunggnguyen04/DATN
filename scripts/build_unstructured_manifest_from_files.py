import argparse
import csv
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path


FIELDNAMES = [
    "document_id",
    "entity_type",
    "entity_id",
    "doc_type",
    "file_path",
    "file_format",
    "created_at",
    "source",
    "sha256",
    "file_size_bytes",
    "ocr_text",
    "run_date",
]


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _stable_doc_uuid(entity_type: str, entity_id: int, doc_type: str) -> str:
    name = f"{entity_type}:{entity_id}:{doc_type}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _iso_z_from_mtime(path: Path) -> str:
    ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z")


def build_rows(unstructured_root: Path, run_date: str, doc_type: str) -> list[dict]:
    run_dir = (
        unstructured_root
        / "documents"
        / f"doc_type={doc_type}"
        / f"run_date={run_date}"
    )
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    expected_name = {
        "id_card": "id_card_scan.jpg",
        "savings_book": "savings_book_scan.jpg",
    }.get(doc_type)
    if expected_name is None:
        raise ValueError("doc_type must be one of: id_card, savings_book")

    rows: list[dict] = []
    for user_dir in sorted(run_dir.glob("user_id=*")):
        if not user_dir.is_dir():
            continue
        try:
            entity_id = int(user_dir.name.split("=")[1])
        except Exception:
            continue

        file_path = user_dir / expected_name
        if not file_path.exists():
            continue

        file_format = file_path.suffix.lstrip(".").lower()
        rows.append(
            {
                "document_id": _stable_doc_uuid("user", entity_id, doc_type),
                "entity_type": "user",
                "entity_id": entity_id,
                "doc_type": doc_type,
                "file_path": file_path.as_posix(),
                "file_format": file_format,
                "created_at": _iso_z_from_mtime(file_path),
                "source": "ai_generated",
                "sha256": _sha256_file(file_path),
                "file_size_bytes": file_path.stat().st_size,
                "ocr_text": None,
                "run_date": run_date,
            }
        )

    if not rows:
        raise RuntimeError(f"No files found for doc_type={doc_type} under {run_dir}")

    return rows


def write_manifest_csv(out_path: Path, rows: list[dict]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in FIELDNAMES})


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--doc-type", required=True, choices=["id_card", "savings_book"]) 
    p.add_argument("--unstructured-root", default="output/unstructured")
    p.add_argument("--out", required=True, help="Output CSV path")
    args = p.parse_args()

    rows = build_rows(Path(args.unstructured_root), args.run_date, args.doc_type)
    write_manifest_csv(Path(args.out), rows)
    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()

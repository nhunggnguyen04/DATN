"""
Setup audit infrastructure: tạo schema audit + bảng pipeline_run_log + views.
Chạy 1 lần khi deploy lần đầu.

Usage:
    python scripts/setup_audit.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


def main():
    sql_file = PROJECT_ROOT / "sql" / "create_audit_tables.sql"
    if not sql_file.exists():
        print(f"ERROR: {sql_file} not found")
        sys.exit(1)

    sql = sql_file.read_text(encoding="utf-8")
    # SQL Server: split by 'GO' (case-insensitive, line-only)
    batches = []
    current = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            if current:
                batches.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        batches.append("\n".join(current))

    engine = get_target_engine()
    print(f"Running {len(batches)} batches from {sql_file.name}...")
    with engine.begin() as conn:
        for i, batch in enumerate(batches, 1):
            stripped = batch.strip()
            if not stripped:
                continue
            print(f"  Batch {i}/{len(batches)}...")
            conn.exec_driver_sql(stripped)
    print("Done. audit.pipeline_run_log + views created.")


if __name__ == "__main__":
    main()

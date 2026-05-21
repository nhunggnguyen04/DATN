"""
Load OCR extraction results vào bronze tables (simple version).

2 bảng:
- bronze.id_card_results
- bronze.savings_book_results
"""
import argparse
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


def load_csv_to_bronze(csv_path: Path, doc_type: str, clear_existing: bool = True):
    """Load CSV vào bảng bronze tương ứng"""
    df = pd.read_csv(csv_path)

    # Determine table name
    if doc_type == 'id_card':
        table_name = 'id_card_results'
    elif doc_type == 'savings_book':
        table_name = 'savings_book_results'
    else:
        raise ValueError(f"Unsupported doc_type: {doc_type}")

    engine = get_target_engine()
    schema = 'bronze'
    full_table = f"{schema}.{table_name}"

    print(f"Loading {len(df)} records to {full_table}")

    # Optional: Clear existing data for this run_date
    if clear_existing and 'run_date' in df.columns:
        run_date = df['run_date'].iloc[0] if len(df) > 0 else None
        if run_date:
            with engine.begin() as conn:
                result = conn.execute(
                    text(f"DELETE FROM {full_table} WHERE run_date = :run_date"),
                    {"run_date": run_date}
                )
                print(f"  Cleared {result.rowcount} existing records for run_date={run_date}")

    # Load to database
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=1000
    )

    # Verify
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) as total, status FROM {full_table} WHERE run_date = :run_date GROUP BY status"),
            {"run_date": df['run_date'].iloc[0] if 'run_date' in df.columns else None}
        )
        counts = result.fetchall()

        if counts:
            print(f"  Total records for this run_date:")
            for row in counts:
                status, cnt = row
                print(f"    {status}: {cnt}")
        else:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table}"))
            total = result.scalar()
            print(f"  Total records in table: {total}")

    return len(df)


def main():
    parser = argparse.ArgumentParser(description='Load OCR extraction results to bronze tables')
    parser.add_argument('--csv', required=True, help='CSV file path')
    parser.add_argument('--doc-type', required=True, choices=['id_card', 'savings_book'])
    parser.add_argument('--no-clear', action='store_true', help='Do not clear existing data for this run_date')
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return 1

    print(f"Loading {csv_path} to bronze.{args.doc_type}_results")
    try:
        count = load_csv_to_bronze(csv_path, args.doc_type, clear_existing=not args.no_clear)
        print(f"\n✓ Successfully loaded {count} records")
        return 0
    except Exception as e:
        print(f"\n✗ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine
import pandas as pd

engine = get_target_engine()

print('=== BRONZE TABLES ===')
for table in ['documents_tdy', 'documents_pdy', 'documents_mns', 'ocr_results_tdy', 'ocr_results_pdy', 'ocr_results_mns']:
    try:
        df = pd.read_sql(f'SELECT COUNT(*) as cnt FROM bronze.{table}', engine)
        print(f'{table}: {df.iloc[0,0]} rows')
    except Exception as e:
        print(f'{table}: ERROR - {e}')

print('\n=== SILVER TABLES ===')
for table in ['silver_id_card_documents', 'silver_savings_book_documents']:
    try:
        df = pd.read_sql(f'SELECT COUNT(*) as cnt FROM silver.{table}', engine)
        print(f'{table}: {df.iloc[0,0]} rows')
    except Exception as e:
        print(f'{table}: ERROR - {e}')

print('\n=== GOLD TABLES ===')
for table in ['gold_id_card_daily_stats', 'gold_savings_book_daily_stats']:
    try:
        df = pd.read_sql(f'SELECT COUNT(*) as cnt FROM gold.{table}', engine)
        print(f'{table}: {df.iloc[0,0]} rows')
    except Exception as e:
        print(f'{table}: ERROR - {e}')

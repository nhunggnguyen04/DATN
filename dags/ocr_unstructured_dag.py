"""
ocr_unstructured_dag — OCR CCCD + Sổ tiết kiệm → Bronze.
Trigger: manual hoặc FileSensor watch folder ảnh mới.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

sys.path.append(str(Path(__file__).resolve().parent))

from common.constants import (
    DEFAULT_ARGS, SCRIPT_ENV, SCRIPTS_DIR, DATA_DIR, PROJECT_ROOT,
    VENV_OCR, POOL_OCR, VAR_OCR_CONF_THRESHOLD,
)
from common.callbacks import notify_success
from common.operators import AuditedBashOperator, AuditedPythonOperator


DAG_ID = "ocr_unstructured_dag"


def validate_ocr_quality(audit_row_id: int = None, **ctx):
    """
    Kiểm tra chất lượng OCR sau khi load.
    Warn (không fail) nếu low_confidence ratio > threshold.
    """
    from airflow.models import Variable
    from sqlalchemy import text
    from scripts.utils.db_connection import get_target_engine

    threshold = float(Variable.get(VAR_OCR_CONF_THRESHOLD, default_var="0.5"))
    run_date = ctx["ds"]

    engine = get_target_engine()
    results = {}
    for table in ["id_card_results", "savings_book_results"]:
        with engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN final_confidence < :th THEN 1 ELSE 0 END) AS low_conf,
                    AVG(CAST(final_confidence AS FLOAT)) AS avg_conf
                FROM bronze.{table}
                WHERE run_date = :rd
            """), {"th": threshold, "rd": run_date}).fetchone()
            total = int(row[0] or 0)
            low = int(row[1] or 0)
            avg = float(row[2] or 0)
            results[table] = {"total": total, "low_conf": low, "avg_conf": avg}
            print(f"[ocr_quality] {table}: total={total}, low_conf={low}, avg={avg:.3f}")
            if total > 0 and low / total > 0.30:
                print(f"  ⚠ WARN: {low}/{total} = {low/total:.1%} < {threshold}")
    return results


with DAG(
    dag_id=DAG_ID,
    description="OCR pipeline: CCCD + Sổ tiết kiệm → Bronze",
    schedule_interval=None,         # manual trigger
    start_date=datetime(2026, 5, 25),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["banking", "ocr", "unstructured"],
) as dag:

    # FileSystemSensor (opt-in): bật khi muốn tự động trigger
    # wait_for_images = FileSystemSensor(
    #     task_id="wait_for_images",
    #     filepath=f"{DATA_DIR}/unstructured/documents/doc_type=id_card/run_date={{{{ ds }}}}",
    #     poke_interval=60,
    #     timeout=60 * 60 * 2,
    #     mode="reschedule",
    # )

    # -------------------------------------------------------------------------
    # OCR extraction (chạy trong .venv_ocr riêng vì PaddleOCR)
    # -------------------------------------------------------------------------
    ocr_id_card = AuditedBashOperator(
        task_id="ocr_extract_id_card",
        bash_command=(
            f"source {VENV_OCR} && "
            f"cd {PROJECT_ROOT} && "
            f"python {SCRIPTS_DIR}/extract/ocr_extract_id_card.py "
            f"--input-dir {DATA_DIR}/unstructured/documents/doc_type=id_card/run_date=2026-05-29 "
            f"--run-date {{{{ ds }}}}"
        ),
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_OCR,
        execution_timeout=timedelta(hours=1),
    )

    ocr_savings_book = AuditedBashOperator(
        task_id="ocr_extract_savings_book",
        bash_command=(
            f"source {VENV_OCR} && "
            f"cd {PROJECT_ROOT} && "
            f"python {SCRIPTS_DIR}/extract/ocr_extract_savings_book.py "
            f"--input-dir {DATA_DIR}/unstructured/documents/doc_type=savings_book/run_date=2026-05-29 "
            f"--run-date {{{{ ds }}}} "
            f"--limit 10"
        ),
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_OCR,
        execution_timeout=timedelta(hours=1),
    )

    # -------------------------------------------------------------------------
    # Load extracted CSV/XLSX → bronze.* (chạy trong .venv chính)
    # -------------------------------------------------------------------------
    load_id_card = AuditedBashOperator(
        task_id="load_bronze_id_card",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python {SCRIPTS_DIR}/extract/load_bronze_unstructured.py "
            f"--csv {DATA_DIR}/unstructured/extracted/id_card_extractions_{{{{ ds }}}}.csv "
            f"--doc-type id_card"
        ),
        env=SCRIPT_ENV, append_env=True,
        execution_timeout=timedelta(minutes=15),
    )

    load_savings_book = AuditedBashOperator(
        task_id="load_bronze_savings_book",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python {SCRIPTS_DIR}/extract/load_bronze_unstructured.py "
            f"--csv {DATA_DIR}/unstructured/extracted/savings_book_roi_extractions_{{{{ ds }}}}.csv "
            f"--doc-type savings_book"
        ),
        env=SCRIPT_ENV, append_env=True,
        execution_timeout=timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # Validate quality (warn, không fail)
    # -------------------------------------------------------------------------
    validate = AuditedPythonOperator(
        task_id="validate_ocr_quality",
        python_callable=validate_ocr_quality,
        retries=0,
        execution_timeout=timedelta(minutes=5),
        trigger_rule="all_done",
    )

    notify = AuditedPythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        retries=0,
        trigger_rule="all_success",
    )

    ocr_id_card      >> load_id_card      >> validate
    ocr_savings_book >> load_savings_book >> validate
    validate >> notify

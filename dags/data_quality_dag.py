"""
data_quality_dag — DQ checks chạy sau banking_structured_dag.
Schedule: @daily 04:00 (sau structured DAG)
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG

sys.path.append(str(Path(__file__).resolve().parent))

from common.constants import (
    DEFAULT_ARGS, VAR_FRESHNESS_MAX_HOURS, VAR_ROW_COUNT_DRIFT_PCT,
)
from common.callbacks import notify_success
from common.operators import AuditedPythonOperator


DAG_ID = "data_quality_dag"


# =============================================================================
# DQ check callables
# =============================================================================
def _engine():
    from scripts.utils.db_connection import get_target_engine
    return get_target_engine()


def check_row_count_drift(audit_row_id: int = None, **ctx):
    """fact_transaction row count today vs 7-day avg. Warn nếu lệch > threshold%."""
    from airflow.models import Variable
    from sqlalchemy import text

    threshold_pct = float(Variable.get(VAR_ROW_COUNT_DRIFT_PCT, default_var="20"))
    run_date = ctx["ds"]
    date_key = int(run_date.replace("-", ""))

    with _engine().connect() as conn:
        today = conn.execute(text("""
            SELECT COUNT(*) FROM gold.fact_transaction WHERE date_key = :dk
        """), {"dk": date_key}).scalar() or 0

        avg7 = conn.execute(text("""
            SELECT AVG(CAST(c AS FLOAT)) FROM (
                SELECT date_key, COUNT(*) AS c
                FROM gold.fact_transaction
                WHERE date_key BETWEEN :dk_min AND :dk_max
                GROUP BY date_key
            ) t
        """), {
            "dk_min": int((datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y%m%d")),
            "dk_max": int((datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")),
        }).scalar() or 0

    drift = abs(today - avg7) / max(avg7, 1) * 100
    print(f"[row_count_drift] today={today}, avg7d={avg7:.1f}, drift={drift:.1f}%, threshold={threshold_pct}%")
    if drift > threshold_pct:
        # warn only, do not raise
        print(f"⚠ WARN: row count drift {drift:.1f}% exceeds {threshold_pct}%")


def check_data_freshness(audit_row_id: int = None, **ctx):
    """fact_transaction max(dbt_updated_at) phải trong ngưỡng (mặc định 24h)."""
    from airflow.models import Variable
    from sqlalchemy import text

    max_hours = float(Variable.get(VAR_FRESHNESS_MAX_HOURS, default_var="24"))

    with _engine().connect() as conn:
        latest = conn.execute(text("""
            SELECT MAX(dbt_updated_at) FROM gold.fact_transaction
        """)).scalar()
    if latest is None:
        print("[freshness] ⚠ WARN: fact_transaction empty — freshness undefined")
        return

    now_utc = datetime.now(tz=timezone.utc)
    latest_aware = latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else latest
    age_hours = (now_utc - latest_aware).total_seconds() / 3600
    print(f"[freshness] latest={latest}, age={age_hours:.2f}h, max={max_hours}h")
    if age_hours > max_hours:
        raise ValueError(f"Stale data: {age_hours:.1f}h > {max_hours}h")


def check_null_fk(audit_row_id: int = None, **ctx):
    """Fact phải có FK match dim tương ứng."""
    from sqlalchemy import text
    issues = []
    queries = {
        "customer": """
            SELECT COUNT(*) FROM gold.fact_transaction f
            LEFT JOIN gold.dim_customer d ON f.customer_id = d.customer_id
            WHERE d.customer_id IS NULL""",
        "card": """
            SELECT COUNT(*) FROM gold.fact_transaction f
            LEFT JOIN gold.dim_card d ON f.card_id = d.card_id
            WHERE d.card_id IS NULL""",
        "merchant": """
            SELECT COUNT(*) FROM gold.fact_transaction f
            LEFT JOIN gold.dim_merchant d ON f.merchant_id = d.merchant_id
            WHERE d.merchant_id IS NULL""",
        "mcc": """
            SELECT COUNT(*) FROM gold.fact_transaction f
            LEFT JOIN gold.dim_mcc d ON f.mcc_id = d.mcc_id
            WHERE d.mcc_id IS NULL""",
        "date": """
            SELECT COUNT(*) FROM gold.fact_transaction f
            LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL""",
    }
    with _engine().connect() as conn:
        for dim, sql in queries.items():
            cnt = conn.execute(text(sql)).scalar() or 0
            print(f"[null_fk] {dim}: {cnt} orphans")
            if cnt > 0:
                issues.append(f"{dim}={cnt}")
    if issues:
        # warn only — for visibility; promote to raise if strict
        print(f"⚠ WARN: null FK detected → {', '.join(issues)}")


def check_duplicate_pk(audit_row_id: int = None, **ctx):
    """fact_transaction.transaction_id phải unique. Đây là CRITICAL — raise."""
    from sqlalchemy import text
    with _engine().connect() as conn:
        cnt = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT transaction_id
                FROM gold.fact_transaction
                GROUP BY transaction_id HAVING COUNT(*) > 1
            ) t
        """)).scalar() or 0
    print(f"[duplicate_pk] duplicate transaction_id count = {cnt}")
    if cnt > 0:
        raise ValueError(f"CRITICAL: {cnt} duplicate transaction_id in fact_transaction")


def check_ocr_quality(audit_row_id: int = None, **ctx):
    """Average OCR confidence cho ngày hôm nay (Bronze OCR tables)."""
    from sqlalchemy import text
    run_date = ctx["ds"]
    with _engine().connect() as conn:
        for table in ["id_card_results", "savings_book_results"]:
            try:
                row = conn.execute(text(f"""
                    SELECT COUNT(*) AS n, AVG(CAST(final_confidence AS FLOAT)) AS avg_conf
                    FROM bronze.{table}
                    WHERE run_date = :rd
                """), {"rd": run_date}).fetchone()
                n = int(row[0] or 0)
                avg = float(row[1] or 0)
                print(f"[ocr_quality] {table}: n={n}, avg_conf={avg:.3f}")
                if n > 0 and avg < 0.7:
                    print(f"  ⚠ WARN: avg confidence {avg:.3f} < 0.70")
            except Exception as e:
                print(f"[ocr_quality] {table}: skipped ({e})")


def wait_for_structured_dag(audit_row_id: int = None, **ctx):
    """
    Kiểm tra banking_structured_dag đã chạy thành công gần nhất (trong 24h qua).
    - Tìm thấy → log thông tin run.
    - Không tìm thấy → log WARNING và tiếp tục (DQ vẫn chạy, data có thể không mới).
    """
    import datetime
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState
    from airflow.utils.session import create_session
    from sqlalchemy import desc

    cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=24)

    with create_session() as session:
        recent_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "banking_structured_dag",
                DagRun.state == DagRunState.SUCCESS,
                DagRun.end_date >= cutoff,
            )
            .order_by(desc(DagRun.end_date))
            .first()
        )

    if recent_run:
        print(f"[wait_structured] ✓ Found: run_id={recent_run.run_id}, "
              f"ended={recent_run.end_date}")
    else:
        print("[wait_structured] ⚠ WARNING: banking_structured_dag chưa có run thành công "
              "trong 24h qua — DQ checks vẫn chạy nhưng data có thể không mới nhất.")


def generate_dq_report(audit_row_id: int = None, **ctx):
    """Đọc audit.pipeline_run_log → tổng hợp báo cáo cuối ngày."""
    from sqlalchemy import text
    run_date = ctx["ds"]
    with _engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT dag_id, task_id, status, duration_sec, rows_inserted, error_message
            FROM audit.pipeline_run_log
            WHERE run_date = :rd
            ORDER BY dag_id, started_at
        """), {"rd": run_date}).fetchall()
    print(f"\n========== DAILY DQ REPORT — {run_date} ==========")
    for r in rows:
        status_icon = {"success": "✓", "failed": "✗", "started": "…"}.get(r[2], "?")
        print(f"  {status_icon} {r[0]}.{r[1]:<40} {r[2]:<8} {r[3] or 0}s  rows={r[4] or 0}")
        if r[5]:
            print(f"      ERROR: {r[5][:200]}")
    print("=" * 60)


# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id=DAG_ID,
    description="Data quality checks sau khi banking_structured_dag chạy xong",
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 5, 25),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["banking", "dq", "monitoring"],
) as dag:

    wait_for_structured = AuditedPythonOperator(
        task_id="wait_for_structured_dag",
        python_callable=wait_for_structured_dag,
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )

    dq_row_drift   = AuditedPythonOperator(
        task_id="check_row_count_drift",
        python_callable=check_row_count_drift,
        trigger_rule="all_done",
    )
    dq_freshness   = AuditedPythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        trigger_rule="all_done",
    )
    dq_null_fk     = AuditedPythonOperator(
        task_id="check_null_fk",
        python_callable=check_null_fk,
        trigger_rule="all_done",
    )
    dq_dup_pk      = AuditedPythonOperator(
        task_id="check_duplicate_pk",
        python_callable=check_duplicate_pk,
        trigger_rule="all_done",
    )
    dq_ocr         = AuditedPythonOperator(
        task_id="check_ocr_quality",
        python_callable=check_ocr_quality,
        trigger_rule="all_done",
    )

    report = AuditedPythonOperator(
        task_id="generate_dq_report",
        python_callable=generate_dq_report,
        trigger_rule="all_done",
    )

    notify = AuditedPythonOperator(
        task_id="notify_team",
        python_callable=notify_success,
        # all_success: mail "✅" chỉ bắn khi MỌI DQ check + report đều pass.
        # (notify phải nhận trực tiếp các check làm cha, vì trigger_rule chỉ xét cha trực tiếp;
        #  nếu chỉ nối sau report — vốn all_done nên luôn success — mail sẽ bắn nhầm cả khi check fail.)
        trigger_rule="all_success",
    )

    checks = [dq_row_drift, dq_freshness, dq_null_fk, dq_dup_pk, dq_ocr]
    # report giữ all_done để LUÔN sinh báo cáo dù check fail; notify all_success nên chỉ chạy khi mọi check pass.
    wait_for_structured >> checks >> report
    checks >> notify
    report >> notify

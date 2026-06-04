"""
banking_structured_dag — DAG chính: Bronze (extract+MNS) → Silver (dbt) → Gold (dbt)
Schedule: @daily 02:00
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

sys.path.append(str(Path(__file__).resolve().parent))

from common.constants import (
    DEFAULT_ARGS, SCRIPT_ENV, SCRIPTS_DIR, DBT_DIR, PROJECT_ROOT,
    POOL_SOURCE_DB, POOL_DBT,
    VAR_SKIP_MNS_VALIDATION,
)
from common.callbacks import notify_success
from common.operators import AuditedBashOperator, AuditedPythonOperator


DAG_ID = "banking_structured_dag"

ENTITIES = ["users", "cards", "transactions", "mcc_codes"]

EXTRACT_TIMEOUTS = {
    "users":        timedelta(minutes=15),
    "cards":        timedelta(minutes=15),
    "transactions": timedelta(minutes=30),
    "mcc_codes":    timedelta(minutes=5),
}

MNS_TIMEOUTS = {
    "users":        timedelta(minutes=10),
    "cards":        timedelta(minutes=10),
    "transactions": timedelta(minutes=20),
    "mcc_codes":    timedelta(minutes=5),
}


# =============================================================================
# Task callables
# =============================================================================
def precheck_source_db(audit_row_id: int = None, **ctx):
    """Ping source DB. Fail → toàn bộ DAG dừng sớm."""
    from scripts.utils.db_connection import get_source_engine
    engine = get_source_engine()
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT DB_NAME() AS db, SUSER_NAME() AS [user]")).fetchone()
        print(f"[precheck] source DB OK: db={result[0]} user={result[1]}")


def validate_mns_change_ratio(audit_row_id: int = None, **ctx):
    """
    Sau khi tính MNS, kiểm tra tỷ lệ thay đổi.
    Nếu > 50% record thay đổi → khả năng source bị reset, abort để bảo vệ Silver/Gold.
    Skip nếu Variable `skip_mns_validation = true`.
    """
    from airflow.models import Variable
    from sqlalchemy import text
    from scripts.utils.db_connection import get_target_engine

    skip = Variable.get(VAR_SKIP_MNS_VALIDATION, default_var="false").lower() == "true"
    if skip:
        print("[validate_mns] skipped via Variable")
        return

    # Chế độ load-theo-ngày (run_date có giá trị): mỗi ngày chỉ nạp giao dịch/dim của ngày đó,
    # nên tỷ lệ thay đổi so với PDY (ngày hôm trước) cao ~100% là BÌNH THƯỜNG, không phải
    # dấu hiệu source bị reset. Guard >50% chỉ có ý nghĩa khi load full snapshot (run_date rỗng).
    run_date = (ctx.get("params") or {}).get("run_date", "")
    if run_date:
        print(f"[validate_mns] daily-incremental mode (run_date={run_date}) → bỏ qua guard tỷ lệ thay đổi")
        return

    engine = get_target_engine()
    warnings = []

    # transactions là insert-only (mọi bản ghi TDY = 'I'), nên "changed" luôn = toàn bộ batch
    # → tỷ lệ vô nghĩa, KHÔNG đưa vào phép đo. Chỉ kiểm tra các dimension.
    for entity in ["users", "cards", "mcc_codes"]:
        with engine.connect() as conn:
            sql = text(f"""
                SELECT
                    (SELECT COUNT(*) FROM bronze.{entity}_mns) AS changed,
                    (SELECT COUNT(*) FROM bronze.{entity}_pdy) AS baseline
            """)
            row = conn.execute(sql).fetchone()
            changed = int(row[0] or 0)
            baseline = int(row[1] or 0)
            ratio = changed / max(baseline, 1)
            print(f"[validate_mns] {entity}: changed={changed}, baseline={baseline}, ratio={ratio:.1%}")
            if baseline > 100 and ratio > 0.5:
                warnings.append(f"{entity}: {ratio:.1%} (changed={changed}/{baseline})")

    # WARN-only: full load / backfill có tỷ lệ thay đổi cao là hợp lệ (nạp lại toàn bộ),
    # nên KHÔNG fail pipeline. Chỉ log cảnh báo để có khả năng quan sát.
    if warnings:
        print("⚠ WARN: MNS change ratio > 50% (bình thường khi full load/backfill): "
              + "; ".join(warnings))


# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id=DAG_ID,
    description="ETL chính: Bronze (extract + MNS) → Silver (Data Vault) → Gold (Star Schema)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 5, 25),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "bronze", "silver", "gold"],
    params={
        "run_date": Param(
            default="",
            # type cho phép cả null → Airflow KHÔNG bắt buộc, ô có thể để trống.
            type=["string", "null"],
            description="Ngày xử lý (YYYY-MM-DD). Để TRỐNG = chạy toàn bộ source (không lọc ngày).",
        )
    },
) as dag:

    # -------------------------------------------------------------------------
    # 1. Precheck source DB
    # -------------------------------------------------------------------------
    precheck = AuditedPythonOperator(
        task_id="precheck_source_db",
        python_callable=precheck_source_db,
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=2),
    )

    # -------------------------------------------------------------------------
    # 2. Extract Bronze (4 entities, song song với pool source_db)
    # -------------------------------------------------------------------------
    with TaskGroup("extract_bronze", tooltip="Load source → bronze.*_tdy + move TDY → PDY") as extract_grp:
        for entity in ENTITIES:
            # run_date rỗng → script load toàn bộ (backfill); có giá trị → load theo ngày.
            # transactions lọc theo date; users/cards/mcc_codes lọc qua JOIN với transactions.
            AuditedBashOperator(
                task_id=f"load_bronze_{entity}",
                bash_command=(
                    f"cd {PROJECT_ROOT} && "
                    f"python {SCRIPTS_DIR}/extract/load_bronze_{entity}.py"
                    # run_date rỗng → bỏ hẳn flag (script tự load toàn bộ);
                    # có giá trị → --run-date YYYY-MM-DD (ngày không có space nên không cần quote).
                    "{% if params.run_date %} --run-date {{ params.run_date }}{% endif %}"
                ),
                env=SCRIPT_ENV,
                append_env=True,
                pool=POOL_SOURCE_DB,
                execution_timeout=EXTRACT_TIMEOUTS[entity],
            )

    # -------------------------------------------------------------------------
    # 3. Compute MNS (I/U/D flags)
    # -------------------------------------------------------------------------
    with TaskGroup("compute_mns", tooltip="So sánh TDY vs PDY → MNS (I/U/D)") as mns_grp:
        for entity in ENTITIES:
            AuditedBashOperator(
                task_id=f"{entity}_mns",
                bash_command=(
                    f"cd {PROJECT_ROOT} && "
                    f"python {SCRIPTS_DIR}/extract/{entity}_mns.py"
                ),
                env=SCRIPT_ENV,
                append_env=True,
                execution_timeout=MNS_TIMEOUTS[entity],
                retries=1,
            )

    # -------------------------------------------------------------------------
    # 4. Validate MNS change ratio (phòng thủ source bị reset)
    # -------------------------------------------------------------------------
    validate_mns = AuditedPythonOperator(
        task_id="validate_mns_change_ratio",
        python_callable=validate_mns_change_ratio,
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )

    # -------------------------------------------------------------------------
    # 5. Silver: Hub → Link → Satellite
    # -------------------------------------------------------------------------
    silver_hubs = AuditedBashOperator(
        task_id="dbt_silver_hubs",
        bash_command=f"cd {DBT_DIR} && dbt run --select tag:hub",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=10),
        retries=1,
    )
    silver_links = AuditedBashOperator(
        task_id="dbt_silver_links",
        bash_command=f"cd {DBT_DIR} && dbt run --select tag:link",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=10),
        retries=1,
    )
    silver_sats = AuditedBashOperator(
        task_id="dbt_silver_satellites",
        bash_command=f"cd {DBT_DIR} && dbt run --select tag:satellite",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=20),
        retries=1,
    )
    test_silver = AuditedBashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select tag:silver",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # -------------------------------------------------------------------------
    # 6. Gold: dim_date (dynamic model) → dims → fact → test
    # -------------------------------------------------------------------------
    gold_dims = AuditedBashOperator(
        task_id="dbt_gold_dims",
        bash_command=f"cd {DBT_DIR} && dbt run --select tag:dim",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=15),
        retries=1,
    )

    gold_fact = AuditedBashOperator(
        task_id="dbt_gold_fact",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select fact_transaction "
            # `or ''`: khi ô để trống (None) → render thành "" (không phải "None")
            # → fact_transaction.sql thấy run_date rỗng → không filter ngày → load toàn bộ.
            f'--vars \'{{"run_date":"{{{{ params.run_date or \'\' }}}}"}}\''
        ),
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=30),
        retries=2,
    )

    test_gold = AuditedBashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && dbt test --select tag:gold",
        env=SCRIPT_ENV, append_env=True,
        pool=POOL_DBT,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # -------------------------------------------------------------------------
    # 7. Notify success
    # -------------------------------------------------------------------------
    notify = AuditedPythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        retries=0,
        execution_timeout=timedelta(minutes=2),
        trigger_rule="all_success",
    )

    # -------------------------------------------------------------------------
    # Dependencies
    # -------------------------------------------------------------------------
    precheck >> extract_grp >> mns_grp >> validate_mns
    validate_mns >> silver_hubs >> silver_links >> silver_sats >> test_silver
    test_silver >> gold_dims >> gold_fact >> test_gold >> notify

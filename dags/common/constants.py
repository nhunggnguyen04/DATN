"""
Constants chung cho mọi DAG: paths, pools, default args, emails.
"""
from datetime import timedelta

# ============================================================================
# Paths (chạy bên trong Airflow container)
# ============================================================================
PROJECT_ROOT = "/opt/airflow"
SCRIPTS_DIR  = f"{PROJECT_ROOT}/scripts"
DBT_DIR      = f"{PROJECT_ROOT}/dbt_bank"
LOGS_DIR     = f"{PROJECT_ROOT}/logs"
DATA_DIR     = f"{PROJECT_ROOT}/data"

# Python virtual env paths (cho OCR vs main)
VENV_MAIN     = f"{PROJECT_ROOT}/.venv/bin/activate"
VENV_OCR      = f"{PROJECT_ROOT}/.venv_ocr/bin/activate"

# ============================================================================
# Pools — giới hạn concurrency
# ============================================================================
POOL_SOURCE_DB = "source_db_pool"     # max 2 — extract từ source song song
POOL_DBT       = "dbt_pool"           # max 4 — dbt models
POOL_OCR       = "ocr_pool"           # max 1 — CPU bound

# ============================================================================
# Email
# ============================================================================
EMAIL_OWNERS = ["nguyenhongnhungtxa@gmail.com"]
EMAIL_ONCALL = ["nguyenhongnhungtxa@gmail.com"]

# ============================================================================
# Airflow connections / variables
# ============================================================================
CONN_SOURCE = "sql_source"
CONN_TARGET = "sql_target"

VAR_SKIP_MNS_VALIDATION    = "skip_mns_validation"
VAR_OCR_CONF_THRESHOLD     = "ocr_conf_threshold"
VAR_FRESHNESS_MAX_HOURS    = "freshness_max_hours"
VAR_ROW_COUNT_DRIFT_PCT    = "row_count_drift_pct"

# ============================================================================
# DEFAULT_ARGS cho DAGs
# ============================================================================
DEFAULT_ARGS = {
    "owner": "nhunggnguyen",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    # Thông báo chỉ qua email (đã bỏ Slack): email khi task lỗi và khi retry.
    "email_on_failure": True,
    "email_on_retry": True,
    "email": EMAIL_OWNERS,
}

# ============================================================================
# Env vars truyền xuống mọi BashOperator (để script Python tự ghi audit)
# ============================================================================
SCRIPT_ENV = {
    "AIRFLOW_RUN_DATE":   "{{ ds }}",
    "AIRFLOW_TRY_NUMBER": "{{ task_instance.try_number }}",
    "AIRFLOW_LOG_PATH":   "",
    "AIRFLOW_LOG_URL":    "{{ ti.log_url }}",
    "AIRFLOW_DAG_ID":     "{{ dag.dag_id }}",
    "AIRFLOW_TASK_ID":    "{{ task.task_id }}",
    "PYTHONPATH":         PROJECT_ROOT,
}

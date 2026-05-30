"""
Audit logger — ghi run log của mỗi pipeline task vào audit.pipeline_run_log.

Hai cách dùng:

1. Context manager (cho Python scripts):
    with audit_run("banking_structured_dag", "load_bronze_users", "2026-05-25") as a:
        df = extract()
        a["rows_inserted"] = len(df)

2. Function API (cho operators):
    row_id = audit_start("dag", "task", "2026-05-25")
    try:
        ...
        audit_finish(row_id, status="success", rows_inserted=100)
    except Exception as e:
        audit_finish(row_id, status="failed", error=str(e))
        raise
"""
import json
import os
import socket
import traceback
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from scripts.utils.db_connection import get_target_engine


def _read_airflow_env() -> dict:
    """Đọc env vars do DAG truyền xuống. Default empty nếu chạy local."""
    return {
        "attempt": int(os.environ.get("AIRFLOW_TRY_NUMBER", 1)),
        "log_file_path": os.environ.get("AIRFLOW_LOG_PATH"),
        "airflow_log_url": os.environ.get("AIRFLOW_LOG_URL"),
    }


def audit_start(
    dag_id: str,
    task_id: str,
    run_date: str,
    attempt: Optional[int] = None,
    log_file: Optional[str] = None,
    airflow_log_url: Optional[str] = None,
) -> int:
    """
    Insert một row 'started' vào audit.pipeline_run_log, trả về run_log_id.
    """
    env = _read_airflow_env()
    attempt = attempt if attempt is not None else env["attempt"]
    log_file = log_file or env["log_file_path"]
    airflow_log_url = airflow_log_url or env["airflow_log_url"]

    sql = text("""
        INSERT INTO audit.pipeline_run_log
            (dag_id, task_id, run_date, attempt, status,
             started_at, log_file_path, airflow_log_url, host_name)
        OUTPUT INSERTED.run_log_id
        VALUES
            (:dag_id, :task_id, :run_date, :attempt, 'started',
             SYSUTCDATETIME(), :log_file, :airflow_url, :host)
    """)

    engine = get_target_engine()
    with engine.begin() as conn:
        result = conn.execute(sql, {
            "dag_id": dag_id,
            "task_id": task_id,
            "run_date": run_date,
            "attempt": attempt,
            "log_file": log_file,
            "airflow_url": airflow_log_url,
            "host": socket.gethostname(),
        })
        return int(result.scalar())


def audit_finish(
    run_log_id: int,
    status: str,
    rows_processed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_deleted: int = 0,
    error_message: Optional[str] = None,
    extra_metadata: Optional[dict] = None,
) -> None:
    """Update row tương ứng thành success/failed."""
    sql = text("""
        UPDATE audit.pipeline_run_log
        SET status         = :status,
            ended_at       = SYSUTCDATETIME(),
            rows_processed = :rp,
            rows_inserted  = :ri,
            rows_updated   = :ru,
            rows_deleted   = :rd,
            error_message  = :err,
            extra_metadata = :extra
        WHERE run_log_id = :rid
    """)

    err = error_message[:4000] if error_message else None
    extra = json.dumps(extra_metadata, default=str) if extra_metadata else None

    engine = get_target_engine()
    with engine.begin() as conn:
        conn.execute(sql, {
            "status": status,
            "rp": rows_processed,
            "ri": rows_inserted,
            "ru": rows_updated,
            "rd": rows_deleted,
            "err": err,
            "extra": extra,
            "rid": run_log_id,
        })


@contextmanager
def audit_run(
    dag_id: str,
    task_id: str,
    run_date: str,
    attempt: Optional[int] = None,
    log_file: Optional[str] = None,
):
    """
    Context manager tự ghi started/success/failed.

    Usage:
        with audit_run("banking_structured_dag", "load_bronze_users", "2026-05-25") as a:
            df = extract()
            a["rows_processed"] = len(df)
            a["rows_inserted"]  = len(df)
            a["extra"] = {"source_host": "..."}
    """
    run_log_id = audit_start(dag_id, task_id, run_date, attempt, log_file)
    audit = {
        "rows_processed": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "extra": {},
    }

    try:
        yield audit
        audit_finish(
            run_log_id,
            status="success",
            rows_processed=audit["rows_processed"],
            rows_inserted=audit["rows_inserted"],
            rows_updated=audit["rows_updated"],
            rows_deleted=audit["rows_deleted"],
            extra_metadata=audit["extra"] or None,
        )
    except Exception as e:
        err_text = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        audit_finish(
            run_log_id,
            status="failed",
            error_message=err_text,
        )
        raise

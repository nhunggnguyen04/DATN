"""
Custom operators wrap BashOperator/PythonOperator + ghi audit.pipeline_run_log
khi task bắt đầu/kết thúc — không cần chỉnh script Python hiện hữu.
"""
import logging
import sys
from pathlib import Path
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Cho phép import scripts.utils.audit_logger từ trong Airflow worker
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


def _safe_audit_finish(run_log_id, **kwargs) -> None:
    """audit_finish with silent fallback — never overrides the original exception."""
    if run_log_id is None:
        return
    try:
        from scripts.utils.audit_logger import audit_finish
        audit_finish(run_log_id, **kwargs)
    except Exception as exc:
        logging.warning(f"[audit] audit_finish failed (ignored): {exc}")


class AuditedBashOperator(BashOperator):
    """
    BashOperator + tự động ghi audit.pipeline_run_log:
      - 'started' trước khi chạy bash
      - 'success' nếu return 0
      - 'failed'  nếu raise (ghi traceback)

    Lưu ý: row counts không lấy được từ bash output —
    để có rows_*, script Python phải tự dùng audit_run() context manager.
    """

    template_fields = BashOperator.template_fields + ("dag_id_override",)

    def __init__(self, *, dag_id_override: str = None, **kwargs):
        super().__init__(**kwargs)
        self.dag_id_override = dag_id_override

    def execute(self, context: dict) -> Any:
        from airflow.exceptions import AirflowSkipException
        from scripts.utils.audit_logger import audit_start

        dag_id = self.dag_id_override or context["dag"].dag_id
        task_id = context["task"].task_id
        run_date = context["ds"]
        attempt = context["task_instance"].try_number
        log_url = context["task_instance"].log_url

        run_log_id = None
        try:
            run_log_id = audit_start(
                dag_id=dag_id,
                task_id=task_id,
                run_date=run_date,
                attempt=attempt,
                airflow_log_url=log_url,
            )
        except Exception as exc:
            logging.warning(f"[AuditedBashOperator] audit_start failed (ignored): {exc}")

        logging.info(f"[AuditedBashOperator] audit row_id={run_log_id}")

        try:
            result = super().execute(context)
            _safe_audit_finish(run_log_id, status="success")
            return result
        except AirflowSkipException:
            _safe_audit_finish(run_log_id, status="skipped")
            raise
        except Exception as e:
            import traceback
            _safe_audit_finish(
                run_log_id,
                status="failed",
                error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
            )
            raise


class AuditedPythonOperator(PythonOperator):
    """
    PythonOperator + audit logging. python_callable nhận thêm
    kwarg `audit_row_id` để callable có thể update row_counts nếu muốn.
    """

    def execute(self, context: dict) -> Any:
        from airflow.exceptions import AirflowSkipException
        from scripts.utils.audit_logger import audit_start

        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        run_date = context["ds"]
        attempt = context["task_instance"].try_number
        log_url = context["task_instance"].log_url

        run_log_id = None
        try:
            run_log_id = audit_start(
                dag_id=dag_id, task_id=task_id, run_date=run_date,
                attempt=attempt, airflow_log_url=log_url,
            )
        except Exception as exc:
            logging.warning(f"[AuditedPythonOperator] audit_start failed (ignored): {exc}")

        # Inject audit_row_id vào op_kwargs để callable có thể dùng
        self.op_kwargs = {**(self.op_kwargs or {}), "audit_row_id": run_log_id}

        try:
            result = super().execute(context)
            _safe_audit_finish(run_log_id, status="success")
            return result
        except AirflowSkipException:
            _safe_audit_finish(run_log_id, status="skipped")
            raise
        except Exception as e:
            import traceback
            _safe_audit_finish(
                run_log_id,
                status="failed",
                error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
            )
            raise

"""
Callbacks cho on_success — gửi email thông báo (chỉ email, không dùng Slack).
Email khi task lỗi/retry do Airflow tự gửi qua email_on_failure/email_on_retry.
"""
import logging
from typing import Any


def _send_email(subject: str, body: str, to: list) -> None:
    """Gửi email qua Airflow SMTP. Fail silently nếu chưa cấu hình."""
    try:
        from airflow.utils.email import send_email
        send_email(to=to, subject=subject, html_content=body)
        logging.info(f"Email sent to {to}: {subject}")
    except Exception as e:
        logging.warning(f"Email alert failed (ignored): {e}")


def notify_success(**context: Any) -> None:
    """Task callable — gọi cuối DAG để báo cáo + ghi log + gửi email."""
    from common.constants import EMAIL_OWNERS
    dag_id = context.get("dag").dag_id
    run_date = context.get("ds")
    logging.info(f"[notify_success] {dag_id} completed for {run_date}")

    # Email
    subject = f"[Airflow] ✅ {dag_id} completed — {run_date}"
    body = f"""
    <h3>✅ Pipeline hoàn thành</h3>
    <table>
      <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
      <tr><td><b>Run date</b></td><td>{run_date}</td></tr>
      <tr><td><b>Status</b></td><td>SUCCESS</td></tr>
    </table>
    <p>Kiểm tra chi tiết tại <a href="http://localhost:8080">Airflow UI</a></p>
    """
    _send_email(subject=subject, body=body, to=EMAIL_OWNERS)

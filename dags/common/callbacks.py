"""
Callbacks cho on_failure / on_retry / on_success.
Gửi Slack alert + ghi audit nếu task không tự ghi (ví dụ task fail sớm).
"""
import logging
from typing import Any


def _format_alert(context: dict, level: str) -> str:
    """Format Slack alert text từ Airflow context."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    dag_id = dag.dag_id if dag else "unknown"
    task_id = ti.task_id if ti else "unknown"
    try_number = ti.try_number if ti else "?"
    max_tries = ti.max_tries + 1 if ti else "?"
    log_url = ti.log_url if ti else ""
    run_date = str(context.get("ds", ""))
    err = str(exception)[:500] if exception else ""

    emoji = {"failure": ":rotating_light:", "retry": ":warning:", "success": ":white_check_mark:"}.get(level, "")
    headline = {
        "failure": f"{emoji} *{dag_id}.{task_id}* FAILED",
        "retry":   f"{emoji} *{dag_id}.{task_id}* retry {try_number}/{max_tries}",
        "success": f"{emoji} *{dag_id}* completed successfully",
    }[level]

    lines = [
        headline,
        f"   Date: {run_date}",
        f"   Attempt: {try_number}/{max_tries}",
    ]
    if err:
        lines.append(f"   Error: `{err}`")
    if log_url:
        lines.append(f"   :link: <{log_url}|View log>")
    lines.append(
        f"   :clipboard: `SELECT * FROM audit.pipeline_run_log "
        f"WHERE dag_id='{dag_id}' AND task_id='{task_id}' AND run_date='{run_date}' "
        f"ORDER BY started_at DESC`"
    )
    return "\n".join(lines)


def _send_slack(message: str) -> None:
    """Gửi message tới Slack. Fail silently nếu chưa cấu hình conn."""
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_alert")
        hook.send(text=message)
    except Exception as e:
        logging.warning(f"Slack alert failed (ignored): {e}")


def slack_alert(context: dict) -> None:
    """on_failure_callback — gửi cảnh báo critical."""
    msg = _format_alert(context, "failure")
    logging.error(msg)
    _send_slack(msg)


def slack_retry_notice(context: dict) -> None:
    """on_retry_callback — thông báo retry."""
    msg = _format_alert(context, "retry")
    logging.warning(msg)
    _send_slack(msg)


def slack_success(context: dict) -> None:
    """on_success_callback (DAG-level only) — báo DAG xong."""
    msg = _format_alert(context, "success")
    logging.info(msg)
    _send_slack(msg)


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

    # Slack
    _send_slack(f":white_check_mark: *{dag_id}* OK for {run_date}")

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

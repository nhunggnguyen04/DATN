"""
Centralized logger for pipeline scripts.
Ghi đồng thời stdout + file logs/scripts/{name}/{run_date}.log
"""
import logging
import os
import socket
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = PROJECT_ROOT / "logs" / "scripts"


def get_logger(name: str, run_date: str = None) -> tuple[logging.Logger, str]:
    """
    Tạo logger ghi vào stdout + file.

    Args:
        name: Tên job (ví dụ 'load_bronze_users'). Dùng làm subfolder.
        run_date: YYYY-MM-DD, default = today.

    Returns:
        (logger, log_file_path_str)
    """
    run_date = run_date or str(date.today())
    log_dir = LOG_ROOT / name
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{run_date}.log"

    logger = logging.getLogger(f"datn.{name}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] [PID=%(process)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info("-" * 70)
    logger.info(f"Logger initialized: name={name} run_date={run_date} host={socket.gethostname()}")
    logger.info(f"Log file: {log_file}")
    logger.info("-" * 70)

    return logger, str(log_file)

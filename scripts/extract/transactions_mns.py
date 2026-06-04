import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


# Giao dịch là dữ liệu BẤT BIẾN (immutable): mỗi giao dịch ghi nhận 1 lần, không
# sửa/xóa. Khi load-theo-ngày, TDY chỉ chứa giao dịch của run_date → mọi bản ghi
# đều là Insert. KHÔNG so sánh với PDY (giao dịch ngày khác có ID khác nhau, so sánh
# sẽ sinh cờ 'D' sai cho giao dịch hôm trước). Đánh dấu toàn bộ TDY = 'I'; tầng Silver
# tự khử trùng lặp theo business key nên việc chạy lại 1 ngày vẫn idempotent.
COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.transactions_mns;

INSERT INTO bronze.transactions_mns (id, operation_flag)
SELECT
    t.id,
    'I' AS operation_flag
FROM bronze.transactions_tdy t
WHERE t.id IS NOT NULL;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.transactions_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing transactions MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
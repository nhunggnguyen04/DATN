import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from scripts.utils.db_connection import get_target_engine


COMPUTE_MNS_SQL = """
TRUNCATE TABLE bronze.users_mns;

-- I: Có trong TDY nhưng không có trong PDY
INSERT INTO bronze.users_mns (id, operation_flag)
SELECT
    t.id,
    'I' AS operation_flag
FROM bronze.users_tdy t
LEFT JOIN bronze.users_pdy p
    ON t.id = p.id
WHERE p.id IS NULL;

-- U: Có trong cả TDY và PDY nhưng dữ liệu thay đổi
INSERT INTO bronze.users_mns (id, operation_flag)
SELECT
    t.id,
    'U' AS operation_flag
FROM bronze.users_tdy t
INNER JOIN bronze.users_pdy p
    ON t.id = p.id
WHERE
    ISNULL(t.current_age, -1) <> ISNULL(p.current_age, -1)
    OR ISNULL(t.retirement_age, -1) <> ISNULL(p.retirement_age, -1)
    OR ISNULL(t.birth_year, -1) <> ISNULL(p.birth_year, -1)
    OR ISNULL(t.birth_month, -1) <> ISNULL(p.birth_month, -1)
    OR ISNULL(t.gender, '') <> ISNULL(p.gender, '')
    OR ISNULL(t.address, '') <> ISNULL(p.address, '')
    OR ISNULL(t.latitude, '') <> ISNULL(p.latitude, '')
    OR ISNULL(t.longitude, '') <> ISNULL(p.longitude, '')
    OR ISNULL(t.per_capita_income, -1) <> ISNULL(p.per_capita_income, -1)
    OR ISNULL(t.yearly_income, -1) <> ISNULL(p.yearly_income, -1)
    OR ISNULL(t.total_debt, -1) <> ISNULL(p.total_debt, -1)
    OR ISNULL(t.credit_score, -1) <> ISNULL(p.credit_score, -1)
    OR ISNULL(t.num_credit_cards, -1) <> ISNULL(p.num_credit_cards, -1);

-- KHÔNG sinh cờ 'D': ở chế độ load-theo-ngày, TDY chỉ chứa users có giao dịch trong
-- ngày run_date. Một user vắng mặt hôm nay (không giao dịch) KHÔNG có nghĩa bị xóa khỏi
-- source → nếu đánh 'D' sẽ khiến sat_customer_profile soft-delete nhầm hàng loạt.

-- Cập nhật PDY thành snapshot LŨY KẾ: SAU khi đã tính I/U dựa trên PDY cũ, upsert toàn bộ
-- TDY (trạng thái mới nhất hôm nay) vào PDY. Nhờ vậy PDY luôn giữ trạng thái gần nhất của
-- MỌI user đã từng xuất hiện → khi user quay lại giao dịch sau nhiều ngày, diff so với lần
-- thấy gần nhất nên đổi thuộc tính được đánh đúng 'U' (không bị nhầm 'I'), SCD2 chuẩn.
DELETE FROM bronze.users_pdy
WHERE id IN (SELECT id FROM bronze.users_tdy);

INSERT INTO bronze.users_pdy (
    id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards
)
SELECT
    id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards
FROM bronze.users_tdy;
"""


CHECK_MNS_SQL = """
SELECT 
    operation_flag,
    COUNT(*) AS total_records
FROM bronze.users_mns
GROUP BY operation_flag
ORDER BY operation_flag;
"""


def main():
    target_engine = get_target_engine()

    print("Computing users MNS...")
    with target_engine.begin() as conn:
        conn.exec_driver_sql(COMPUTE_MNS_SQL)
        result = conn.exec_driver_sql(CHECK_MNS_SQL)
        rows = result.fetchall()

    print("MNS result:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
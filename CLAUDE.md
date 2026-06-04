# CLAUDE.md

Tài liệu này hướng dẫn Claude Code (claude.ai/code) khi làm việc với mã nguồn trong repository này.

Bộ quy tắc được chia theo chủ đề trong `.claude/rules/` và được nạp tự động qua các import bên dưới. Mỗi file độc lập, có mô tả mục đích ở đầu file.

## Bộ quy tắc

- @.claude/rules/project-conventions.md — Tổng quan hai pipeline, quy ước ngôn ngữ tiếng Việt, tên DAG/database chuẩn (đọc trước tiên).
- @.claude/rules/architecture.md — Cơ sở dữ liệu, luồng pipeline có cấu trúc, mẫu incremental MNS, quy ước tầng Bronze.
- @.claude/rules/dbt-data-vault.md — Quy ước dbt: hash key, satellite SCD2, gold fact, `dim_date`, tag điều phối.
- @.claude/rules/audit-and-data-quality.md — Audit logging và các DQ check (cảnh báo vs. raise).
- @.claude/rules/environment-and-infrastructure.md — Hai virtualenv, layout Docker, tiện ích kết nối/hằng số dùng chung.
- @.claude/rules/commands.md — Lệnh thường dùng cho từng pipeline (PowerShell).
- @.claude/rules/security.md — Xử lý credential và thông tin nhạy cảm.

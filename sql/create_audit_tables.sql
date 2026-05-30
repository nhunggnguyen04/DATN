-- =============================================================================
-- Audit schema for pipeline run logging
-- Mọi DAG task ghi 1 row khi started, update status khi success/failed
-- =============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO

IF OBJECT_ID('audit.pipeline_run_log', 'U') IS NOT NULL
    DROP TABLE audit.pipeline_run_log;
GO

CREATE TABLE audit.pipeline_run_log (
    run_log_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
    dag_id          NVARCHAR(100)   NOT NULL,
    task_id         NVARCHAR(100)   NOT NULL,
    run_date        DATE            NOT NULL,
    attempt         INT             NOT NULL DEFAULT 1,
    status          NVARCHAR(20)    NOT NULL,   -- started / success / failed / skipped
    started_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    ended_at        DATETIME2       NULL,
    duration_sec    AS DATEDIFF(SECOND, started_at, ended_at) PERSISTED,
    rows_processed  BIGINT          NULL,
    rows_inserted   BIGINT          NULL,
    rows_updated    BIGINT          NULL,
    rows_deleted    BIGINT          NULL,
    error_message   NVARCHAR(MAX)   NULL,
    log_file_path   NVARCHAR(500)   NULL,
    airflow_log_url NVARCHAR(500)   NULL,
    extra_metadata  NVARCHAR(MAX)   NULL,       -- JSON freeform
    host_name       NVARCHAR(100)   NULL
);
GO

CREATE INDEX IX_pipeline_run_log_dag_date ON audit.pipeline_run_log(dag_id, run_date);
CREATE INDEX IX_pipeline_run_log_status   ON audit.pipeline_run_log(status, run_date);
CREATE INDEX IX_pipeline_run_log_task     ON audit.pipeline_run_log(task_id, run_date);
GO

-- =============================================================================
-- View tiện dụng: trạng thái run mới nhất theo task
-- =============================================================================
IF OBJECT_ID('audit.v_latest_run_per_task', 'V') IS NOT NULL
    DROP VIEW audit.v_latest_run_per_task;
GO

CREATE VIEW audit.v_latest_run_per_task AS
WITH ranked AS (
    SELECT
        run_log_id, dag_id, task_id, run_date, attempt, status,
        started_at, ended_at, duration_sec,
        rows_inserted, error_message,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id, task_id
            ORDER BY started_at DESC
        ) AS rn
    FROM audit.pipeline_run_log
)
SELECT *
FROM ranked
WHERE rn = 1;
GO

-- =============================================================================
-- View: success rate 30 ngày qua
-- =============================================================================
IF OBJECT_ID('audit.v_task_success_rate_30d', 'V') IS NOT NULL
    DROP VIEW audit.v_task_success_rate_30d;
GO

CREATE VIEW audit.v_task_success_rate_30d AS
SELECT
    dag_id,
    task_id,
    COUNT(*)                                                AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)     AS success_count,
    SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END)     AS failed_count,
    CAST(
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)
    )                                                       AS success_rate_pct,
    AVG(CAST(duration_sec AS BIGINT))                       AS avg_duration_sec,
    MAX(duration_sec)                                       AS max_duration_sec
FROM audit.pipeline_run_log
WHERE run_date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
GROUP BY dag_id, task_id;
GO

PRINT 'Audit tables and views created successfully.';

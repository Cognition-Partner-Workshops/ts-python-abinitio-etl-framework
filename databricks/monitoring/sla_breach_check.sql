-- Databricks SQL Alert: SLA Breach Detection
-- Migrated from: monitoring/sla_tracker.py (SLATracker.check_sla)
--
-- Original pattern: SLATracker checked if jobs completed before their
-- sla_window_end time. If not, it logged a warning and triggered alerts.
--
-- This query fires if the daily_orders_pipeline has NOT completed
-- successfully by 06:00 UTC on the current day.

WITH sla_definitions AS (
    SELECT 'daily_orders_pipeline' AS job_name, '06:00' AS sla_time_utc
),
todays_completions AS (
    SELECT
        job_name,
        MAX(CAST(end_time AS TIMESTAMP)) AS last_completion
    FROM system.workflow.job_run_timeline
    WHERE DATE(start_time) = CURRENT_DATE
      AND result_state = 'SUCCESS'
      AND job_name = 'daily_orders_pipeline'
    GROUP BY job_name
)
SELECT
    s.job_name,
    s.sla_time_utc AS sla_deadline,
    COALESCE(t.last_completion, NULL) AS last_successful_run,
    CASE
        WHEN t.last_completion IS NULL THEN 'NOT_COMPLETED'
        ELSE 'COMPLETED_LATE'
    END AS current_status
FROM sla_definitions s
LEFT JOIN todays_completions t ON s.job_name = t.job_name
WHERE CAST(current_timestamp() AS TIME) > CAST(s.sla_time_utc AS TIME)
  AND (t.last_completion IS NULL OR CAST(t.last_completion AS TIME) > CAST(s.sla_time_utc AS TIME));

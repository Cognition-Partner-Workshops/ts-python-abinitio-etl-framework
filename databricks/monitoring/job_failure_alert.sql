-- Databricks SQL Alert: Job Failure Detection
-- Migrated from: monitoring/job_monitor.py (AutoSys API + Slack alerts)
--
-- Original Ab Initio pattern:
--   JobMonitor.monitor_jobs() polled AutoSys REST API every 60s,
--   sent Slack alerts on FAILURE status.
--
-- Databricks equivalent:
--   SQL Alert queries the system.workflow.job_run_timeline table
--   and fires on any failed runs within the lookback window.
--
-- Schedule: Every 5 minutes
-- Trigger condition: Query returns > 0 rows

SELECT
    job_id,
    run_id,
    job_name,
    task_key,
    result_state,
    CAST(end_time AS TIMESTAMP) AS failed_at,
    ROUND((end_time - start_time) / 1000 / 60, 1) AS duration_minutes,
    error_message
FROM system.workflow.job_run_timeline
WHERE result_state = 'FAILED'
  AND end_time >= date_sub(current_timestamp(), INTERVAL 15 MINUTE)
  AND (
    -- Monitor migrated Ab Initio pipelines specifically
    job_name LIKE '%orders%'
    OR job_name LIKE '%customer_cdc%'
    OR tags['source'] = 'abinitio-migration'
  )
ORDER BY end_time DESC;

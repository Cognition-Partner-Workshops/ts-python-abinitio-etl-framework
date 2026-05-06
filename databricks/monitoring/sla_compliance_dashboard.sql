-- Databricks SQL Dashboard Query: SLA Compliance Reporting
-- Migrated from: monitoring/sla_tracker.py (90-day SLA compliance tracking)
--
-- Original Ab Initio pattern:
--   SLATracker.generate_report() computed SLA compliance % over rolling 7/90 days,
--   tracked which jobs met their completion deadlines.
--
-- Databricks equivalent:
--   Query system tables for job run history and compare against SLA targets.
--   Dashboard widget visualizes compliance trends.

-- ============================================================
-- Panel 1: SLA Compliance Summary (Last 7 Days)
-- ============================================================
WITH sla_targets AS (
    -- SLA definitions (migrated from Ab Initio job registration)
    SELECT 'daily_orders_pipeline'  AS job_name, '06:00' AS sla_deadline_utc, 'high' AS criticality UNION ALL
    SELECT 'customer_cdc_pipeline'  AS job_name, NULL    AS sla_deadline_utc, 'high' AS criticality
),
recent_runs AS (
    SELECT
        j.job_name,
        j.run_id,
        j.result_state,
        CAST(j.start_time AS TIMESTAMP) AS started_at,
        CAST(j.end_time AS TIMESTAMP) AS completed_at,
        ROUND((j.end_time - j.start_time) / 1000 / 60, 1) AS duration_minutes,
        DATE(j.start_time) AS run_date
    FROM system.workflow.job_run_timeline j
    WHERE j.end_time >= date_sub(current_timestamp(), INTERVAL 7 DAY)
      AND j.result_state IN ('SUCCESS', 'FAILED', 'TIMED_OUT')
      AND (
        j.job_name LIKE '%orders%'
        OR j.job_name LIKE '%customer_cdc%'
      )
)
SELECT
    r.job_name,
    t.criticality,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(
        SUM(CASE WHEN r.result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS sla_compliance_pct,
    ROUND(AVG(r.duration_minutes), 1) AS avg_duration_min,
    MAX(r.duration_minutes) AS max_duration_min
FROM recent_runs r
LEFT JOIN sla_targets t ON r.job_name = t.job_name
GROUP BY r.job_name, t.criticality
ORDER BY sla_compliance_pct ASC;

-- ============================================================
-- Panel 2: Daily SLA Trend (Last 90 Days)
-- ============================================================
-- SELECT
--     DATE(end_time) AS run_date,
--     job_name,
--     COUNT(*) AS runs,
--     SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
--     ROUND(
--         SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
--         1
--     ) AS daily_sla_pct
-- FROM system.workflow.job_run_timeline
-- WHERE end_time >= date_sub(current_timestamp(), INTERVAL 90 DAY)
--   AND (job_name LIKE '%orders%' OR job_name LIKE '%customer_cdc%')
--   AND result_state IN ('SUCCESS', 'FAILED', 'TIMED_OUT')
-- GROUP BY DATE(end_time), job_name
-- ORDER BY run_date DESC, job_name;

-- ============================================================
-- Panel 3: Long-Running Job Detection
-- ============================================================
-- (equivalent to Ab Initio SLATracker duration tracking)
-- SELECT
--     job_name,
--     run_id,
--     ROUND((end_time - start_time) / 1000 / 60, 1) AS duration_minutes,
--     result_state,
--     CAST(start_time AS TIMESTAMP) AS started_at
-- FROM system.workflow.job_run_timeline
-- WHERE (end_time - start_time) / 1000 / 60 > 60  -- runs exceeding 60 minutes
--   AND end_time >= date_sub(current_timestamp(), INTERVAL 7 DAY)
--   AND (job_name LIKE '%orders%' OR job_name LIKE '%customer_cdc%')
-- ORDER BY duration_minutes DESC
-- LIMIT 20;

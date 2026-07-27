-- Schedules the GDELT incremental loader as an SPCS job, run every 15 minutes.
-- Run after: sql/setup_snowflake.sql, scripts/create_gdelt_tables.py, and
-- pushing the image + uploading spcs/job_spec.yaml (scripts/deploy.sh).
--
-- Each job run advances from the gdelt_watermark channel offset token:
--   cold start (empty) → ingest only the latest ~15-minute window
--   otherwise          → watermark + 15 minutes (catch-up capped)
-- Watermark is set to each completed window's YYYYMMDDHHMMSS.
-- Tracking progress on the streaming channel offset token avoids warehouse use.
--
-- This is a *serverless* task (no WAREHOUSE=): Snowflake-managed compute runs
-- the DROP + EXECUTE JOB SERVICE statements. Prefer ASYNC=TRUE so the task
-- finishes as soon as the job is accepted, instead of holding compute for the
-- whole container lifetime.
--
-- Named jobs linger after DONE and block the next EXECUTE with
-- "Object already exists". DROP SERVICE IF EXISTS at the start of each run
-- clears the prior job name. Safe at a 15-minute cadence: prior runs finish
-- well under 15 minutes (a DROP of a still-RUNNING job would cancel it).
--
-- Role needs EXECUTE MANAGED TASK (granted in sql/setup_snowflake.sql).

USE ROLE ACCOUNTADMIN;
USE DATABASE SC_DB;
USE SCHEMA GDELT;

CREATE OR REPLACE TASK GDELT_INCREMENTAL_TASK
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '15 MINUTE'
  COMMENT = 'Serverless task: pull newest GDELT 15-min trio via SPCS job + Snowpipe Streaming'
  AS
  BEGIN
    DROP SERVICE IF EXISTS SC_DB.GDELT.GDELT_INCREMENTAL_JOB;
    EXECUTE JOB SERVICE
      IN COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU
      FROM @SC_DB.GDELT.GDELT_STAGE
      SPECIFICATION_FILE = 'job_spec.rendered.yaml'
      EXTERNAL_ACCESS_INTEGRATIONS = (gdelt_public_access)
      NAME = 'SC_DB.GDELT.GDELT_INCREMENTAL_JOB'
      ASYNC = TRUE;
  END;

ALTER TASK GDELT_INCREMENTAL_TASK RESUME;

-- Useful commands:
--   EXECUTE TASK GDELT_INCREMENTAL_TASK;                          -- run immediately, out of schedule
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'GDELT_INCREMENTAL_TASK')) ORDER BY SCHEDULED_TIME DESC;
--   CALL SYSTEM$GET_SERVICE_LOGS('SC_DB.GDELT.GDELT_INCREMENTAL_JOB', 0, 'gdelt-incremental-job');
--   ALTER TASK GDELT_INCREMENTAL_TASK SUSPEND;                    -- pause the schedule

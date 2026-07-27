-- One-time Snowflake infrastructure for the GDELT incremental SPCS job loader.
-- Run as ACCOUNTADMIN (or a role with equivalent privileges). Replace the
-- placeholders (<...>) before running, or use scripts/render_sql.py
-- which renders this file from your .env.
--
-- All GDELT objects (tables, pipes, watermark streaming sink, compute pool,
-- image repository, task) live in the dedicated SC_DB.GDELT schema.
-- SPCS jobs use compute pool GDELT_INCREMENTAL_POOL (default GDELT_INCREMENTAL_POOL,
-- CPU_X64_XS). Progress is the gdelt_watermark channel offset token
-- (watermark + 15m); create tables/pipes with scripts/create_gdelt_tables.py.
--
-- Unlike a long-running streaming service, this job container authenticates
-- to Snowflake with its own SPCS session token (AUTHORIZATION_TYPE=SPCS) --
-- no private key / secret is required for the running job.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS SC_DB;
CREATE SCHEMA IF NOT EXISTS SC_DB.GDELT;

CREATE ROLE IF NOT EXISTS ACCOUNTADMIN;

-- Optional: used only by laptop setup scripts (create_gdelt_tables.py), not by
-- the SPCS job. Watermark progress is a Snowpipe Streaming channel offset token.
CREATE WAREHOUSE IF NOT EXISTS XSMALL_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Optional: local setup scripts only (not used by GDELT SPCS job)';

CREATE COMPUTE POOL IF NOT EXISTS GDELT_INCREMENTAL_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_RESUME = TRUE
  AUTO_SUSPEND_SECS = 300
  COMMENT = 'GDELT incremental loader job (runs every 15 minutes)';

CREATE IMAGE REPOSITORY IF NOT EXISTS SC_DB.GDELT.GDELT_LOADER_REPO;

CREATE STAGE IF NOT EXISTS SC_DB.GDELT.GDELT_STAGE
  DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE NETWORK RULE gdelt_data_access
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('data.gdeltproject.org:80', 'data.gdeltproject.org:443');

CREATE OR REPLACE NETWORK RULE snowflake_ingest_access
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = (
    'SFPRODUCTSTRATEGY-SC_ZPXQFMGOHT.snowflakecomputing.com:443',
    '*.ingest.prod3.snowflakecomputing.com:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION gdelt_public_access
  ALLOWED_NETWORK_RULES = (gdelt_data_access, snowflake_ingest_access)
  ENABLED = TRUE
  COMMENT = 'Outbound access to GDELT public files and the Snowflake ingest API';

GRANT USAGE ON DATABASE SC_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA SC_DB.GDELT TO ROLE ACCOUNTADMIN;
GRANT CREATE TABLE, CREATE PIPE, CREATE STAGE, CREATE IMAGE REPOSITORY, CREATE SERVICE, CREATE TASK
  ON SCHEMA SC_DB.GDELT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE XSMALL_WH TO ROLE ACCOUNTADMIN;
GRANT USAGE, MONITOR ON COMPUTE POOL GDELT_INCREMENTAL_POOL TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION gdelt_public_access TO ROLE ACCOUNTADMIN;
GRANT READ, WRITE ON IMAGE REPOSITORY SC_DB.GDELT.GDELT_LOADER_REPO TO ROLE ACCOUNTADMIN;
GRANT READ, WRITE ON STAGE SC_DB.GDELT.GDELT_STAGE TO ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

GRANT ROLE ACCOUNTADMIN TO USER SC_ADMINUSER;

-- If the account has a restrictive NETWORK POLICY, allow this compute pool to
-- call Snowflake APIs (Snowpipe Streaming ingest) from SPCS:
--
--   CREATE NETWORK RULE IF NOT EXISTS SC_DB.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS
--     TYPE = COMPUTE_POOL MODE = INGRESS
--     VALUE_LIST = ('GDELT_INCREMENTAL_POOL');
--   ALTER NETWORK POLICY "<policy_name>"
--     ADD ALLOWED_NETWORK_RULE_LIST = ('SC_DB.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS');
--
-- Run scripts/create_gdelt_tables.py next to create EVENTS/EVENTMENTIONS/GKG,
-- their Snowpipe Streaming pipes, and the GDELT_WATERMARK streaming sink
-- (progress is a standard-channel offset token — avoids warehouse use).
--
-- After pushing the container image and uploading spcs/job_spec.yaml to the
-- stage, run sql/create_task.sql to schedule the job every 15 minutes.

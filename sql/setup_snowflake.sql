-- One-time Snowflake infrastructure for the GDELT incremental SPCS job loader.
-- Run as ACCOUNTADMIN (or a role with equivalent privileges). Replace the
-- placeholders (<...>) before running, or use scripts/render_sql.py
-- which renders this file from your .env.
--
-- All GDELT objects (tables, pipes, watermark streaming sink, compute pool,
-- image repository, task) live in the dedicated <database>.GDELT schema.
--
-- Unlike a long-running streaming service, this job container authenticates
-- to Snowflake with its own SPCS session token (AUTHORIZATION_TYPE=SPCS) --
-- no private key / secret is required for the running job.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS <database>;
CREATE SCHEMA IF NOT EXISTS <database>.GDELT;

CREATE ROLE IF NOT EXISTS <role>;

-- Optional: used only by laptop setup scripts (create_gdelt_tables.py), not by
-- the SPCS job. Watermark progress is a Snowpipe Streaming channel offset token.
CREATE WAREHOUSE IF NOT EXISTS <warehouse>
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Optional: local setup scripts only (not used by GDELT SPCS job)';

CREATE COMPUTE POOL IF NOT EXISTS <compute_pool>
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_RESUME = TRUE
  AUTO_SUSPEND_SECS = 300
  COMMENT = 'GDELT incremental loader job (runs every 15 minutes)';

CREATE IMAGE REPOSITORY IF NOT EXISTS <database>.GDELT.<image_repo>;

CREATE STAGE IF NOT EXISTS <database>.GDELT.<stage>
  DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE NETWORK RULE gdelt_data_access
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('data.gdeltproject.org:80', 'data.gdeltproject.org:443');

CREATE OR REPLACE NETWORK RULE snowflake_ingest_access
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = (
    '<account_host>.snowflakecomputing.com:443',
    '*.ingest.<deployment_id>.snowflakecomputing.com:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION gdelt_public_access
  ALLOWED_NETWORK_RULES = (gdelt_data_access, snowflake_ingest_access)
  ENABLED = TRUE
  COMMENT = 'Outbound access to GDELT public files and the Snowflake ingest API';

GRANT USAGE ON DATABASE <database> TO ROLE <role>;
GRANT USAGE ON SCHEMA <database>.GDELT TO ROLE <role>;
GRANT CREATE TABLE, CREATE PIPE, CREATE STAGE, CREATE IMAGE REPOSITORY, CREATE SERVICE, CREATE TASK
  ON SCHEMA <database>.GDELT TO ROLE <role>;
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>;
GRANT USAGE, MONITOR ON COMPUTE POOL <compute_pool> TO ROLE <role>;
GRANT USAGE ON INTEGRATION gdelt_public_access TO ROLE <role>;
GRANT READ, WRITE ON IMAGE REPOSITORY <database>.GDELT.<image_repo> TO ROLE <role>;
GRANT READ, WRITE ON STAGE <database>.GDELT.<stage> TO ROLE <role>;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <role>;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE <role>;

GRANT ROLE <role> TO USER <user>;

-- If the account has a restrictive NETWORK POLICY, allow this compute pool to
-- call Snowflake APIs (Snowpipe Streaming ingest) from SPCS. Same pattern as
-- streaming_gdelt's _ensure_spcs_network_policy:
--
--   CREATE NETWORK RULE IF NOT EXISTS <database>.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS
--     TYPE = COMPUTE_POOL MODE = INGRESS
--     VALUE_LIST = ('<compute_pool>');
--   ALTER NETWORK POLICY "<policy_name>"
--     ADD ALLOWED_NETWORK_RULE_LIST = ('<database>.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS');
--
-- Run scripts/create_gdelt_tables.py next to create EVENTS/EVENTMENTIONS/GKG,
-- their Snowpipe Streaming pipes, and the GDELT_WATERMARK streaming sink
-- (progress is a standard-channel offset token, not warehouse SQL).
--
-- After pushing the container image and uploading spcs/job_spec.yaml to the
-- stage, run sql/create_task.sql to schedule the job every 15 minutes.

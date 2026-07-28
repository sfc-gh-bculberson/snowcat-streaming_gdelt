#!/usr/bin/env bash
# Build/push the GDELT incremental SPCS job image, stage the job spec, ensure
# compute pool + ingress, and (re)create the serverless EXECUTE JOB SERVICE task.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env — copy .env.example and fill in Snowflake settings." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${SNOWFLAKE_DATABASE:?Set SNOWFLAKE_DATABASE in .env}"
: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT in .env}"
: "${SNOWFLAKE_USER:?Set SNOWFLAKE_USER in .env}"
: "${SNOWFLAKE_PRIVATE_KEY:?Set SNOWFLAKE_PRIVATE_KEY in .env}"

SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-GDELT}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-XSMALL_WH}"
SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-ACCOUNTADMIN}"
SPCS_COMPUTE_POOL="${SPCS_COMPUTE_POOL:-GDELT_INCREMENTAL_POOL}"
SPCS_IMAGE_REPO="${SPCS_IMAGE_REPO:-GDELT_LOADER_REPO}"
SPCS_STAGE="${SPCS_STAGE:-GDELT_STAGE}"
SPCS_JOB_NAME="${SPCS_JOB_NAME:-GDELT_INCREMENTAL_JOB}"
SPCS_INSTANCE_FAMILY="${SPCS_INSTANCE_FAMILY:-CPU_X64_XS}"

# macOS ships Bash 3.2 (no ${var,,}); lowercase via tr.
account_lc="$(printf '%s' "${SNOWFLAKE_ACCOUNT}" | tr '[:upper:]' '[:lower:]')"
database_lc="$(printf '%s' "${SNOWFLAKE_DATABASE}" | tr '[:upper:]' '[:lower:]')"
schema_lc="$(printf '%s' "${SNOWFLAKE_SCHEMA}" | tr '[:upper:]' '[:lower:]')"
repo_lc="$(printf '%s' "${SPCS_IMAGE_REPO}" | tr '[:upper:]' '[:lower:]')"

# Registry host uses hyphens where the account locator has underscores.
REGISTRY_HOST="${account_lc//_/-}.registry.snowflakecomputing.com"
IMAGE_FQN="${REGISTRY_HOST}/${database_lc}/${schema_lc}/${repo_lc}/gdelt-incremental:latest"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# CPU_X64_* compute pools need amd64; GEN_ARM_* would use linux/arm64.
DOCKER_PLATFORM="${SPCS_DOCKER_PLATFORM:-linux/amd64}"

log "Rendering SQL templates ..."
"${ROOT}/.venv/bin/python" scripts/render_sql.py

log "Building Docker image (${DOCKER_PLATFORM}) ..."
docker build --platform "${DOCKER_PLATFORM}" -t gdelt-incremental:latest .

log "Tagging image as ${IMAGE_FQN}"
docker tag gdelt-incremental:latest "${IMAGE_FQN}"

# Write PEM to a temp file for snow CLI / docker login (never leave it behind).
PEM_FILE="$(mktemp)"
cleanup() { rm -f "${PEM_FILE}"; }
trap cleanup EXIT
"${ROOT}/.venv/bin/python" - <<'PY' >"${PEM_FILE}"
import os
print(os.environ["SNOWFLAKE_PRIVATE_KEY"].replace("\\n", "\n").strip())
PY

# Account locator for snow CLI: prefer host derived from SNOWFLAKE_URL.
ACCOUNT_FOR_CLI="${SNOWFLAKE_ACCOUNT}"
if [[ -n "${SNOWFLAKE_URL:-}" ]]; then
  host="${SNOWFLAKE_URL#https://}"
  host="${host#http://}"
  host="${host%%/*}"
  if [[ "${host}" == *.snowflakecomputing.com ]]; then
    ACCOUNT_FOR_CLI="${host%.snowflakecomputing.com}"
  fi
fi

log "Logging in to Snowflake image registry (${REGISTRY_HOST}) ..."
snow spcs image-registry login \
  --account "${ACCOUNT_FOR_CLI}" \
  --user "${SNOWFLAKE_USER}" \
  --authenticator SNOWFLAKE_JWT \
  --private-key-file "${PEM_FILE}" \
  --role "${SNOWFLAKE_ROLE}" \
  --warehouse "${SNOWFLAKE_WAREHOUSE}" \
  --database "${SNOWFLAKE_DATABASE}" \
  --schema "${SNOWFLAKE_SCHEMA}"

log "Pushing image ..."
docker push "${IMAGE_FQN}"

log "Rendering job spec ..."
export ROOT SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_ROLE SPCS_IMAGE_REPO
"${ROOT}/.venv/bin/python" - <<'PY'
import os
from pathlib import Path

repo = Path(os.environ["ROOT"])
template = (repo / "spcs/job_spec.yaml").read_text()
rendered = (
    template.replace("{{ database }}", os.environ["SNOWFLAKE_DATABASE"])
    .replace("{{ role }}", os.environ["SNOWFLAKE_ROLE"])
    .replace("{{ image_repo }}", os.environ["SPCS_IMAGE_REPO"])
)
out = repo / "spcs/job_spec.rendered.yaml"
out.write_text(rendered)
print(f"Wrote {out}")
PY

log "Ensuring compute pool, ingress, stage PUT, and SPCS task ..."
export SPCS_COMPUTE_POOL SPCS_STAGE SPCS_JOB_NAME SPCS_INSTANCE_FAMILY SPCS_IMAGE_REPO
"${ROOT}/.venv/bin/python" - <<'PY'
import os
from pathlib import Path
from cryptography.hazmat.primitives import serialization
import snowflake.connector

repo = Path(".").resolve()
account = os.environ["SNOWFLAKE_ACCOUNT"]
url = os.environ.get("SNOWFLAKE_URL", "").strip()
if url:
    host = url.replace("https://", "").replace("http://", "").split("/")[0]
    if host.endswith(".snowflakecomputing.com"):
        account = host[: -len(".snowflakecomputing.com")]

pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].replace("\\n", "\n").strip()
pk = serialization.load_pem_private_key(pem.encode(), password=None)
conn = snowflake.connector.connect(
    account=account,
    user=os.environ["SNOWFLAKE_USER"],
    private_key=pk,
    role=os.environ.get("SNOWFLAKE_ROLE"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "GDELT"),
)

db = os.environ["SNOWFLAKE_DATABASE"]
sch = os.environ.get("SNOWFLAKE_SCHEMA", "GDELT")
stage = os.environ.get("SPCS_STAGE", "GDELT_STAGE")
pool = os.environ.get("SPCS_COMPUTE_POOL", "GDELT_INCREMENTAL_POOL")
job = os.environ.get("SPCS_JOB_NAME", "GDELT_INCREMENTAL_JOB")
family = os.environ.get("SPCS_INSTANCE_FAMILY", "CPU_X64_XS")
image_repo = os.environ.get("SPCS_IMAGE_REPO", "GDELT_LOADER_REPO")
role = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
spec = repo / "spcs" / "job_spec.rendered.yaml"

with conn.cursor() as cur:
    cur.execute(
        f"""
        CREATE COMPUTE POOL IF NOT EXISTS {pool}
          MIN_NODES = 1
          MAX_NODES = 1
          INSTANCE_FAMILY = {family}
          AUTO_RESUME = TRUE
          AUTO_SUSPEND_SECS = 300
          COMMENT = 'GDELT incremental loader job (runs every 15 minutes)'
        """
    )
    print("compute pool:", cur.fetchone())
    try:
        cur.execute(f"ALTER COMPUTE POOL {pool} RESUME")
        print("resume pool:", cur.fetchone())
    except Exception as exc:
        print("resume pool:", exc)

    cur.execute(f"CREATE IMAGE REPOSITORY IF NOT EXISTS {db}.{sch}.{image_repo}")
    print("image repo:", cur.fetchone())
    cur.execute(f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{stage} DIRECTORY = (ENABLE = TRUE)")
    print("stage:", cur.fetchone())

    cur.execute(
        f"""
        CREATE NETWORK RULE IF NOT EXISTS SC_DB.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS
          TYPE = COMPUTE_POOL
          MODE = INGRESS
          VALUE_LIST = ('{pool}')
          COMMENT = 'Allow Snowpipe Streaming from GDELT SPCS job pool'
        """
    )
    print("ingress create:", cur.fetchone())
    cur.execute(
        f"""
        ALTER NETWORK RULE SC_DB.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS
          SET VALUE_LIST = ('{pool}')
        """
    )
    print("ingress set:", cur.fetchone())
    for policy in ('"limited_policy"', '"user_policy"'):
        try:
            cur.execute(
                f"""
                ALTER NETWORK POLICY {policy}
                  ADD ALLOWED_NETWORK_RULE_LIST = ('SC_DB.SC_SCHEMA.GDELT_LOADER_POOL_INGRESS')
                """
            )
            print("policy", policy, cur.fetchone())
        except Exception as exc:
            print("policy", policy, exc)

    cur.execute(f"GRANT USAGE, MONITOR ON COMPUTE POOL {pool} TO ROLE {role}")
    cur.execute(f"GRANT CREATE SERVICE ON SCHEMA {db}.{sch} TO ROLE {role}")

    put = f"PUT file://{spec} @{db}.{sch}.{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    print(put)
    cur.execute(put)
    print("put:", cur.fetchall())

    try:
        cur.execute(f"ALTER TASK IF EXISTS {db}.{sch}.GDELT_INCREMENTAL_TASK SUSPEND")
        cur.execute(f"DROP TASK IF EXISTS {db}.{sch}.GDELT_INCREMENTAL_TASK")
        print("dropped prior task")
    except Exception as exc:
        print("drop prior task:", exc)

    try:
        cur.execute(f"DROP SERVICE IF EXISTS {db}.{sch}.{job}")
        print("dropped prior job:", cur.fetchone())
    except Exception as exc:
        print("drop prior job:", exc)

    create_task = f"""
    CREATE OR REPLACE TASK {db}.{sch}.GDELT_INCREMENTAL_TASK
      USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
      SCHEDULE = '15 MINUTE'
      COMMENT = 'Serverless task: pull newest GDELT 15-min trio via SPCS job + Snowpipe Streaming'
      AS
      BEGIN
        DROP SERVICE IF EXISTS {db}.{sch}.{job};
        EXECUTE JOB SERVICE
          IN COMPUTE POOL {pool}
          FROM @{db}.{sch}.{stage}
          SPECIFICATION_FILE = 'job_spec.rendered.yaml'
          EXTERNAL_ACCESS_INTEGRATIONS = (gdelt_public_access)
          NAME = '{db}.{sch}.{job}'
          ASYNC = TRUE;
      END
    """
    cur.execute(create_task)
    print("create task:", cur.fetchone())
    cur.execute(f"ALTER TASK {db}.{sch}.GDELT_INCREMENTAL_TASK RESUME")
    print("resume task:", cur.fetchone())
    cur.execute(f"EXECUTE TASK {db}.{sch}.GDELT_INCREMENTAL_TASK")
    print("execute task:", cur.fetchone())

conn.close()
print("SPCS job deploy complete.")
PY

log "Done. Monitor with TASK_HISTORY / SYSTEM\$GET_SERVICE_STATUS / GET_SERVICE_LOGS."

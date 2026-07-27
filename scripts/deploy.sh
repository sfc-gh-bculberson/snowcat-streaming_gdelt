#!/usr/bin/env bash
# Build, push, and stage the GDELT incremental job image + spec.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env — copy .env.example and fill in Snowflake settings." >&2
  exit 1
fi

set -a
source .env
set +a

: "${SNOWFLAKE_DATABASE:?Set SNOWFLAKE_DATABASE in .env}"
: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT in .env}"

SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-GDELT}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-GDELT_LOADER_WH}"
SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-GDELT_LOADER_ROLE}"
SPCS_COMPUTE_POOL="${SPCS_COMPUTE_POOL:-SYSTEM_COMPUTE_POOL_CPU}"
SPCS_IMAGE_REPO="${SPCS_IMAGE_REPO:-GDELT_LOADER_REPO}"
SPCS_STAGE="${SPCS_STAGE:-GDELT_STAGE}"
SPCS_JOB_NAME="${SPCS_JOB_NAME:-GDELT_INCREMENTAL_JOB}"

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

log "Building Docker image (${DOCKER_PLATFORM}) ..."
docker build --platform "${DOCKER_PLATFORM}" -t gdelt-incremental:latest .

log "Tagging image as ${IMAGE_FQN}"
docker tag gdelt-incremental:latest "${IMAGE_FQN}"

log "Logging in to Snowflake image registry (${REGISTRY_HOST}) ..."
docker login "${REGISTRY_HOST}"

log "Pushing image ..."
docker push "${IMAGE_FQN}"

log "Rendering job spec ..."
export ROOT SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_ROLE SPCS_IMAGE_REPO
python3 - <<'PY'
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

log "Create stage and upload spec, then schedule the task, with Snowflake CLI or SQL:"
cat <<EOF

  CREATE STAGE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SPCS_STAGE}
    DIRECTORY = (ENABLE = TRUE);

  PUT file://${ROOT}/spcs/job_spec.rendered.yaml
    @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SPCS_STAGE}
    AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

Then run sql/create_task.sql (with placeholders substituted, e.g. via
scripts/render_sql.py) to schedule ${SPCS_JOB_NAME} every 15 minutes.

  The job advances watermark+15m (cold start = latest ~15-minute window only).

  To trigger one run immediately without waiting for the schedule:
    EXECUTE TASK ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.GDELT_INCREMENTAL_TASK;

EOF

log "Deployment artifacts ready. See output above for stage PUT + task steps."

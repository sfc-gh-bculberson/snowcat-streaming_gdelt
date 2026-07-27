"""Environment-variable-driven configuration.

Auth pattern mirrors ../streaming_gdelt/gdelt/config.py: inside an SPCS job the
container automatically gets SNOWFLAKE_ACCOUNT / SNOWFLAKE_HOST env vars and an
OAuth token mounted at /snowflake/session/token, so no secret or private key is
needed in production. Locally (or for one-time setup scripts) key-pair (JWT) or
password auth via .env is used instead.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

from gdelt_incremental.schemas import DEFAULT_TABLE_NAMES, PIPE_ENV_KEYS, TABLE_ENV_KEYS, TableKind

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "GDELT")
SNOWFLAKE_CLIENT_NAME = os.getenv("SNOWFLAKE_CLIENT_NAME", "gdelt-incremental")
# Optional: only used by one-shot setup scripts (create tables), not by the job.
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_PRIVATE_KEY = (
    os.getenv("SNOWFLAKE_PRIVATE_KEY", "").strip().strip('"').replace("\\n", "\n")
)
SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL", "")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST", "").strip()
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "").strip()
AUTHORIZATION_TYPE = os.getenv("AUTHORIZATION_TYPE", "").strip().upper()
SPCS_TOKEN_PATH = os.getenv("SPCS_TOKEN_PATH", "/snowflake/session/token")

GDELT_BASE_URL = os.getenv("GDELT_BASE_URL", "http://data.gdeltproject.org/gdeltv2")
LASTUPDATE_URL = os.getenv("LASTUPDATE_URL", f"{GDELT_BASE_URL}/lastupdate.txt")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/gdelt"))
MAX_CATCHUP_WINDOWS = int(os.getenv("MAX_CATCHUP_WINDOWS", "8"))

APPEND_ROWS_MAX_RETRIES = int(os.getenv("APPEND_ROWS_MAX_RETRIES", "5"))
APPEND_ROWS_BACKOFF_BASE_SEC = float(os.getenv("APPEND_ROWS_BACKOFF_BASE_SEC", "0.5"))
APPEND_ROWS_BACKOFF_MAX_SEC = float(os.getenv("APPEND_ROWS_BACKOFF_MAX_SEC", "30.0"))

# Streaming sink for watermark rows (pipe target only). Progress is the
# standard-channel offset token, not a SQL SELECT against this table.
WATERMARK_TABLE = os.getenv("SNOWFLAKE_WATERMARK_TABLE", "GDELT_WATERMARK")
WATERMARK_PIPE = os.getenv("SNOWFLAKE_PIPE_WATERMARK", "")
WATERMARK_CHANNEL_NAME = os.getenv("SNOWFLAKE_WATERMARK_CHANNEL", "gdelt_watermark")
WATERMARK_COMMIT_TIMEOUT_SEC = int(os.getenv("WATERMARK_COMMIT_TIMEOUT_SEC", "120"))

REQUIRED_ENV_VARS = (
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_ACCOUNT",
)


def table_name(table: TableKind) -> str:
    env_key = TABLE_ENV_KEYS[table]
    return os.getenv(env_key, DEFAULT_TABLE_NAMES[table])


def pipe_name(table: TableKind) -> str:
    env_key = PIPE_ENV_KEYS[table]
    explicit = os.getenv(env_key, "").strip()
    if explicit:
        return explicit
    return f"{table_name(table)}_MATCH_BY_COLUMN"


def watermark_pipe_name() -> str:
    if WATERMARK_PIPE:
        return WATERMARK_PIPE
    return f"{WATERMARK_TABLE}_MATCH_BY_COLUMN"


def uses_spcs_auth() -> bool:
    if AUTHORIZATION_TYPE in ("JWT", "PASSWORD"):
        return False
    if AUTHORIZATION_TYPE == "SPCS":
        return True
    return Path(SPCS_TOKEN_PATH).is_file()


def uses_jwt_auth() -> bool:
    if AUTHORIZATION_TYPE == "JWT":
        return True
    if uses_spcs_auth():
        return False
    return bool(SNOWFLAKE_PRIVATE_KEY)


def read_spcs_token() -> str:
    return Path(SPCS_TOKEN_PATH).read_text().strip()


def _snowflake_url() -> str:
    if SNOWFLAKE_URL:
        return SNOWFLAKE_URL
    if SNOWFLAKE_HOST:
        return SNOWFLAKE_HOST if SNOWFLAKE_HOST.startswith("http") else f"https://{SNOWFLAKE_HOST}"
    return ""


def snowflake_properties() -> Dict[str, str]:
    """Properties dict for snowflake.ingest.streaming.StreamingIngestClient."""
    if uses_spcs_auth():
        props = {
            "authorization_type": "SPCS",
            "account": SNOWFLAKE_ACCOUNT,
            "url": _snowflake_url(),
            "spcs_token_path": SPCS_TOKEN_PATH,
        }
        if SNOWFLAKE_ROLE:
            props["role"] = SNOWFLAKE_ROLE
        return props

    props = {
        "authorization_type": "JWT",
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "private_key": SNOWFLAKE_PRIVATE_KEY,
        "url": _snowflake_url(),
    }
    if SNOWFLAKE_ROLE:
        props["role"] = SNOWFLAKE_ROLE
    return props


def validate() -> None:
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if not _snowflake_url():
        missing.append("SNOWFLAKE_URL or SNOWFLAKE_HOST")
    if uses_spcs_auth():
        if not SNOWFLAKE_ROLE:
            missing.append("SNOWFLAKE_ROLE")
    else:
        if not SNOWFLAKE_USER:
            missing.append("SNOWFLAKE_USER")
        if not SNOWFLAKE_PRIVATE_KEY and not SNOWFLAKE_PASSWORD:
            missing.append("SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PASSWORD")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

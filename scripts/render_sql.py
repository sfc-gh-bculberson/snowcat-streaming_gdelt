#!/usr/bin/env python3
"""Render sql/*.sql templates by substituting <placeholders> with .env values.

Usage:
    set -a && source .env && set +a
    python scripts/render_sql.py
    # -> writes sql/setup_snowflake.rendered.sql and sql/create_task.rendered.sql
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _account_host() -> str:
    url = os.getenv("SNOWFLAKE_URL", "").strip()
    if url:
        host = url.replace("https://", "").replace("http://", "").split("/")[0]
        if host.endswith(".snowflakecomputing.com"):
            return host[: -len(".snowflakecomputing.com")]
    return _require("SNOWFLAKE_ACCOUNT")


PLACEHOLDERS = {
    "<database>": lambda: _require("SNOWFLAKE_DATABASE"),
    "<role>": lambda: os.getenv("SNOWFLAKE_ROLE", "GDELT_LOADER_ROLE"),
    "<user>": lambda: os.getenv("SNOWFLAKE_USER", ""),
    "<warehouse>": lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "GDELT_LOADER_WH"),
    "<compute_pool>": lambda: os.getenv("SPCS_COMPUTE_POOL", "GDELT_INCREMENTAL_POOL"),
    "<image_repo>": lambda: os.getenv("SPCS_IMAGE_REPO", "GDELT_LOADER_REPO"),
    "<stage>": lambda: os.getenv("SPCS_STAGE", "GDELT_STAGE"),
    "<job_name>": lambda: os.getenv("SPCS_JOB_NAME", "GDELT_INCREMENTAL_JOB"),
    "<account_host>": _account_host,
    "<deployment_id>": lambda: os.getenv("SNOWFLAKE_DEPLOYMENT_ID", "prod3"),
}


def render(path: Path) -> Path:
    text = path.read_text()
    for placeholder, resolver in PLACEHOLDERS.items():
        text = text.replace(placeholder, resolver())
    out = path.with_suffix(".rendered.sql")
    out.write_text(text)
    return out


def main() -> None:
    for name in ("setup_snowflake.sql", "create_task.sql"):
        out = render(REPO_ROOT / "sql" / name)
        print(f"Wrote {out}")
    print(
        "\nReview the rendered files (especially <account_host>/<deployment_id> "
        "in setup_snowflake.rendered.sql) before running them."
    )


if __name__ == "__main__":
    try:
        main()
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        raise

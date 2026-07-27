#!/usr/bin/env python3
"""Create GDELT tables, Snowpipe Streaming pipes, and the watermark streaming
sink (table + pipe). Watermark *progress* is a standard-channel offset token;
the sink table only exists as the pipe COPY target.

Run once from a laptop (key-pair / password auth) after sql/setup_snowflake.sql.
If an older SQL-state GDELT_WATERMARK row exists, this script seeds the channel
offset token from it so the next job run continues without re-ingest.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import snowflake.connector
from cryptography.hazmat.primitives import serialization

from gdelt_incremental import config, watermark as wm
from gdelt_incremental.config import (
    SNOWFLAKE_SCHEMA,
    WATERMARK_TABLE,
    pipe_name,
    table_name,
    watermark_pipe_name,
)
from gdelt_incremental.schemas import (
    TABLE_ORDER,
    render_pipe_ddl,
    render_table_ddl,
    render_watermark_pipe_ddl,
    render_watermark_table_ddl,
)


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _account_from_env() -> str:
    account = _require("SNOWFLAKE_ACCOUNT")
    url = os.getenv("SNOWFLAKE_URL", "").strip()
    if url:
        host = url.replace("https://", "").replace("http://", "").split("/")[0]
        if host.endswith(".snowflakecomputing.com"):
            from_url = host[: -len(".snowflakecomputing.com")]
            if from_url:
                return from_url
    return account


def _connect() -> snowflake.connector.SnowflakeConnection:
    user = _require("SNOWFLAKE_USER")
    account = _account_from_env()
    kwargs: dict = {"account": account, "user": user}

    role = os.getenv("SNOWFLAKE_ROLE", "").strip()
    if role:
        kwargs["role"] = role

    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
    if warehouse:
        kwargs["warehouse"] = warehouse

    private_key_pem = os.getenv("SNOWFLAKE_PRIVATE_KEY", "").replace("\\n", "\n").strip()
    if private_key_pem:
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"), password=None
        )
        kwargs["private_key"] = private_key
        auth = "key-pair"
    else:
        kwargs["password"] = _require("SNOWFLAKE_PASSWORD")
        auth = "password"

    database = _require("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA).strip() or SNOWFLAKE_SCHEMA
    kwargs["database"] = database
    kwargs["schema"] = schema

    print(f"Connecting to Snowflake as {user} @ {account} ({auth}) ...")
    return snowflake.connector.connect(**kwargs)


def _legacy_sql_watermark(cur, database: str, schema: str) -> str | None:
    """Read LAST_TIMESTAMP from a pre-offset-token watermark table, if present."""
    fqn = f"{database}.{schema}.{WATERMARK_TABLE}"
    try:
        cur.execute(f"DESCRIBE TABLE {fqn}")
        cols = {str(row[0]).upper() for row in cur.fetchall()}
    except snowflake.connector.errors.ProgrammingError:
        return None
    if "LAST_TIMESTAMP" not in cols:
        return None
    if "ID" in cols:
        cur.execute(f"SELECT LAST_TIMESTAMP FROM {fqn} WHERE ID = 1")
    else:
        cur.execute(f"SELECT MAX(LAST_TIMESTAMP) FROM {fqn}")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _seed_offset_token(seed_ts: str) -> None:
    config.validate()
    client = wm.open_client()
    try:
        channel = wm.open_channel(client)
        current = wm.get_watermark(channel)
        if current is not None and current >= seed_ts:
            print(f"Channel offset already at {current} (>= seed {seed_ts}); skip seed.")
            return
        print(f"Seeding watermark channel offset token to {seed_ts} ...")
        wm.set_watermark(channel, seed_ts)
    finally:
        client.close(wait_for_flush=True)


def main() -> None:
    database = _require("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA).strip() or SNOWFLAKE_SCHEMA
    wm_table = WATERMARK_TABLE
    wm_pipe = watermark_pipe_name()
    legacy_ts: str | None = None

    conn = _connect()
    try:
        with conn.cursor() as cur:
            print(f"Creating schema {database}.{schema} ...")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

            for table in TABLE_ORDER:
                tbl_name = table_name(table)
                pipe = pipe_name(table)
                table_fqn = f"{database}.{schema}.{tbl_name}"
                pipe_fqn = f"{database}.{schema}.{pipe}"

                print(f"Creating table {table_fqn} ...")
                cur.execute(render_table_ddl(table, database, schema, tbl_name))

                print(f"Creating pipe {pipe_fqn} ...")
                cur.execute(render_pipe_ddl(table, database, schema, tbl_name, pipe))

            watermark_fqn = f"{database}.{schema}.{wm_table}"
            legacy_ts = _legacy_sql_watermark(cur, database, schema)

            # Recreate as streaming sink (drop old ID/MERGE shape if present).
            print(f"Creating watermark streaming sink {watermark_fqn} ...")
            cur.execute(f"DROP TABLE IF EXISTS {watermark_fqn}")
            cur.execute(render_watermark_table_ddl(database, schema, wm_table))

            pipe_fqn = f"{database}.{schema}.{wm_pipe}"
            print(f"Creating watermark pipe {pipe_fqn} ...")
            cur.execute(f"DROP PIPE IF EXISTS {pipe_fqn}")
            cur.execute(render_watermark_pipe_ddl(database, schema, wm_table, wm_pipe))
    finally:
        conn.close()

    seed = os.getenv("SEED_WATERMARK_TIMESTAMP", "").strip() or legacy_ts
    if seed:
        _seed_offset_token(seed)
    else:
        print(
            "No legacy/SEED_WATERMARK_TIMESTAMP; channel starts empty "
            "(first job run cold-starts from latest window only)."
        )

    print("\nGDELT tables, pipes, and watermark streaming sink are ready.")
    for table in TABLE_ORDER:
        print(f"  {table}: table={table_name(table)} pipe={pipe_name(table)}")
    print(f"  watermark: table={wm_table} pipe={wm_pipe} channel={config.WATERMARK_CHANNEL_NAME}")


if __name__ == "__main__":
    try:
        main()
    except snowflake.connector.errors.Error as exc:
        print(f"Snowflake error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

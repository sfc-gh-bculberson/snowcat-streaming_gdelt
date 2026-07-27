#!/usr/bin/env python3
"""Create (or fully reset) GDELT tables, Snowpipe Streaming pipes, and the
watermark streaming sink (table + pipe). Watermark *progress* is a standard-
channel offset token; the sink table only exists as the pipe COPY target.

Run from a laptop (key-pair / password auth) after sql/setup_snowflake.sql.

By default this DROPs and recreates all objects so fact tables are empty and
streaming channels (including the watermark offset token) are cleared — the
next job run cold-starts with only the latest ~15-minute window.

Set SEED_WATERMARK_TIMESTAMP to force an offset token after recreate (optional).
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

    conn = _connect()
    try:
        with conn.cursor() as cur:
            print(f"Ensuring schema {database}.{schema} ...")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

            for table in TABLE_ORDER:
                tbl_name = table_name(table)
                pipe = pipe_name(table)
                table_fqn = f"{database}.{schema}.{tbl_name}"
                pipe_fqn = f"{database}.{schema}.{pipe}"

                # Drop pipe first so streaming channels (and any offset tokens)
                # are cleared along with the empty recreate.
                print(f"Resetting {pipe_fqn} + {table_fqn} ...")
                cur.execute(f"DROP PIPE IF EXISTS {pipe_fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {table_fqn}")
                cur.execute(render_table_ddl(table, database, schema, tbl_name))
                cur.execute(render_pipe_ddl(table, database, schema, tbl_name, pipe))

            watermark_fqn = f"{database}.{schema}.{wm_table}"
            pipe_fqn = f"{database}.{schema}.{wm_pipe}"
            print(f"Resetting watermark {pipe_fqn} + {watermark_fqn} ...")
            cur.execute(f"DROP PIPE IF EXISTS {pipe_fqn}")
            cur.execute(f"DROP TABLE IF EXISTS {watermark_fqn}")
            cur.execute(render_watermark_table_ddl(database, schema, wm_table))
            cur.execute(render_watermark_pipe_ddl(database, schema, wm_table, wm_pipe))
    finally:
        conn.close()

    seed = os.getenv("SEED_WATERMARK_TIMESTAMP", "").strip()
    if seed:
        _seed_offset_token(seed)
    else:
        print(
            "No SEED_WATERMARK_TIMESTAMP; watermark channel starts empty "
            "(first job run cold-starts from the latest ~15-minute window only)."
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

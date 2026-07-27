#!/usr/bin/env python3
"""Generate an RSA key pair and register the public key on a Snowflake user.

Only needed for LOCAL development / running the setup scripts from your
laptop. The production job running in SPCS authenticates with its own
session token instead (see gdelt_incremental/config.py).
"""

from __future__ import annotations

import argparse
import base64
import os
import re
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import snowflake.connector


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


def generate_key_pair() -> tuple[str, str]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    public_der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_b64 = base64.b64encode(public_der).decode("ascii")
    return private_pem, public_b64


def _env_line_value(private_pem: str) -> str:
    escaped = private_pem.replace("\n", "\\n")
    return f'SNOWFLAKE_PRIVATE_KEY="{escaped}"'


def register_public_key(public_b64: str) -> None:
    user = _require("SNOWFLAKE_USER")
    password = _require("SNOWFLAKE_PASSWORD")
    account = _account_from_env()

    connect_kwargs = {"account": account, "user": user, "password": password}
    role = os.getenv("SNOWFLAKE_ROLE", "").strip()
    if role:
        connect_kwargs["role"] = role

    print(f"Connecting to Snowflake as {user} @ {account} ...")
    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER USER {user} SET RSA_PUBLIC_KEY = %s", (public_b64,))
        print(f"Registered RSA public key on user {user}.")
    finally:
        conn.close()


def write_env(repo_root: Path, private_pem: str) -> None:
    env_path = repo_root / ".env"
    if not env_path.exists():
        raise SystemExit(f"{env_path} not found — copy .env.example first")

    new_line = _env_line_value(private_pem)
    comment = "# PEM on one line (backslash-n for newlines inside the key)"
    text = env_path.read_text()
    if re.search(r"^SNOWFLAKE_PRIVATE_KEY=", text, flags=re.MULTILINE):
        text = re.sub(
            r"^# PEM on one line[^\n]*\n(?:[^\n]*\n)*?"
            r'^SNOWFLAKE_PRIVATE_KEY="(?:\\.|[^"\\])*"\n'
            r"|^SNOWFLAKE_PRIVATE_KEY=.*?\n",
            f"{comment}\n{new_line}\n",
            text,
            count=1,
            flags=re.MULTILINE | re.DOTALL,
        )
    else:
        text = text.rstrip() + f"\n{comment}\n{new_line}\n"
    env_path.write_text(text)
    print(f"Updated {env_path} with SNOWFLAKE_PRIVATE_KEY.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-env",
        action="store_true",
        help="Update .env with SNOWFLAKE_PRIVATE_KEY (default: print only)",
    )
    args = parser.parse_args()

    private_pem, public_b64 = generate_key_pair()
    register_public_key(public_b64)

    if args.write_env:
        repo_root = Path(__file__).resolve().parent.parent
        write_env(repo_root, private_pem)
    else:
        print("\nAdd to .env:\n")
        print(_env_line_value(private_pem))
        print("\nOr re-run with --write-env")


if __name__ == "__main__":
    main()

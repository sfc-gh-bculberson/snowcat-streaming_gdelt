"""Watermark state via a Snowpipe Streaming *standard* channel offset token.

Progress is the channel's ``latest_committed_offset_token`` (a GDELT
``YYYYMMDDHHMMSS`` string). On each completed window we ``append_row`` a tiny
payload with that timestamp as ``offset_token`` and ``wait_for_commit``.

The destination table exists only because a streaming pipe needs a COPY target.
Tracking progress on the channel offset token avoids warehouse use.
"""

from __future__ import annotations

import logging
from typing import Optional

from snowflake.ingest.streaming import StreamingIngestClient
from snowflake.ingest.streaming.streaming_ingest_channel import StreamingIngestChannel

from gdelt_incremental import config

logger = logging.getLogger(__name__)


def open_client() -> StreamingIngestClient:
    return StreamingIngestClient(
        client_name=f"{config.SNOWFLAKE_CLIENT_NAME}-watermark",
        db_name=config.SNOWFLAKE_DATABASE,
        schema_name=config.SNOWFLAKE_SCHEMA,
        pipe_name=config.watermark_pipe_name(),
        properties=config.snowflake_properties(),
    )


def open_channel(client: StreamingIngestClient) -> StreamingIngestChannel:
    channel, status = client.open_channel(config.WATERMARK_CHANNEL_NAME)
    logger.info(
        "Opened watermark channel=%s latest_committed_offset_token=%s status=%s",
        config.WATERMARK_CHANNEL_NAME,
        status.latest_committed_offset_token,
        status.status_code,
    )
    return channel


def get_watermark(channel: StreamingIngestChannel) -> Optional[str]:
    token = channel.get_latest_committed_offset_token()
    if token is None or token == "":
        return None
    return token


def set_watermark(channel: StreamingIngestChannel, timestamp: str) -> None:
    """Persist ``timestamp`` as the committed offset token (and a sink row)."""
    channel.append_row(
        {"LAST_TIMESTAMP": timestamp},
        offset_token=timestamp,
    )
    channel.wait_for_commit(
        lambda committed: committed is not None and committed >= timestamp,
        timeout_seconds=config.WATERMARK_COMMIT_TIMEOUT_SEC,
    )
    logger.info("Watermark offset token committed: %s", timestamp)

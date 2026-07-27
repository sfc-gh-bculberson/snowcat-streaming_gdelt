"""One incremental GDELT ingest cycle: discover new 15-minute windows since the
watermark, stream their rows into Snowflake via Snowpipe Streaming, and
advance the watermark. Designed to run to completion and exit -- this is the
function invoked by main.py, which is what the SPCS job container runs every
time the Snowflake Task fires it (every 15 minutes).

Watermark state lives on a *standard* channel offset token (see
``gdelt_incremental.watermark``); EVENTS/MENTIONS/GKG use elastic channels.
"""

from __future__ import annotations

import logging
import time
from typing import Dict

from snowflake.ingest.streaming import StreamingIngestClient
from snowflake.ingest.streaming.streaming_ingest_error import (
    StreamingIngestError,
    StreamingIngestErrorCode,
)

from gdelt_incremental import config, gdelt_client, watermark as wm
from gdelt_incremental.gdelt_client import GdeltFileNotFound
from gdelt_incremental.row_encoder import iter_rows_from_zip
from gdelt_incremental.schemas import TABLE_ORDER, TableKind

logger = logging.getLogger(__name__)

_RETRYABLE_ERROR_CODES = {
    StreamingIngestErrorCode.MEMORY_THRESHOLD_EXCEEDED,
    StreamingIngestErrorCode.MEMORY_THRESHOLD_EXCEEDED_IN_CONTAINER,
    StreamingIngestErrorCode.RECEIVER_SATURATED,
    StreamingIngestErrorCode.HTTP_RETRYABLE_CLIENT_ERROR,
    StreamingIngestErrorCode.SF_API_INTERNAL_SERVER_ERROR,
}
_RETRYABLE_HTTP_STATUS = {429, 500, 503}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, StreamingIngestError):
        if exc.error_code in _RETRYABLE_ERROR_CODES:
            return True
        if exc.http_status_code in _RETRYABLE_HTTP_STATUS:
            return True
    return False


def _open_clients() -> Dict[TableKind, StreamingIngestClient]:
    clients: Dict[TableKind, StreamingIngestClient] = {}
    for table in TABLE_ORDER:
        clients[table] = StreamingIngestClient(
            client_name=f"{config.SNOWFLAKE_CLIENT_NAME}-{table}",
            db_name=config.SNOWFLAKE_DATABASE,
            schema_name=config.SNOWFLAKE_SCHEMA,
            pipe_name=config.pipe_name(table),
            properties=config.snowflake_properties(),
        )
    return clients


def _append_batch_with_retry(channel, batch: list) -> None:
    max_attempts = config.APPEND_ROWS_MAX_RETRIES + 1
    base_delay = config.APPEND_ROWS_BACKOFF_BASE_SEC
    max_delay = config.APPEND_ROWS_BACKOFF_MAX_SEC
    for attempt in range(max_attempts):
        try:
            result = channel.append_rows(batch)
            # Elastic channels return a Future; wait so we don't advance the
            # watermark past unacknowledged batches.
            if result is not None and hasattr(result, "result"):
                result.result()
            return
        except Exception as exc:  # noqa: BLE001 - re-raised below when not retryable
            is_last = attempt >= max_attempts - 1
            if is_last or not _is_retryable(exc):
                raise
            delay = min(max_delay, base_delay * (2**attempt))
            logger.warning(
                "append_rows attempt %d/%d failed (%d rows): %s; retrying in %.2fs",
                attempt + 1,
                max_attempts,
                len(batch),
                exc,
                delay,
            )
            time.sleep(delay)


def _ingest_file(channel, table: TableKind, ts: str) -> int:
    """Download, decode, and stream one table's file for one 15-min window.
    Returns rows ingested (0 if the file legitimately doesn't exist)."""
    files = gdelt_client.urls_for_timestamp(ts)
    gdelt_file = files[table]
    try:
        local_path = gdelt_client.download(gdelt_file.url)
    except GdeltFileNotFound:
        logger.warning("table=%s ts=%s file not found upstream, skipping: %s", table, ts, gdelt_file.url)
        return 0

    client_ts_ms = int(time.time() * 1000)
    rows_ingested = 0
    try:
        for batch in iter_rows_from_zip(
            local_path,
            table,
            gdelt_timestamp=ts,
            client_ts_ms=client_ts_ms,
            batch_size=config.BATCH_SIZE,
        ):
            _append_batch_with_retry(channel, batch)
            rows_ingested += len(batch)
    finally:
        local_path.unlink(missing_ok=True)
    return rows_ingested


def run_once() -> int:
    """Run one incremental cycle. Returns the number of 15-minute windows
    successfully ingested (0 if already up to date)."""
    config.validate()

    wm_client = wm.open_client()
    try:
        wm_channel = wm.open_channel(wm_client)
        last_watermark = wm.get_watermark(wm_channel)

        lastupdate_files = gdelt_client.fetch_lastupdate()
        latest = gdelt_client.latest_timestamp(lastupdate_files)
        if latest is None:
            logger.error("lastupdate.txt returned no parseable files; aborting run")
            raise RuntimeError("Could not determine latest GDELT timestamp")

        windows = gdelt_client.pending_timestamps(last_watermark, latest)
        if not windows:
            logger.info("Up to date: watermark=%s latest=%s", last_watermark, latest)
            return 0

        logger.info(
            "Watermark=%s latest=%s -> %d window(s) to ingest: %s",
            last_watermark,
            latest,
            len(windows),
            windows,
        )

        clients = _open_clients()
        channels = {table: clients[table].get_elastic_channel() for table in TABLE_ORDER}
        completed = 0
        try:
            for ts in windows:
                totals = {}
                for table in TABLE_ORDER:
                    totals[table] = _ingest_file(channels[table], table, ts)
                logger.info(
                    "ts=%s ingested events=%d mentions=%d gkg=%d",
                    ts,
                    totals["events"],
                    totals["mentions"],
                    totals["gkg"],
                )
                # Elastic append Futures are awaited in _append_batch_with_retry
                # (wait_for_flush is not supported on elastic channels). Advance
                # the standard-channel offset token only after those acks.
                wm.set_watermark(wm_channel, ts)
                completed += 1
        finally:
            for table, client in clients.items():
                try:
                    client.close(wait_for_flush=True)
                except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                    logger.warning("Error closing streaming client for %s: %s", table, exc)
        return completed
    finally:
        try:
            wm_client.close(wait_for_flush=True)
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup
            logger.warning("Error closing watermark streaming client: %s", exc)

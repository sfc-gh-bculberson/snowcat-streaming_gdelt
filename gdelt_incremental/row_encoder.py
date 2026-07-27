"""Tab-delimited GDELT zip files -> row dicts for Snowpipe Streaming.

GDELT files have a ``.csv``/``.CSV`` extension but are actually tab-delimited
with no header row. Pure-Python encoder is enough here: a single 15-minute
incremental trio is a few tens of thousands of rows at most.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from gdelt_incremental.schemas import TABLE_COLUMNS, ColumnSpec, TableKind


def _parse_value(raw: str, spec: ColumnSpec) -> Any:
    if not raw:
        return None
    value = raw if spec.kind == "STRING" else raw.strip()
    if not value:
        return None
    if spec.kind == "INTEGER":
        try:
            return int(value)
        except ValueError:
            return None
    if spec.kind == "FLOAT":
        try:
            return float(value)
        except ValueError:
            return None
    return value


def _row_from_fields(
    fields: List[str],
    columns: tuple,
    gdelt_timestamp: str,
    client_ts_ms: int,
) -> Optional[Dict[str, Any]]:
    if not fields:
        return None
    row: Dict[str, Any] = {"_GDELT_TIMESTAMP": gdelt_timestamp, "client_ts_ms": client_ts_ms}
    has_data = False
    for idx, spec in enumerate(columns):
        value = _parse_value(fields[idx] if idx < len(fields) else "", spec)
        row[spec.name] = value
        if value is not None:
            has_data = True
    if not has_data:
        return None
    return row


def _first_zip_member(archive: zipfile.ZipFile) -> Optional[str]:
    members = [name for name in archive.namelist() if not name.endswith("/")]
    return members[0] if members else None


def _iter_lines_from_zip(zip_path: Path) -> Iterator[str]:
    with zipfile.ZipFile(zip_path) as archive:
        member = _first_zip_member(archive)
        if member is None:
            return
        with archive.open(member) as raw_handle:
            text = io.TextIOWrapper(raw_handle, encoding="utf-8", errors="replace", newline="")
            for line in text:
                line = line.rstrip("\r\n")
                if line:
                    yield line


def iter_rows_from_zip(
    zip_path: Path,
    table: TableKind,
    *,
    gdelt_timestamp: str,
    client_ts_ms: int,
    batch_size: int,
) -> Iterator[List[Dict[str, Any]]]:
    """Yield batches of row dicts ready for ``channel.append_rows(batch)``."""
    columns = TABLE_COLUMNS[table]
    batch: List[Dict[str, Any]] = []
    for line in _iter_lines_from_zip(zip_path):
        row = _row_from_fields(line.split("\t"), columns, gdelt_timestamp, client_ts_ms)
        if row is None:
            continue
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

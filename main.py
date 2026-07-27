#!/usr/bin/env python3
"""Entrypoint for the GDELT incremental loader SPCS job.

Runs exactly one incremental ingest cycle then exits. Snowflake runs this
container as an ``EXECUTE JOB SERVICE`` every 15 minutes via a scheduled Task
(see sql/create_task.sql) -- there is no internal loop or scheduler here, the
container's whole lifecycle *is* one job run.
"""

from __future__ import annotations

import logging
import sys

from gdelt_incremental.ingest import run_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("gdelt_incremental.main")


def main() -> int:
    try:
        windows_ingested = run_once()
    except Exception:
        logger.exception("GDELT incremental ingest run failed")
        return 1
    logger.info("Run complete: %d window(s) ingested", windows_ingested)
    return 0


if __name__ == "__main__":
    sys.exit(main())

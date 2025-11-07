from __future__ import annotations

import os
import pathlib
import time

import daft
from daft.functions.ai import embed_text

import logging

logger = logging.getLogger(__name__)


def mkdir() -> str:
    desktop = os.path.join(pathlib.Path("~").expanduser(), "Desktop")
    timestamp = str(int(time.time()))
    path = os.path.join(desktop, timestamp)
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)
    return path


def my_func(source: daft.DataFrame) -> dict:
    count = 4
    logger.info(f"Hello from my_workflow with count={count}")

    source.show()

    logger.info("My job is ready, starting queries..")

    results = {}
    for i in range(count):
        logger.info(f"Starting query {i+1} of {count}.")
        dest = mkdir()
        source.write_parquet(dest)
        results[f"run_{i + 1}"] = dest

    logger.info("All queries are done, sending the results back.")

    return {"results": results}


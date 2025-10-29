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


def my_workflow() -> dict:
    count = 4
    logger.info(f"Hello from my_workflow with count={count}")

    df = daft.from_pydict(
        {
            "text": [
                "Alice was beginning to get very tired of sitting by her sister on the bank.",
                "So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),",
                "whether the pleasure of making a daisy-chain would be worth the trouble of getting up and picking the daisies,",
                "when suddenly a White Rabbit with pink eyes ran close by her.",
                "There was nothing so very remarkable in that;",
                "nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'",
            ]
        }
    )

    logger.info("My job is ready, starting queries..")

    results = {}
    for i in range(count):
        logger.info(f"Starting query {i+1} of {count}.")
        dest = mkdir()
        df.write_parquet(dest)
        results[f"run_{i + 1}"] = dest

    logger.info("All queries are done, sending the results back.")

    return {"results": results}


def my_func(source: daft.DataFrame, count: int) -> dict:
    logger.info("Hello from my_workflow with count={count}")
    source.show()
    return {"results": source.to_pydict()}

from __future__ import annotations

import os
import pathlib
import time

import daft
from daft.functions.ai import embed_text

import logging

logger = logging.getLogger(__name__)


def test_echo_embeddings() -> dict:
    """Test the echo provider for embeddings.

    This function creates text embeddings using whatever provider is configured
    via DAFT_PROVIDER environment variable. When DAFT_PROVIDER=echo, it will
    use the deterministic echo provider that doesn't require any external APIs.
    """
    logger.info("Starting echo embeddings test...")

    df = daft.from_pydict({
        "text": [
            "Hello, world!",
            "This is a test of the echo embedding provider.",
            "Embeddings should be deterministic - same input gives same output.",
            "No GPU or API calls required!",
        ]
    })

    logger.info("Creating embeddings...")
    df_with_embeddings = df.with_column(
        "embedding",
        embed_text(
            df["text"],
            provider="transformers",
            model="sentence-transformers/all-MiniLM-L6-v2",
        )
    )

    # Collect results
    return df_with_embeddings


def hello_world():
    logger.info("Creating embeddings...")
    return


def mkdir() -> str:
    desktop = os.path.join(pathlib.Path("~").expanduser(), "Desktop")
    timestamp = str(int(time.time()))
    path = os.path.join(desktop, timestamp)
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)
    return path


def my_workflow() -> dict:
    count = 4
    logger.info(f"Hello from my_workflow with count={count}")
    catalog = daft.Catalog.from_postgres(supabase_connection)
    catalog.get_table("resume_embeddings").read().show()

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
    print(f"Hello from my_workflow with count={count}")
    source.show()
    return {"results": source.to_pydict()}


def use_db(source: daft.DataFrame, cat: daft.Catalog) -> dict:
    table = cat.create_table_if_not_exists("demo_table", source.schema())
    table.append(source)
    return {"num_rows": source.count_rows()}


def read_sammy(source: daft.DataFrame, cat: daft.Catalog) -> dict:
    output = source.limit(10)
    table = cat.create_table_if_not_exists("sammy_table", output.schema())
    table.append(output)
    return {"num_rows": source.count_rows()}


class CustomResult:
    def __repr__(self) -> str:
        return "CustomResult()"


def unserializable_result() -> CustomResult:
    result = CustomResult()
    return result

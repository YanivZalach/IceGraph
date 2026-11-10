from typing import Optional

import polars as pl


def find_newest_file(metadata_log_entries: pl.DataFrame) -> Optional[str]:
    newest_entries = metadata_log_entries.sort(
        [pl.col("timestamp_ms"), pl.col("file").str.len_chars(), pl.col("file")],
        descending=True,
        nulls_last=True,
    )
    if newest_entries.is_empty():
        return None

    return newest_entries["file"][0]

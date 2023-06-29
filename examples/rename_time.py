"""Fix cache breaking change.

In version 0.2.0, a breaking change was implemented that affects cache data. The `time`
column was renamed to `block_time` to be more consistent with blockfrost.

In order for the code to work with cached data generated in previous versions, the cache
column name needs to be updated. This code will perform that task.
"""

from pathlib import Path

import pandas as pd
from tqdm import tqdm

from minswap.transactions import TRANSACTION_CACHE_PATH, TRANSACTION_UTXO_CACHE_PATH

pools = list(Path(TRANSACTION_CACHE_PATH).iterdir())
for pool in tqdm(pools):
    if pool.is_dir():
        for chunk in pool.iterdir():
            if not chunk.name.endswith(".arrow"):
                continue
            cache = pd.read_feather(chunk)

            cache = cache.rename(columns={"time": "block_time"})

            cache.to_feather(chunk)

pools = list(Path(TRANSACTION_UTXO_CACHE_PATH).iterdir())
for pool in tqdm(pools):
    if pool.is_dir():
        for chunk in pool.iterdir():
            if not chunk.name.endswith(".arrow"):
                continue
            cache = pd.read_feather(chunk)

            cache = cache.rename(columns={"time": "block_time"})

            cache.to_feather(chunk)

# mypy: ignore-errors
import logging
import time

import numpy as np

import minswap.transactions as transactions
from minswap.assets import asset_ticker
from minswap.pools import get_pools

# Just to see what minswap-py is doing under the hood...
logging.getLogger("minswap").setLevel(logging.DEBUG)

# Maximum number of API calls allowed for this script to run
# If only using this to update transactions once per day, and it's the only code using
# Blockfrost, this can be set to 50,000 for a free account.
max_calls = 49000
total_calls = 0

# Get a list of pools
pools = get_pools()
assert isinstance(pools, list)

for ind, pool in enumerate(pools[1000:]):
    if total_calls >= max_calls:
        print("Reached maximum requests. Exiting script.")
        break

    print(
        f"{ind} - Gettings historical UTXOs for pool: {asset_ticker(pool.unit_a)}-{asset_ticker(pool.unit_b)}"
    )

    calls = transactions.cache_utxos(pool.id, max_calls - total_calls)

    cooloff = min(50, calls / 10)
    print(f"Made {calls} calls. Cooling off {cooloff:0.2f}s before starting next pool")
    time.sleep(cooloff)

    cache = transactions.get_utxo_cache(pool.id)

    if cache is None:
        continue

    assert not (
        (cache.time[1:].as_numpy().values - cache.time[:-1].as_numpy().values).astype(
            np.float32
        )
        < 0
    ).any()

    total_calls += calls

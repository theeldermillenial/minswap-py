# mypy: ignore-errors
import numpy as np
from pydantic import ValidationError
from tqdm import tqdm

import minswap.transactions as transactions
from minswap.pools import get_pools

# Just to see what minswap-py is doing under the hood...
# logging.getLogger("minswap").setLevel(logging.DEBUG)

# Maximum number of API calls allowed for this script to run
# If only using this to update transactions once per day, and it's the only code using
# Blockfrost, this can be set to 50,000 for a free account.
total_calls = 0

# Get a list of pools
pools = get_pools()
assert isinstance(pools, list)

for pool in tqdm(pools, total=len(pools)):
    try:
        calls = transactions.cache_utxos(pool, progress=True)
    except ValidationError:
        continue

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

print(f"Finished! Made {total_calls} API calls total.")

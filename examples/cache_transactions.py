# mypy: ignore-errors
"""Build a transaction cache.

The goal of this script is to build a transaction cache. Use this with caution.
Blockfrost will ban an account that tries to maliciously overuse their API based on the
limits permitted for the account.

Built into the caching functions are two levels of rate limiters designed to prevent
Blockfrost issues.

The first is a `max_calls` input, which limits the maximum number of calls that will be
made. For a free account on Blockfrost, 50,000 requests/day are permitted. This is not
an intelligent system. The function will stop when Blockfrost returns an error code or
when all available transactions have been acquired. It is up to the user to not
repeatedly call the caching functions if an error code is returned from Blockfrost.

The second is a rate limiting step, that pauses requests when they are being made too
quickly. Blockfrost allows 10 calls/second, with 500 request bursts and a 10 call/second
regeneration. This means that 500 requests can be sent at once, but then there is a 50
second cooloff period before additional requests can be made. The caching code tries to
account for this and will pause requests when getting near this limit. A warning is
shown to indicate to the user that the code is waiting to cooloff, and this is expected
behavior that can be ignored in most cases.
"""
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from minswap.pools import get_pools
from minswap.transactions import cache_transactions

# Just to see what minswap-py is doing under the hood...
# logging.getLogger("minswap").setLevel(logging.DEBUG)
total_calls = 0

# Get a list of pools
pools = get_pools()
assert isinstance(pools, list)

total_calls = 0
with ThreadPoolExecutor() as executor:
    threads = executor.map(cache_transactions, [pool.id for pool in pools[2500:]])

    with logging_redirect_tqdm():
        for thread in tqdm(threads, total=len(pools), initial=2500):
            total_calls += thread

print(f"Finished! Made {total_calls} API calls total.")

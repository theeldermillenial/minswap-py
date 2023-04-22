"""Functions for getting cardano transactions."""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path
from typing import List, Optional

import numpy
import pandas
import vaex

from minswap.pools import PoolTransactionReference, get_pool_transactions
from minswap.utils import save_timestamp

logger = logging.getLogger(__name__)

TRANSACTION_CACHE_PATH = Path(__file__).parent.joinpath("data/transactions")
TRANSACTION_CACHE_PATH.mkdir(exist_ok=True, parents=True)


def get_transaction_cache(pool_id: str) -> Optional[vaex.DataFrame]:
    """Get a vaex dataframe of locally cached transaction data.

    This function returns a vaex dataframe containing transaction data for the
    specified pool. The transaction data does not contain volume, TVL, price, etc. It
    only contains transation id, transaction hash, timestamp, and block height. This
    information is useful for getting a coarse understanding of number of transactions
    associated with a particular pool.

    Args:
        pool_id: The pool id requesting for a dataframe.

    Returns:
        A memory mapped vaex dataframe.
    """
    cache_path = TRANSACTION_CACHE_PATH.joinpath(pool_id)
    try:
        df = vaex.open(cache_path.joinpath("[0-9][0-9][0-9][0-9][0-9][0-9].arrow"))
    except OSError:
        df = None

    return df


def _cache_transactions(
    transactions: List[PoolTransactionReference], cache_path: Path
) -> List[PoolTransactionReference]:
    if transactions[0].time.month == transactions[-1].time.month:
        index = len(transactions)
    else:
        for index in range(len(transactions) - 1):
            if transactions[index].time.month != transactions[index + 1].time.month:
                index += 1
                break

    # Convert data to a vaex dataframe
    df = pandas.DataFrame([d.dict() for d in transactions[:index]])
    df["time"] = df.time.astype("datetime64[s]")

    # Define the output path
    cache_name = (
        f"{transactions[0].time.year}"
        + f"{str(transactions[0].time.month).zfill(2)}.arrow"
    )
    path = cache_path.joinpath(cache_name)

    # If the cache exists, append to it
    if path.exists():
        cache_df = pandas.read_feather(path)
        tmp_path = path.with_name(path.name.replace(".arrow", "_temp.arrow"))
        threshold = cache_df.time.astype("datetime64[s]").values[-1]
        filtered = df[df.time > threshold]
        if len(filtered) > 0:
            pandas.concat([cache_df, filtered], ignore_index=True).to_feather(tmp_path)
            path.unlink()
            tmp_path.rename(path)

    # Otherwise, just dump the whole dataframe to cache
    else:
        df.to_feather(path)

    return transactions[index:]


@save_timestamp(TRANSACTION_CACHE_PATH, 0, "pool_id")
def cache_transactions(pool_id: str, max_calls: int = 1000) -> int:
    """Cache transactions for a pool.

    This function will build up a local cache of transactions for a specific pool. The
    transactions only contained transaction id, transaction hash, block height, and
    timestamp.

    If a local cache already exists, an attempt to resume from the most recent data
    point will be made. The local cache will also be used to estimate the number of
    transactions made over the time period of the cache and use this information to
    estimate the number of API calls needed to update the cache. At the minimum, 1
    API call will be made.

    Args:
        pool_id: The pool id to cache transactions for.
        max_calls: Maximum number of API calls to use. If this limit is reached before
            finding all transactions, it will cache the transactions it has found and
            return. Defaults to 1000.

    Returns:
        The number of API calls made. To get the transaction cache, use the
            `get_transaction_cache` function.
    """
    cache_path = TRANSACTION_CACHE_PATH.joinpath(pool_id)
    now = datetime.utcnow()

    # blockfrost allows 10 calls/sec, with 500 call bursts with a 10 call/sec cooloff
    calls_allowed = 500

    # Load existing cache
    cache = get_transaction_cache(pool_id=pool_id)
    if cache is not None:
        # Get the starting page based off existing data cache
        page = len(cache) // 100 + 1

        # Get a rough estimate of how many calls are needed to update the cache
        last_month = now - timedelta(days=30)
        filtered = cache[cache.time > numpy.datetime64(last_month)]

        # If no data from the previous month,
        if len(filtered) <= 1:
            filtered = cache

        # If still no data, then just grab data one page at a time
        if len(filtered) <= 1:
            call_batch = 1
        else:
            # Calculate the mean transaction rate
            time_period = (
                filtered.time.values[-1].as_py() - filtered.time.values[0].as_py()
            ).total_seconds()
            n_transactions = len(filtered)
            if time_period is None or time_period == 0:
                call_batch = 1
            else:
                tps = n_transactions / time_period

                # Estimate number of pages needed to update to the current time
                time_delta = (
                    datetime.utcnow() - filtered.time.values[-1].as_py()
                ).total_seconds()
                call_batch = min(cpu_count(), int(time_delta * tps // 100))

        filtered.close()
        cache.close()

    else:
        page = 1
        call_batch = cpu_count()

    if call_batch == 0:
        call_batch = 1

    logger.debug(f"start page: {page}")

    def get_transaction_batch(page: int) -> List[PoolTransactionReference]:
        transactions = get_pool_transactions(
            pool_id=pool_id, page=page, count=100, order="asc"
        )

        return transactions

    with ThreadPoolExecutor(call_batch) as executor:
        done = False
        num_calls = 0
        transactions: List[PoolTransactionReference] = []
        call_start = None
        call_end = None
        while not done and num_calls < max_calls:
            if num_calls + call_batch > max_calls:
                call_batch = max_calls - num_calls
            num_calls += call_batch
            logger.debug(f"Calling page range: {page}-{page+call_batch}")

            # Rate limit the calling
            call_end = datetime.now()
            calls_allowed = calls_allowed - call_batch
            if call_start is not None:
                call_diff = call_end - call_start

                # Cooloff is 10 requests per second
                calls_allowed += call_diff.total_seconds() * 10

                if calls_allowed < 0:
                    delay_time = 5 * call_batch / 10
                    logger.warning(
                        "Nearing rate limit, waiting "
                        + f"{delay_time:0.2f} seconds to resume."
                    )
                    time.sleep(delay_time)
                    calls_allowed += (datetime.now() - call_end).total_seconds() * 10

            call_start = datetime.now()

            # Make the calls
            threads = executor.map(
                get_transaction_batch, range(page, page + call_batch)
            )
            for thread in threads:
                if len(thread) == 0:
                    done = True
                    break
                transactions.extend(thread)

            # Store the data if all data for a month is collected
            while (
                len(transactions) > 0
                and transactions[0].time.month != transactions[-1].time.month
            ):
                logger.debug(
                    "Caching transactions for "
                    + f"{transactions[0].time.year}"
                    + f"{str(transactions[0].time.month).zfill(2)}"
                )
                transactions = _cache_transactions(transactions, cache_path)
            page += call_batch

        if len(transactions) > 0:
            _cache_transactions(transactions, cache_path)

    return num_calls

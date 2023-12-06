"""Functions for getting cardano transactions."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

import numpy
import pandas
import vaex
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import minswap
import minswap.addr
import minswap.models
import minswap.utils

if TYPE_CHECKING:
    import minswap.assets.asset_ticker
    import minswap.models.PoolState

logger = logging.getLogger(__name__)

TRANSACTION_CACHE_PATH = Path(__file__).parent.joinpath("data/transactions")
TRANSACTION_CACHE_PATH.mkdir(exist_ok=True, parents=True)

TRANSACTION_UTXO_CACHE_PATH = Path(__file__).parent.joinpath("data/utxos")
TRANSACTION_UTXO_CACHE_PATH.mkdir(exist_ok=True, parents=True)


def get_transaction_cache(
    pool: Union[minswap.models.PoolState, str]
) -> Optional[vaex.DataFrame]:
    """Get a vaex dataframe of locally cached transaction data.

    This function returns a vaex dataframe containing transaction data for the
    specified pool. The transaction data does not contain volume, TVL, price, etc. It
    only contains transation id, transaction hash, timestamp, and block height. This
    information is useful for getting a coarse understanding of number of transactions
    associated with a particular pool.

    Args:
        pool: A pool state object or pool id requested for a dataframe.

    Returns:
        A memory mapped vaex dataframe.
    """
    pool_id = pool if isinstance(pool, str) else pool.id
    return minswap.utils._get_cache(cache_path=TRANSACTION_CACHE_PATH.joinpath(pool_id))


def get_utxo_cache(
    pool: Union[minswap.models.PoolState, str]
) -> Optional[vaex.DataFrame]:
    """Get a vaex dataframe of locally cached transaction data.

    This function returns a vaex dataframe containing transaction data for the
    specified pool. The transaction data does not contain volume, TVL, price, etc. It
    only contains transation id, transaction hash, timestamp, and block height. This
    information is useful for getting a coarse understanding of number of transactions
    associated with a particular pool.

    Args:
        pool: A pool state object or pool id requested for a dataframe.

    Returns:
        A memory mapped vaex dataframe.
    """
    pool_id = pool if isinstance(pool, str) else pool.id
    return minswap.utils._get_cache(
        cache_path=TRANSACTION_UTXO_CACHE_PATH.joinpath(pool_id)
    )


def get_pool_transaction_history(
    pool: Union[minswap.models.PoolState, str],
    page: int = 1,
    count: int = 100,
    order: str = "desc",
) -> List[minswap.models.PoolTransactionReference]:
    """Get a list of pool history transactions.

    This returns only a list of `PoolHistory` items, each providing enough information
    to track down a particular pool transaction.

    Args:
        pool: A pool state object or pool id.
        page: The index of paginated results to return. Defaults to 1.
        count: The total number of results to return. Defaults to 100.
        order: Must be "asc" or "desc". Defaults to "desc".

    Returns:
        A list of `PoolHistory` items.
    """
    pool_id = pool if isinstance(pool, str) else pool.id

    nft = f"{minswap.addr.POOL_NFT_POLICY_ID}{pool_id}"
    nft_txs = minswap.utils.BlockfrostBackend.api().asset_transactions(
        nft, count=count, page=page, order=order, return_type="json"
    )

    pool_snapshots = [
        minswap.models.PoolTransactionReference.model_validate(tx) for tx in nft_txs
    ]

    return pool_snapshots


@minswap.utils.save_timestamp(TRANSACTION_CACHE_PATH, 0, "pool_id")
def cache_transactions(
    pool: Union[str, minswap.models.PoolState],
    max_calls: int = minswap.utils.BlockfrostBackend.remaining_calls(),
) -> int:
    """Cache transactions for a pool.

    This function will build up a local cache of transactions for a specific pool. The
    transactions only contained transaction id, transaction hash, block height, and
    timestamp.

    If a local cache already exists, an attempt to resume from the most recent data
    point will be made. The local cache will also be used to estimate the number of
    transactions made over the time period of the cache and use this information to
    estimate the number of API calls needed to update the cache. At the minimum, 1
    API call will be made.

    Todo:
        Change the way `max_calls` operates. The default should be None, which just runs
            until the `BlockfrostBackend` runs out of requests. Then, the code should
            run until either `max_calls` or the `BlockfrostBackend` runs out of
            requests. Also add the option to allow `max_calls=-1` to turn off all
            request limit checks.

    Args:
        pool: The pool state or pool id to cache transactions for.
        max_calls: Maximum number of API calls to use. If this limit is reached before
            finding all transactions, it will cache the transactions it has found and
            return. Defaults to 1000.

    Returns:
        The number of API calls made. To get the transaction cache, use the
            `get_transaction_cache` function.
    """
    pool_id = pool if isinstance(pool, str) else pool.id
    cache_path = TRANSACTION_CACHE_PATH.joinpath(pool_id)
    now = datetime.utcnow()

    # Load existing cache
    cache = get_transaction_cache(pool=pool_id)

    if cache is not None:
        # Get the starting page based off existing data cache
        page = len(cache) // 100 + 1

        # Get a rough estimate of how many calls are needed to update the cache
        last_month = now - timedelta(days=30)
        filtered = cache[cache.block_time > numpy.datetime64(last_month)]

        # If no data from the previous month,
        if len(filtered) <= 1:
            filtered = cache

        # If still no data, then just grab data one page at a time
        if len(filtered) <= 1:
            call_batch = 1
        else:
            # Calculate the mean transaction rate
            time_period = (
                filtered.block_time.values[-1].as_py()
                - filtered.block_time.values[0].as_py()
            ).total_seconds()
            n_transactions = len(filtered)
            if time_period is None or time_period == 0:
                call_batch = 1
            else:
                tps = n_transactions / time_period

                # Estimate number of pages needed to update to the current time
                time_delta = (
                    datetime.utcnow() - filtered.block_time.values[-1].as_py()
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

    def get_transaction_batch(
        page: int,
    ) -> List[minswap.models.PoolTransactionReference]:
        transactions = get_pool_transaction_history(
            pool=pool_id, page=page, count=100, order="asc"
        )

        return transactions

    with ThreadPoolExecutor(call_batch) as executor:
        done = False
        num_calls = 0
        transactions: List[minswap.models.PoolTransactionReference] = []
        while not done and num_calls < max_calls:
            # Exit if max_calls is reached
            if num_calls + call_batch > max_calls:
                call_batch = max_calls - num_calls

            num_calls += call_batch
            logger.debug(f"Calling page range: {page}-{page+call_batch}")

            # Make the calls
            threads = executor.map(
                get_transaction_batch, range(page, page + call_batch)
            )
            for thread in threads:
                if len(thread) != 100:
                    done = True

                    if len(thread) == 0:
                        break

                transactions.extend(thread)

            # Store the data if all data for a month is collected
            while (
                len(transactions) > 0
                and transactions[0].block_time.month
                != transactions[-1].block_time.month
            ):
                logger.debug(
                    "Caching transactions for "
                    + f"{transactions[0].block_time.year}"
                    + f"{str(transactions[0].block_time.month).zfill(2)}"
                )
                transactions = minswap.utils._cache_timestamp_data(
                    transactions, cache_path
                )
            page += call_batch

        if len(transactions) > 0:
            logger.debug(
                "Caching transactions for "
                + f"{transactions[0].block_time.year}"
                + f"{str(transactions[0].block_time.month).zfill(2)}"
            )
            minswap.utils._cache_timestamp_data(transactions, cache_path)

    return num_calls


@minswap.utils.save_timestamp(TRANSACTION_UTXO_CACHE_PATH, 0, "pool_id")
def cache_utxos(
    pool: Union[minswap.models.PoolState, str],
    max_calls: int = minswap.utils.BlockfrostBackend.remaining_calls(),
    progress: bool = False,
) -> int:
    """Cache transaction utxos for a pool.

    This function will build up a local cache of transaction utxos for a specific pool.
    This function relies on the existing cache of transaction references generated by
    `cache_transactions`, so that function will need to be run prior to running this
    function.

    Like `cache_transactions`, it will try to minimize the number of API calls
    that are made.

    Args:
        pool_id: The pool id to cache transactions for.
        max_calls: Maximum number of API calls to use. If this limit is reached before
            finding all transactions, it will cache the transactions it has found and
            return. Defaults to the number of remaining calls permitted by the
            `BlockfrostBackend`.
        progress: Whether to use a progress bar. This can be useful to see the progress
            when there are many transactions to pull in. Defaults to False.

    Returns:
        The number of API calls made. To get the utxos cache, use the
            `get_utxo_cache` function.
    """
    pool_id = pool if isinstance(pool, str) else pool.id
    cache_path = TRANSACTION_UTXO_CACHE_PATH.joinpath(pool_id)

    # Load existing cache
    cache = get_transaction_cache(pool=pool_id)
    if cache is None:
        return 0

    # Filter transactions to skip over previously cached data
    utxo_cache = get_utxo_cache(pool=pool_id)
    if utxo_cache is not None:
        init_length = len(cache)
        cache = cache[
            cache.block_time
            > numpy.datetime64(utxo_cache.block_time.values[-1].as_py())
        ]
        logger.debug(f"Found {init_length-len(cache)} cached transactions.")
        utxo_cache.close()

    if len(cache) == 0:
        logger.debug("No transactions to get. Returning.")
        return 0

    logger.debug(f"Need to get {len(cache)} transactions.")

    # Batching values
    last_index = min(max_calls, len(cache))

    with ThreadPoolExecutor() as executor:
        num_calls = 0
        tx_utxos: List[pandas.DataFrame] = []

        if progress:
            with logging_redirect_tqdm():
                if not isinstance(pool, str):
                    ticker_a = minswap.assets.asset_ticker(pool.unit_a)
                    ticker_b = minswap.assets.asset_ticker(pool.unit_b)
                    desc = f"{ticker_a}/{ticker_b}"
                else:
                    desc = "Getting UTXOs"
                for ts, df in tqdm(
                    zip(
                        cache.block_time.values[:last_index],
                        executor.map(
                            minswap.utils.get_utxo,
                            cache.tx_hash.values[:last_index],
                        ),
                    ),
                    total=last_index,
                    leave=False,
                    desc=desc,
                    unit="tx",
                ):
                    df["block_time"] = ts.as_py()
                    tx_utxos.append(df)
        else:
            for ts, df in zip(
                cache.block_time.values[:last_index],
                executor.map(minswap.utils.get_utxo, cache.tx_hash.values[:last_index]),
            ):
                num_calls += 1
                df["block_time"] = ts.as_py()
                df["block_time"] = df.block_time.astype("datetime64[s]")
                tx_utxos.append(df)

        while len(tx_utxos) > 0:
            logger.debug(
                "Caching transactions for "
                + f"{tx_utxos[0].block_time[0].year}"
                + f"{str(tx_utxos[0].block_time[0].month).zfill(2)}"
            )
            tx_utxos = minswap.utils._cache_timestamp_data(tx_utxos, cache_path)

    return num_calls

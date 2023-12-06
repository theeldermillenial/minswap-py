"""Important Cardano addresses for Minswap.

These are addresses for pools and smart contracts associated with Minswap.

Attributes:
    POOL: Mainnet pool address.
    POOL_TEST: Testnet pool address.
    ORDER: Mainnet pool address.
    ORDER_TEST: Testnet pool address.
"""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path
from typing import List, Optional, Tuple

import numpy
import pandas
import vaex
from dotenv import load_dotenv
from pyarrow import TimestampScalar

from minswap.models import Address, PoolTransactionReference
from minswap.utils import BlockfrostBackend, get_utxo, save_timestamp

load_dotenv()

logger = logging.getLogger(__name__)

ORDER_MAIN = Address(
    bech32="addr1zxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uw6j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq6s3z70"  # noqa
)
STAKE_ORDER_MAIN = Address(
    bech32="addr1zxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uw6j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq6s3z70"  # noqa
)

ORDER_TEST = Address(
    bech32="addr_test1zzn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uwurajt8r8wqtygrfduwgukk73m5gcnplmztc5tl5ngy0upq932hcy"  # noqa
)
STAKE_ORDER_TEST = Address(
    bech32="addr_test1zzn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uwurajt8r8wqtygrfduwgukk73m5gcnplmztc5tl5ngy0upq932hcy"  # noqa
)

if os.environ.get("NETWORK", "mainnet").lower() == "mainnet":
    ORDER = ORDER_MAIN
    STAKE_ORDER = STAKE_ORDER_MAIN
elif os.environ.get("NETWORK", None) == "preprod":
    ORDER = ORDER_TEST
    STAKE_ORDER = STAKE_ORDER_TEST
else:
    raise ValueError(
        f"The NETWORK environment variable was set to {os.environ.get('NETWORK')}. "
        + "Must be one of ['main', 'preprod']."
    )

# Policies
FACTORY_POLICY_ID = "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f"
FACTORY_ASSET_NAME = "4d494e53574150"
LP_POLICY_ID = "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86"
POOL_NFT_POLICY_ID = "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1"

# Pool Own Liquidity Addresses
POL_MINADA_LBE_LP = Address(
    bech32="addr1q9wz03xdpasq5t7tv4vvqyw9frhz2x9862ct3xyh697pfwjj2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pqyk6dej"  # noqa
)
POL_INDYADA_LP = Address(
    bech32="addr1q9gxe8vx0kvv5g6gv4n5wmsxexjqsjftc599qqcp2vkmmwh7snv5yhw2qqvdev3c7wn6s3xhrnx25eg6zcqjxj9vrv2s0e38ze"  # noqa
)
POL_COSADA_LP = Address(
    bech32="addr1q9gxe8vx0kvv5g6gv4n5wmsxexjqsjftc599qqcp2vkmmwh7snv5yhw2qqvdev3c7wn6s3xhrnx25eg6zcqjxj9vrv2s0e38ze"  # noqa
)
POL_MINADA_LP = Address(
    bech32="addr1qx40kyvkgdxtsdladusny09r0vczmlmrsl52sjel528674jj2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pqg82qtn"  # noqa
)

POL_TRANSACTION_CACHE_PATH = Path(__file__).parent.joinpath("data/pol/transactions")
POL_TRANSACTION_CACHE_PATH.mkdir(exist_ok=True, parents=True)

POL_TRANSACTION_UTXO_CACHE_PATH = Path(__file__).parent.joinpath("data/pol/utxos")
POL_TRANSACTION_UTXO_CACHE_PATH.mkdir(exist_ok=True, parents=True)


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
    df = pandas.DataFrame([d.model_dump() for d in transactions[:index]])
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


def _cache_utxos(
    utxos: List[Tuple[TimestampScalar, pandas.DataFrame]], cache_path: Path
) -> List[Tuple[TimestampScalar, pandas.DataFrame]]:
    if utxos[0][0].as_py().month == utxos[-1][0].as_py().month:
        index = len(utxos)
    else:
        for index in range(len(utxos) - 1):
            if utxos[index][0].as_py().month != utxos[index + 1][0].as_py().month:
                index += 1
                break

    # Add time to all dataframes
    dfs = []
    for t, df in utxos[:index]:
        df["time"] = t.as_py()
        df["time"] = df.time.astype("datetime64[s]")
        dfs.append(df)

    # Concatenate all dataframes
    df = pandas.concat(dfs, ignore_index=True)

    # Define the output path
    cache_name = (
        f"{utxos[0][0].as_py().year}"
        + f"{str(utxos[0][0].as_py().month).zfill(2)}.arrow"
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

    return utxos[index:]


def get_pol_transaction_cache(pol_addr: str) -> Optional[vaex.DataFrame]:
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
    cache_path = POL_TRANSACTION_CACHE_PATH.joinpath(pol_addr)
    try:
        df = vaex.open(cache_path.joinpath("[0-9][0-9][0-9][0-9][0-9][0-9].arrow"))
    except OSError:
        df = None

    return df


def get_pol_utxo_cache(pol_addr: str) -> Optional[vaex.DataFrame]:
    """Get a vaex dataframe of locally cached utxo data.

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
    cache_path = POL_TRANSACTION_UTXO_CACHE_PATH.joinpath(pol_addr)
    try:
        df = vaex.open(cache_path.joinpath("[0-9][0-9][0-9][0-9][0-9][0-9].arrow"))
    except OSError:
        df = None

    return df


def get_pol_transaction_history(
    pol_addr: str,
    page: int = 1,
    count: int = 100,
    order: str = "asc",
) -> List[PoolTransactionReference]:
    """Get a list of pool history transactions.

    This returns only a list of `PoolHistory` items, each providing enough information
    to track down a particular pool transaction.

    Args:
        pool_id: The unique pool id.
        page: The index of paginated results to return. Defaults to 1.
        count: The total number of results to return. Defaults to 100.
        order: Must be "asc" or "desc". Defaults to "desc".

    Returns:
        A list of `PoolHistory` items.
    """
    txs = BlockfrostBackend.api().address_transactions(
        pol_addr, count=count, page=page, order=order, return_type="json"
    )

    tx = [PoolTransactionReference.model_validate(tx) for tx in txs]

    return tx


@save_timestamp(POL_TRANSACTION_CACHE_PATH, 0, "pol_addr")
def cache_pol_transactions(pol_addr: str, max_calls: int = 1000) -> int:
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
    cache_path = POL_TRANSACTION_CACHE_PATH.joinpath(pol_addr)
    now = datetime.utcnow()

    # blockfrost allows 10 calls/sec, with 500 call bursts with a 10 call/sec cooloff
    calls_allowed = 500

    # Load existing cache
    cache = get_pol_transaction_cache(pol_addr=pol_addr)

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
        tx = get_pol_transaction_history(
            pol_addr=pol_addr, page=page, count=100, order="asc"
        )

        return tx

    with ThreadPoolExecutor(call_batch) as executor:
        done = False
        num_calls = 0
        tx: List[PoolTransactionReference] = []
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
                if len(thread) != 100:
                    done = True

                    if len(thread) == 0:
                        break

                tx.extend(thread)

            # Store the data if all data for a month is collected
            while len(tx) > 0 and tx[0].time.month != tx[-1].time.month:
                logger.debug(
                    "Caching transactions for "
                    + f"{tx[0].time.year}"
                    + f"{str(tx[0].time.month).zfill(2)}"
                )
                tx = _cache_transactions(tx, cache_path)
            page += call_batch

        if len(tx) > 0:
            logger.debug(
                "Caching transactions for "
                + f"{tx[0].time.year}"
                + f"{str(tx[0].time.month).zfill(2)}"
            )
            _cache_transactions(tx, cache_path)

    return num_calls


@save_timestamp(POL_TRANSACTION_UTXO_CACHE_PATH, 0, "pol_addr")
def cache_pol_utxos(pol_addr: str, max_calls: int = 1000) -> int:
    """Cache transaction utxos for a pool.

    This function will build up a local cache of transaction utxos for a
    specific pool. This function relies on the existing cache of transaction
    references generated by `cache_transactions`, so that function will need to
    be run prior to running this function.

    Like `cache_transactions`, it will try to minimize the number of API calls
    that are made.

    Args:
        pool_id: The pool id to cache transactions for.
        max_calls: Maximum number of API calls to use. If this limit is reached before
            finding all transactions, it will cache the transactions it has found and
            return. Defaults to 1000.

    Returns:
        The number of API calls made. To get the utxos cache, use the
            `get_utxo_cache` function.
    """
    cache_path = POL_TRANSACTION_UTXO_CACHE_PATH.joinpath(pol_addr)

    # blockfrost allows 10 calls/sec, with 500 call bursts with a 10 call/sec cooloff
    calls_allowed = 500

    # Load existing cache
    cache = get_pol_transaction_cache(pol_addr=pol_addr)
    if cache is None:
        return 0

    # Filter transactions to skip over previously cached data
    utxo_cache = get_pol_utxo_cache(pol_addr=pol_addr)
    if utxo_cache is not None:
        init_length = len(cache)
        cache = cache[cache.time > numpy.datetime64(utxo_cache.time.values[-1].as_py())]
        logger.debug(f"Found {init_length-len(cache)} cached transactions.")
        utxo_cache.close()

    logger.debug(f"Need to get {len(cache)} transactions.")

    # Batching values
    last_index = min(max_calls, len(cache))
    batch_size = cpu_count()

    with ThreadPoolExecutor() as executor:
        num_calls = 0
        tx_utxos: List[pandas.DataFrame] = []
        call_start = None
        call_end = None

        for bs in range(0, last_index, batch_size):
            be = min(bs + batch_size, last_index)

            batch_size = be - bs

            num_calls += batch_size

            # Rate limit the calling
            call_end = datetime.now()
            calls_allowed = calls_allowed - batch_size
            if call_start is not None:
                call_diff = call_end - call_start

                # Cooloff is 10 requests per second
                calls_allowed += call_diff.total_seconds() * 10

                if calls_allowed < 0:
                    delay_time = 5 * batch_size / 10
                    logger.warning(
                        "Nearing rate limit, waiting "
                        + f"{delay_time:0.2f} seconds to resume."
                    )
                    time.sleep(delay_time)
                    calls_allowed += (datetime.now() - call_end).total_seconds() * 10

            call_start = datetime.now()

            logger.debug(f"Calling batch: {bs}-{be}")
            tx_utxos.extend(
                list(
                    zip(
                        cache.time[bs:be].values,
                        executor.map(get_utxo, cache.tx_hash[bs:be].values),
                    )
                )
            )

            while (
                len(tx_utxos) > 0
                and tx_utxos[0][0].as_py().month != tx_utxos[-1][0].as_py().month
            ):
                logger.debug(
                    "Caching transactions for "
                    + f"{tx_utxos[0][0].as_py().year}"
                    + f"{str(tx_utxos[0][0].as_py().month).zfill(2)}"
                )
                tx_utxos = _cache_utxos(tx_utxos, cache_path)

        if len(tx_utxos) > 0:
            logger.debug(
                "Caching transactions for "
                + f"{tx_utxos[0][0].as_py().year}"
                + f"{str(tx_utxos[0][0].as_py().month).zfill(2)}"
            )
            _cache_utxos(tx_utxos, cache_path)

    return num_calls

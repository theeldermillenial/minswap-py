"""Functions for getting cardano assets."""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from decimal import Decimal
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, MutableSet, Optional, Union

import numpy
import pandas
import vaex

import minswap.transactions
import minswap.utils

if TYPE_CHECKING:
    import minswap.utils._cache_utxos

ASSET_CACHE_PATH = Path(__file__).parent.joinpath("data/assets")
ASSET_CACHE_PATH.mkdir(exist_ok=True, parents=True)

ASSET_INFO_CACHE_PATH = ASSET_CACHE_PATH.joinpath("info")
ASSET_INFO_CACHE_PATH.mkdir(exist_ok=True, parents=True)

ASSET_TRANSACTION_CACHE_PATH = ASSET_CACHE_PATH.joinpath("transactions")
ASSET_TRANSACTION_CACHE_PATH.mkdir(exist_ok=True, parents=True)

ASSET_UTXO_CACHE_PATH = ASSET_CACHE_PATH.joinpath("utxos")
ASSET_UTXO_CACHE_PATH.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("minswap.assets")


def get_asset_history_cache(asset_id: str) -> Optional[vaex.DataFrame]:
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
    return minswap.utils._get_cache(
        cache_path=ASSET_INFO_CACHE_PATH.joinpath(asset_id), glob="history.arrow"
    )


def get_asset_history_transaction_cache(asset_id: str) -> Optional[vaex.DataFrame]:
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
    return minswap.utils._get_cache(cache_path=ASSET_INFO_CACHE_PATH.joinpath(asset_id))


def get_asset_transaction_cache(asset_id: str) -> Optional[vaex.DataFrame]:
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
    return minswap.utils._get_cache(
        cache_path=ASSET_TRANSACTION_CACHE_PATH.joinpath(asset_id)
    )


def _cache_txs(
    transactions: List[minswap.models.Transaction],
    cache_path: Path,
    use_hash: bool = False,
) -> List[minswap.models.Transaction]:
    if transactions[0].block_time.month == transactions[-1].block_time.month:
        index = len(transactions)
    else:
        for index in range(len(transactions) - 1):
            if (
                transactions[index].block_time.month
                != transactions[index + 1].block_time.month
            ):
                index += 1
                break

    # Convert data to a vaex dataframe
    df = pandas.DataFrame([d.model_dump() for d in transactions[:index]])
    df["block_time"] = df.block_time.astype("datetime64[s]")

    # Define the output path
    cache_name = (
        f"{transactions[0].block_time.year}"
        + f"{str(transactions[0].block_time.month).zfill(2)}.arrow"
    )
    path = cache_path.joinpath(cache_name)

    # If the cache exists, append to it
    if path.exists():
        cache_df = pandas.read_feather(path)
        tmp_path = path.with_name(path.name.replace(".arrow", "_temp.arrow"))
        if use_hash:
            unique_hashes = list(
                set(df.hash.values.tolist()) - set(cache_df.hash.values.tolist())
            )
            filtered = df[df.hash.isin(unique_hashes)]
            if len(filtered) > 0:
                tmp_df = pandas.concat(
                    [cache_df, filtered], ignore_index=True
                ).sort_values("block_time")
                try:
                    tmp_df.drop("level_0", axis=1, inplace=True)
                except KeyError:
                    pass
                print(tmp_df.head())
                raise Exception
                tmp_df = tmp_df.reset_index()
                tmp_df.drop("level_0", axis=1, inplace=True)
                tmp_df.to_feather(tmp_path)
                path.unlink()
                tmp_path.rename(path)
        else:
            threshold = cache_df.block_time.astype("datetime64[s]").values[-1]
            filtered = df[df.block_time > threshold]
            if len(filtered) > 0:
                pandas.concat([cache_df, filtered], ignore_index=True).to_feather(
                    tmp_path
                )
                path.unlink()
                tmp_path.rename(path)

    # Otherwise, just dump the whole dataframe to cache
    else:
        df.to_feather(path)

    return transactions[index:]


def get_utxo_cache(asset_id: str) -> Optional[vaex.DataFrame]:
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
    return minswap.utils._get_cache(cache_path=ASSET_UTXO_CACHE_PATH.joinpath(asset_id))


@minswap.utils.save_timestamp(ASSET_INFO_CACHE_PATH, 0, "asset")
def update_asset_info(asset: str) -> Optional[minswap.models.AssetIdentity]:
    """Get the latest asset information from the Cardano chain.

    Args:
        asset: The policy id plus hex encoded name of an asset.

    Returns:
        AssetIdentity
    """
    asset_path = ASSET_INFO_CACHE_PATH.joinpath(asset)

    info = minswap.utils.BlockfrostBackend.api().asset(asset, return_type="json")

    asset_id = minswap.models.AssetIdentity.model_validate(info)

    with open(asset_path.joinpath("asset.json"), "w") as fw:
        json.dump(asset_id.model_dump(), fw, indent=2)

    return asset_id


def get_asset_info(
    asset: str, update_cache=False
) -> Optional[minswap.models.AssetIdentity]:
    """Get the asset information.

    Return the asset information. This will use cached information if available, and
    will fetch the asset information from the blockchain if either the cache does not
    exist of `update_cache=True`.

    Args:
        asset: The policy id plus hex encoded name of an asset.
        update_cache: If `True`, fetches the latest asset information from the
            blockchain. Default is `False`.

    Returns:
        The asset identity if available, else `None`.
    """
    asset_path = ASSET_INFO_CACHE_PATH.joinpath(asset).joinpath("asset.json")
    if update_cache or not asset_path.exists():
        return update_asset_info(asset)
    else:
        with open(asset_path) as fr:
            data = fr.read()
        return minswap.models.AssetIdentity.model_validate_json(data)


def update_assets(assets: Union[MutableSet[str], minswap.models.Assets]) -> None:
    """Update asset information cache.

    Args:
        assets: A list of policy ids plus hex encoded names of assets.
    """
    with ThreadPoolExecutor() as executor:
        for _ in executor.map(update_asset_info, assets):
            pass


def asset_decimals(unit: str) -> int:
    """Asset decimals.

    All asset quantities are stored as integers. The decimals indicates a scaling factor
    for the purposes of human readability of asset denominations.

    For example, ADA has 6 decimals. This means every 10**6 units (lovelace) is 1 ADA.

    Args:
        unit: The policy id plus hex encoded name of an asset.

    Returns:
        The decimals for the asset.
    """
    if unit == "lovelace":
        return 6
    else:
        info = get_asset_info(unit)
        decimals = 0 if info is None else info.decimals
        return decimals


def naturalize_assets(assets: minswap.models.Assets) -> Dict[str, Decimal]:
    """Get the number of decimals associated with an asset.

    This returns a `Decimal` with the proper precision context.

    Args:
        asset: The policy id plus hex encoded name of an asset.

    Returns:
        A dictionary where assets are keys and values are `Decimal` objects containing
            exact quantities of the asset, accounting for asset decimals.
    """
    nat_assets = {}
    for unit, quantity in assets.items():
        if unit == "lovelace":
            nat_assets["lovelace"] = Decimal(quantity) / Decimal(10**6)
        else:
            nat_assets[unit] = Decimal(quantity) / Decimal(10 ** asset_decimals(unit))

    return nat_assets


def asset_ticker(unit: str) -> str:
    """Ticker symbol for an asset.

    This function is designed to always return a value. If a `ticker` is available in
    the asset metadata, it is returned. Otherwise, the human readable asset name is
    returned.

    Args:
        unit: The policy id plus hex encoded name of an asset.

    Returns:
        The ticker or human readable name of an asset.
    """
    if unit == "lovelace":
        asset_name = "ADA"
    else:
        info = get_asset_info(unit)
        if info is None:
            raise ValueError("Could not find asset.")
        if info.metadata is not None and info.metadata.ticker is not None:
            asset_name = info.metadata.ticker
            logger.debug(f"Found ticker for {asset_name}.")
        elif (
            info.onchain_metadata is not None
            and hasattr(info.onchain_metadata, "symbol")
            and info.onchain_metadata.symbol is not None
        ):
            asset_name = info.onchain_metadata.symbol
            logger.debug(f"Found symbol for {asset_name}")
        else:
            assert isinstance(info.asset_name, str)
            try:
                asset_name = bytes.fromhex(info.asset_name).decode()
                logger.debug(
                    f"Could not find ticker for asset ({asset_name}), "
                    + "returning the name."
                )
            except UnicodeDecodeError:
                logger.debug(
                    "Could not find ticker, symbol, and asset_name was not decodable. "
                    + "Returning raw asset_name."
                )
                asset_name = info.asset_name

    return asset_name


def get_asset_history(
    asset_id: str,
    page: int = 1,
    count: int = 100,
    order: str = "desc",
) -> List[minswap.models.AssetHistoryReference]:
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
    asset_txs = minswap.utils.BlockfrostBackend.api().asset_history(
        asset_id, count=count, page=page, order=order, return_type="json"
    )

    asset_snapshots = [
        minswap.models.AssetHistoryReference.model_validate(tx) for tx in asset_txs
    ]

    return asset_snapshots


def get_asset_transactions(
    asset_id: str,
    page: int = 1,
    count: int = 100,
    order: str = "desc",
) -> List[minswap.models.PoolTransactionReference]:
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
    asset_txs = minswap.utils.BlockfrostBackend.api().asset_transactions(
        asset_id, count=count, page=page, order=order, return_type="json"
    )

    asset_snapshots = [
        minswap.models.PoolTransactionReference.model_validate(tx) for tx in asset_txs
    ]

    return asset_snapshots


def get_asset_history_transactions(
    tx_hash: str,
) -> minswap.models.Transaction:
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
    tx = minswap.models.Transaction.model_validate(
        minswap.utils.BlockfrostBackend.api().transaction(
            hash=tx_hash, return_type="json"
        )
    )

    return tx


@minswap.utils.save_timestamp(ASSET_INFO_CACHE_PATH, 0, "asset_id")
def cache_history(
    asset_id: str, max_calls: int = minswap.utils.BlockfrostBackend.remaining_calls()
) -> int:
    """Cache transactions for an asset.

    This function will build up a local cache of transactions for a specific asset. The
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
    cache_path = ASSET_INFO_CACHE_PATH.joinpath(asset_id)

    # Load existing cache
    cache = get_asset_history_cache(asset_id=asset_id)

    if cache is not None:
        # Get the starting page based off existing data cache
        page = len(cache) // 100 + 1

    else:
        page = 1

    call_batch = 1

    logger.debug(f"start page: {page}")

    def get_history_batch(page: int) -> List[minswap.models.AssetHistoryReference]:
        transactions = get_asset_history(
            asset_id=asset_id, page=page, count=100, order="asc"
        )

        return transactions

    done = False
    num_calls = 0
    transactions: List[minswap.models.AssetHistoryReference] = []
    while not done and num_calls < max_calls:
        if num_calls + call_batch > max_calls:
            call_batch = max_calls - num_calls
        num_calls += call_batch
        logger.debug(f"Calling page range: {page}-{page+call_batch}")

        # Make the calls
        batch = get_history_batch(page)
        if len(batch) != 100:
            done = True

            if len(batch) == 0:
                break

        transactions.extend(batch)

        page += call_batch

    if len(transactions) > 0:
        logger.debug("Caching transactions.")

        # Convert data to a vaex dataframe
        df = pandas.DataFrame([d.model_dump() for d in transactions])

        # Define the output path
        cache_name = "history.arrow"
        path = cache_path.joinpath(cache_name)

        # If the cache exists, append to it
        if path.exists():
            cache_df = pandas.read_feather(path)
            tmp_path = path.with_name(path.name.replace(".arrow", "_temp.arrow"))
            threshold = len(cache) % 100
            filtered = df.iloc[threshold:]
            cache.close()
            if len(filtered) > 0:
                pandas.concat([cache_df, filtered], ignore_index=True).to_feather(
                    tmp_path
                )
                path.unlink()
                tmp_path.rename(path)

        # Otherwise, just dump the whole dataframe to cache
        else:
            df.to_feather(path)

    return num_calls


@minswap.utils.save_timestamp(ASSET_TRANSACTION_CACHE_PATH, 0, "asset_id")
def cache_transactions(asset_id: str, max_calls: int = 1000) -> int:
    """Cache transactions for an asset.

    This function will build up a local cache of transactions for a specific asset. The
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
    cache_path = ASSET_TRANSACTION_CACHE_PATH.joinpath(asset_id)
    now = datetime.utcnow()

    # Load existing cache
    cache = get_asset_transaction_cache(asset_id=asset_id)

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
        transactions = get_asset_transactions(
            asset_id=asset_id, page=page, count=100, order="asc"
        )

        return transactions

    with ThreadPoolExecutor(1) as executor:
        # with ThreadPoolExecutor(call_batch) as executor:
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


@minswap.utils.save_timestamp(ASSET_UTXO_CACHE_PATH, 0, "asset_id")
def cache_utxos(asset_id: str, max_calls: int = 1000) -> int:
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
    cache_path = ASSET_UTXO_CACHE_PATH.joinpath(asset_id)

    # blockfrost allows 10 calls/sec, with 500 call bursts with a 10 call/sec cooloff
    calls_allowed = 500

    # Load existing cache
    cache = get_asset_transaction_cache(asset_id=asset_id)
    if cache is None:
        return 0

    # Filter transactions to skip over previously cached data
    utxo_cache = get_utxo_cache(asset_id=asset_id)
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
                        executor.map(
                            minswap.utils.get_utxo, cache.tx_hash[bs:be].values
                        ),
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
                tx_utxos = minswap.utils._cache_utxos(tx_utxos, cache_path)

        if len(tx_utxos) > 0:
            logger.debug(
                "Caching transactions for "
                + f"{tx_utxos[0][0].as_py().year}"
                + f"{str(tx_utxos[0][0].as_py().month).zfill(2)}"
            )
            minswap.utils._cache_utxos(tx_utxos, cache_path)

    return num_calls


@minswap.utils.save_timestamp(ASSET_INFO_CACHE_PATH, 0, "asset_id")
def cache_history_transactions(asset_id: str, max_calls: int = 1000) -> int:
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
    cache_path = ASSET_INFO_CACHE_PATH.joinpath(asset_id)

    # blockfrost allows 10 calls/sec, with 500 call bursts with a 10 call/sec cooloff
    calls_allowed = 500

    # Load existing cache
    cache = get_asset_history_cache(asset_id=asset_id)
    if cache is None:
        return 0
    cached_txs = cache.tx_hash.values
    cache.close()

    # Filter transactions to skip over previously cached data
    utxo_cache = get_asset_history_transaction_cache(asset_id=asset_id)
    if utxo_cache is not None:
        init_length = len(cached_txs)
        cached_txs = list(set(cached_txs) - set(utxo_cache.hash.values))
        logger.debug(f"Found {init_length-len(cached_txs)} cached transactions.")
        utxo_cache.close()

    logger.debug(f"Need to get {len(cached_txs)} transactions.")

    # Batching values
    cached_txs = list(cached_txs)
    last_index = min(max_calls, len(cached_txs))
    batch_size = cpu_count()

    with ThreadPoolExecutor() as executor:
        num_calls = 0
        txs: List[minswap.models.Transaction] = []
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
            txs.extend(executor.map(get_asset_history_transactions, cached_txs[bs:be]))

        if len(txs) > 0:
            txs.sort(key=lambda x: x.block_time)
            while len(txs) > 0:
                logger.debug(
                    "Caching transactions for "
                    + f"{txs[0].block_time.year}"
                    + f"{str(txs[0].block_time.month).zfill(2)}"
                )
                txs = _cache_txs(txs, cache_path, use_hash=True)

    return num_calls

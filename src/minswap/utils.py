"""Utility functions."""
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Callable, List, Optional, Union

import blockfrost
import pandas
import vaex
from dotenv import load_dotenv

if TYPE_CHECKING:
    from minswap.models import PoolTransactionReference

logger = logging.getLogger(__name__)

CACHE_GLOB = "[0-9][0-9][0-9][0-9][0-9][0-9].arrow"

# Load the project information
load_dotenv()
PROJECT_ID = os.environ["PROJECT_ID"]
MAX_CALLS = int(os.environ["MAX_CALLS"])
call_lock = Lock()


class BlockfrostCallLimit(Exception):
    """Error when the Blockfrost call limit is reached."""


class BlockfrostBackend:
    """A class to enforce stall calls to Blockfrost when a rate limit is hit."""

    last_call: float = time.time()
    num_limit_calls: float = 0.0
    max_limit_calls: int = 500
    total_calls = 0
    max_total_calls = MAX_CALLS
    backoff_time: int = 10
    _api = blockfrost.BlockFrostApi(PROJECT_ID)

    @classmethod
    def remaining_calls(cls) -> int:
        """Remaining calls before rate limit."""
        return cls.max_total_calls - cls.total_calls

    @classmethod
    def reset_total_calls(cls) -> None:
        """Reset the call count."""
        cls.total_calls = 0

    @classmethod
    def _limiter(cls):
        with call_lock:
            cls.num_limit_calls += 1
            cls.total_calls += 1
            if cls.total_calls >= cls.max_total_calls:
                raise BlockfrostCallLimit(
                    f"Made {cls.total_calls}, "
                    + f"only {cls.max_total_calls} are allowed."
                )
            elif cls.num_limit_calls >= cls.max_limit_calls:
                logger.warning(
                    "At or near blockfrost rate limit. "
                    + f"Waiting {cls.backoff_time}s..."
                )
                time.sleep(cls.backoff_time)
                logger.info("Finished sleeping, resuming...")

        now = time.time()
        cls.num_limit_calls = max(0, cls.num_limit_calls - (now - cls.last_call) * 10)
        cls.last_call = now

    @classmethod
    def api(cls):
        """Blockfrost API with rate limits."""
        cls._limiter()
        return cls._api

    @classmethod
    def rate_limit(cls, func):
        """Wrap with rate limit.

        This can probably be removed. It might have utility in the future for
        customizing imposing rate limits on a function.

        """

        def wrapper(*args, **kwargs):
            cls._limiter()
            try:
                return func(*args, **kwargs)
            except blockfrost.ApiError:
                print(f"cls.num_limit_calls: {cls.num_limit_calls}")
                raise

        return wrapper


def save_timestamp(
    basepath: Path, arg_num: int, kwarg_key: str, func: Optional[Callable] = None
) -> Callable:
    """Timestamp cache decorator.

    Args:
        basepath: Path to save the timestamp to.
        arg_num: Argument position that should be used to append to basepath.
        kwarg_key: Alternative to arg_num, in case kwarg is supplied instead of an arg.
        func: Function to wrap. Defaults to None.

    Returns:
        Wrapped function that will have a timestamp dumped to disk.
    """
    if func is None:
        return lambda x: save_timestamp(
            basepath=basepath, arg_num=arg_num, kwarg_key=kwarg_key, func=x
        )

    def wrapper(*args, **kwargs):
        if len(args) - 1 >= arg_num:
            key = args[arg_num]
        else:
            key = kwargs[kwarg_key]

        if hasattr(key, "id"):
            identifier = key.id
        else:
            identifier = key

        path = basepath.joinpath(identifier)

        path.mkdir(exist_ok=True, parents=True)

        with open(path.joinpath("TIMESTAMP"), "w") as fw:
            fw.write(str(time.time()))

        return func(*args, **kwargs)

    return wrapper


def load_timestamp(path: Path) -> datetime:
    """Load a timestamp for a cache.

    Args:
        path: Path to cache.

    Returns:
        A datetime object for when the function was called.
    """
    with open(path.joinpath("TIMESTAMP")) as fr:
        timestamp = datetime.utcfromtimestamp(float(fr.read()))

    return timestamp


def _get_cache(cache_path: Path, glob: str = CACHE_GLOB) -> Optional[vaex.DataFrame]:
    if len(list(cache_path.glob(glob))) > 0:
        df = vaex.open(cache_path.joinpath(glob))
    else:
        df = None

    return df


def _cache_timestamp_data(
    data: Union[List[PoolTransactionReference], List[pandas.DataFrame]],
    cache_path: Path,
) -> Union[List[PoolTransactionReference], List[pandas.DataFrame]]:
    """Cache a list of objects.

    This is a utility function to cache a list of objects containing a timestamp. It
    searches the list for a change in the timestamp month, caches data for the first
    occurring month, and returns the rest.

    Args:
        data: A list of objects containing a time element.
        cache_path: The path to where the data should be stored.

    Raises:
        TypeError: `data` must be one of [PoolTransactionReference, pandas.DataFrame]

    Returns:
        _description_
    """
    # Convert data to a vaex dataframe
    if isinstance(data[0], PoolTransactionReference):
        if data[0].time.month == data[-1].time.month:
            index = len(data)
        else:
            for index in range(len(data) - 1):
                if data[index].time.month != data[index + 1].time.month:
                    index += 1
                    break
        df = pandas.DataFrame([d.dict() for d in data[:index]])
    elif isinstance(data[0], pandas.DataFrame):
        if data[0].time[0].month == data[-1].time[0].month:  # type: ignore
            index = len(data)
        else:
            for index in range(len(data) - 1):
                if data[index].time.month != data[index + 1].time.month:
                    index += 1
                    break
        df = pandas.concat(data, ignore_index=True)
    else:
        raise TypeError(
            "Transactions should be one of [pydantic.BaseModel, pandas.DataFrame]"
        )

    df["time"] = df.time.astype("datetime64[s]")

    # Define the output path
    cache_name = f"{df.time[0].year}" + f"{str(df.time[0].month).zfill(2)}.arrow"
    path = cache_path.joinpath(cache_name)

    # If the cache exists, append to it
    if path.exists():
        cache_df = pandas.read_feather(path)
        tmp_path = path.with_name(path.name.replace(".arrow", "_temp.arrow"))
        threshold = cache_df.time.astype("datetime64[s]").values[-1]
        filtered = df[df.time > threshold]
        logger.info(len(filtered))
        if len(filtered) > 0:
            pandas.concat([cache_df, filtered], ignore_index=True).to_feather(tmp_path)
            path.unlink()
            tmp_path.rename(path)

    # Otherwise, just dump the whole dataframe to cache
    else:
        df.to_feather(path)

    return data[index:]

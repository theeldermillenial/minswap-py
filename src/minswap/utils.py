"""Utility functions."""
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Callable, Optional

import blockfrost
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load the project information
load_dotenv()
PROJECT_ID = os.environ["PROJECT_ID"]
MAX_CALLS = int(os.environ["MAX_CALLS"])
call_lock = Lock()


class BlockfrostCallLimit(Exception):
    pass


class BlockfrostBackend:
    last_call: float = time.time()
    num_limit_calls: float = 0.0
    max_limit_calls: int = 500
    total_calls = 0
    max_total_calls = MAX_CALLS
    backoff_time: int = 10
    api = blockfrost.BlockFrostApi(PROJECT_ID)

    @classmethod
    def limiter(cls, func):
        def wrapper(*args, **kwargs):
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
            cls.num_limit_calls = max(
                0, cls.num_limit_calls - (now - cls.last_call) * 10
            )
            cls.last_call = now

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
            path = basepath.joinpath(args[arg_num])
        else:
            path = basepath.joinpath(kwargs[kwarg_key])

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

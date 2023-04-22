"""Utility functions."""
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


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

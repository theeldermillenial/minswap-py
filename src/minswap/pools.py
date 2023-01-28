"""Functions for processing minswap pools.

- `get_pools()` - Get a list of all pools.
"""
from typing import Any, Dict

import blockfrost
from dotenv import dotenv_values

from minswap import addr, models


def get_pools() -> Dict[str, Any]:
    """Get a list of all pools.

    Args:
        order: Sort. Defaults to "asc".

    Returns:
        _type_: _description_
    """
    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    utxos = api.address_utxos(
        addr.POOL.address.encode(), gather_pages=True, order="asc"
    )

    utxos = models.to_dict(utxos)

    return utxos


if __name__ == "__main__":

    import pprint

    pprint.pprint(get_pools())

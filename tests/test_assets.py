import pytest

import minswap.assets as assets
import minswap.pools as pools

test_pools = pools.get_pools()
if isinstance(test_pools, tuple):
    test_pools, _ = test_pools


@pytest.mark.parametrize("pool", test_pools, ids=[p.id for p in test_pools])
def test_get_ticker(pool: pools.PoolState):

    asset_a = assets.asset_ticker(pool.unit_a)
    assert asset_a is not None

    asset_b = assets.asset_ticker(pool.unit_b)
    assert asset_b is not None

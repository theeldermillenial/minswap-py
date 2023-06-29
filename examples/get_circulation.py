# mypy: ignore-errors
from decimal import Decimal

import minswap.assets as assets
import minswap.pools as pools

test_pools = {
    "ADA-MIN": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
    "ADA-AGIX": "620719c204a0338059aad43b35332b9353216c719901c8ca9f726ae4ec313da5",
    "ADA-NTX": "face3a0164da55d1627cd6af895a9a0cd4e4edc110632d407494644e3c924937",
    "ADA-LQ": "1b7f4abbf3eb04f8a7e5fbbc2042c524210dd960b6703a02fe52f70a7701e284",
    "ADA-MELD": "39d5a91060c49be0b39c1c59b15bee45a7817d05737c5eaa8842f8fbda0c2aee",
    "ADA-HOSKY": "11e236a5a8826f3f8fbc1114df918b945b0b5d8f9c74bd383f96a0ea14bffade",
}

for pair, pool_id in test_pools.items():
    pool = pools.get_pool_by_id(pool_id)

    price = pool.price[0]

    decimals = Decimal(10 ** assets.asset_decimals(pool.unit_b))

    locked, minted = assets.circulating_asset(pool.unit_b)

    circulating = Decimal(minted.quantity() - locked.quantity()) / decimals

    print(f"{pair.split('-')[-1]} minted: " + f"{minted.quantity()/decimals:,.0f}")

    print(f"{pair.split('-')[-1]} circulating: " + f"{circulating:,.0f}")

    print(f"{pair.split('-')[-1]} market cap: " + f"{price * circulating:,.0f} ₳")

    print()

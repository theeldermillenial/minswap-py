# mypy: ignore-errors

import pprint

import minswap.pools as pools

test_pools = {
    "ADA-MIN": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
    "ADA-AGIX": "620719c204a0338059aad43b35332b9353216c719901c8ca9f726ae4ec313da5",
    "ADA-NTX": "face3a0164da55d1627cd6af895a9a0cd4e4edc110632d407494644e3c924937",
    "ADA-LQ": "1b7f4abbf3eb04f8a7e5fbbc2042c524210dd960b6703a02fe52f70a7701e284",
    "ADA-MELD": "39d5a91060c49be0b39c1c59b15bee45a7817d05737c5eaa8842f8fbda0c2aee",
    "ADA-HOSKY": "11e236a5a8826f3f8fbc1114df918b945b0b5d8f9c74bd383f96a0ea14bffade",
}

pool_id = test_pools["ADA-MIN"]

# Get the 5 most recent pool state hashes
history = pools.get_pool_history(pool_id, count=5)

# Get the transaction information for each of the pool history snapshots
for state in history:
    in_state, out_state = pools.get_pool_in_tx(state.tx_in.tx_hash, return_input=True)

    print(state.time)
    pprint.pprint(in_state.dict(), indent=2)
    pprint.pprint(out_state.dict(), indent=2)

# mypy: ignore-errors
import pprint
from datetime import datetime

import blockfrost
import pandas
from dotenv import dotenv_values

import minswap.pools as pools

env = dotenv_values()
api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

test_pools = {
    "ADA-MIN": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
    "ADA-AGIX": "620719c204a0338059aad43b35332b9353216c719901c8ca9f726ae4ec313da5",
    "ADA-NTX": "face3a0164da55d1627cd6af895a9a0cd4e4edc110632d407494644e3c924937",
    "ADA-LQ": "1b7f4abbf3eb04f8a7e5fbbc2042c524210dd960b6703a02fe52f70a7701e284",
    "ADA-MELD": "39d5a91060c49be0b39c1c59b15bee45a7817d05737c5eaa8842f8fbda0c2aee",
    "ADA-HOSKY": "11e236a5a8826f3f8fbc1114df918b945b0b5d8f9c74bd383f96a0ea14bffade",
}

pool_id = test_pools["ADA-MIN"]

history = pools.get_pool_transactions(
    pool_id, start_date=datetime(2023, 1, 30), stop_date=datetime(2023, 2, 1)
)

pprint.pprint(history)
quit()

# Get the 5 most recent pool state hashes
history = pools.get_pool_transaction_history(pool_id, count=1)

# Get the transaction information for each of the pool history snapshots
for state in history:
    tx = api.transaction_utxos(state.tx_in.tx_hash, return_type="json")

    pprint.pprint(tx, indent=2)

    df = (
        pandas.concat(
            [pandas.DataFrame(tx["inputs"]), pandas.DataFrame(tx["outputs"])],
            keys=["input", "output"],
        )
        .reset_index(level=0)
        .reset_index(drop=True)
    )

    df.rename(columns={"level_0": "side"}, inplace=True)
    df["hash"] = tx["hash"]
    df["time"] = state.time

    print(df)

    # script_data = to_dict(api.script_datum(df.data_hash[1]))
    # pprint.pprint(script_data, indent=2)

    # print(Address(bech32=df.address[0]))
    for row in df.amount:
        print(row)

    # print(
    #     cbor2.loads(
    #         script_data["json_value"]["fields"][0]["fields"][0]["fields"][0][
    #             "bytes"
    #         ].encode()
    #     )
    # )

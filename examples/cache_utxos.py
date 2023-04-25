# mypy: ignore-errors
import logging
import time

import numpy as np
import pandas

import minswap.transactions as transactions
from minswap.assets import asset_ticker
from minswap.pools import get_pools

# Just to see what minswap-py is doing under the hood...
logging.getLogger("minswap").setLevel(logging.DEBUG)

# Maximum number of API calls allowed for this script to run
# If only using this to update transactions once per day, and it's the only code using
# Blockfrost, this can be set to 50,000 for a free account.
max_calls = 15000
total_calls = 0

# Get a list of pools
pools = get_pools()
assert isinstance(pools, list)

# cache = transactions.get_utxo_cache(
#     "952375ca2d04785ff968847082f47feebf3cf957aa5208d617fad7ddb1c437cb"
# )

# print(cache.head())
# print(cache.tail())
# print(len(cache))

# print((cache.time[:-1].as_numpy().values - cache.time[1:].as_numpy().values).dtype)

# print(
#     np.argwhere(
#         (cache.time[1:].as_numpy().values - cache.time[:-1].as_numpy().values).astype(
#             np.float32
#         )
#         < 0
#     )
# )

# assert not (
#     (cache.time[1:].as_numpy().values - cache.time[:-1].as_numpy().values).astype(
#         np.float32
#     )
#     < 0
# ).any()

# print(cache.time[60:70].values)

# cache = transactions.get_transaction_cache(
#     "952375ca2d04785ff968847082f47feebf3cf957aa5208d617fad7ddb1c437cb"
# )
# print(cache.time.values)

# quit()

for ind, pool in enumerate(pools[900:]):
    if total_calls >= max_calls:
        print("Reached maximum requests. Exiting script.")
        break

    print(
        f"{ind} - Gettings historical UTXOs for pool: {asset_ticker(pool.unit_a)}-{asset_ticker(pool.unit_b)}"
    )

    calls = transactions.cache_utxos(pool.id, max_calls)

    cooloff = min(50, calls / 10)
    print(f"Made {calls} calls. Cooling off {cooloff:0.2f}s before starting next pool")
    time.sleep(cooloff)

    cache = transactions.get_utxo_cache(pool.id)

    if cache is None:
        continue

    assert not (
        (cache.time[1:].as_numpy().values - cache.time[:-1].as_numpy().values).astype(
            np.float32
        )
        < 0
    ).any()

    total_calls += calls

    # if total_calls > 0:
    #     break

quit()

history = transactions.get_transaction_cache(pool_id)

# Get the transaction information for each of the pool history snapshots
for index, state in history.tail(1).iterrows():
    print(state)
    # tx = api.transaction_utxos(state["tx_hash"], return_type="json")

    # pprint.pprint(tx, indent=2)

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
    df["time"] = state["time"]

    print(df)

    print(df.data_hash[0])
    # script_data = api.script_datum(df.data_hash[0], return_type="json")
    # pprint.pprint(script_data, indent=2)

    # assets = Assets()
    # for index, row in df.iterrows():
    #     values = Assets(**{v["unit"]: v["quantity"] for v in row.amount})
    #     if row.side == "input":
    #         assets = assets + values
    # print(assets)

    # import pycardano

    # pprint.pprint(pycardano.plutus.PlutusData.from_dict(script_data["json_value"]))

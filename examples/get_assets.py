# mypy: ignore-errors
from minswap import assets, pools

pools = pools.get_pools()

with open("pool_id.csv", "w") as fw:
    fw.write("pair,id\n")
    for pool in pools:
        asset_a = assets.asset_ticker(pool.unit_a)
        asset_b = assets.asset_ticker(pool.unit_b)

        try:
            fw.write(f"{asset_a}\\{asset_b},{pool.id}\n")
        except:
            print(f"Error with: {asset_a}\\{asset_b},{pool.id}\n")

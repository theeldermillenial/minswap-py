# mypy: ignore-errors
import minswap.pools as pools

all_pools = pools.get_pools()

print(f"Number of pools: {len(all_pools)}")

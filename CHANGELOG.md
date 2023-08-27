# Change

### 0.3.4

* Fixed a bug for non-ADA pools that was improperly ordering assets in the pool.

### 0.3.3

* Fixed a bug in how the NFT policy IDs were being checked when restoring a PoolState from JSON.

### 0.3.2

* Small change to fetching pool data, removing hard coded pool addresses and how pool addresses are fetched by finding addresses that contain the Minswap DEX NFT.
* Small change to how swap exact in/out are calculated, where fees are now a PoolState property.

### 0.3.1

* Modified `PoolState` initialization so that `PoolDatum` is not automatically queried from blockfrost. This was causing a large number of calls to be generated to Blockfrost when using `pools.get_pools()` (one for each of the 3,000+ pools). Now, the `lp_total` and `root_k_last` are only retrieved from Blockfrost when requested.

## 0.3.0

Improvements:
1. Added wallet support, including easy methods to create a collateral, send funds, and consolidate UTxOs.
2. Swap transactions.
3. Cancel transactions.
4. Deposit liquidity (including zap in), but should be updated in the future. The Minswap team has not responded to requests for details on how to better estimate expected LP based on amount of token deposited.
https://github.com/minswap/sdk/pull/7#discussion_r1279439474

Changes:
1. There was an inconsistency in how time values were being cached. See the `examples/rename_time.py` for a way to translate previously cached data to the new standard.
2. Changed the way some of the underlying classes were managing amounts to use the `Assets` class. This makes combining assets from different UTxOs easier.

## v0.2.1

There are now multiple Minswap pool addresses. This patch updates the code to read all of them when using `minswap.get_pools` and subsequent functions that require pool addresses.

## v0.2.0
### Improvements
1. Added a blockfrost rate limiter. Every call to blockfrost adds to a time delayed counter, and prevents running into rate limits. This helps to prevent abuse of the blockfrost API.
2. Added significant transaction and asset functionality. It is now possible to pull in asset historys.

### Breaking changes
1. There was an inconsistency in how time values were being cached. See the `examples/rename_time.py` for a way to translate previously cached data to the new standard.

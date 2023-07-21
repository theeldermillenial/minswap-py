# Change

## v0.3.0

* Added wallet class.
* Added ability to send lovelace to an address from a wallet object.

## v0.2.1

There are now multiple Minswap pool addresses. This patch updates the code to read all of them when using `minswap.get_pools` and subsequent functions that require pool addresses.

## v0.2.0
### Improvements
1. Added a blockfrost rate limiter. Every call to blockfrost adds to a time delayed counter, and prevents running into rate limits. This helps to prevent abuse of the blockfrost API.
2. Added significant transaction and asset functionality. It is now possible to pull in asset historys.

### Breaking changes
1. There was an inconsistency in how time values were being cached. See the `examples/rename_time.py` for a way to translate previously cached data to the new standard.

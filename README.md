# minswap-py (v0.4.0-dev0)
<p align="center">
    <img src="https://img.shields.io/pypi/status/minswap-py?style=flat-square" />
    <img src="https://img.shields.io/pypi/dm/minswap-py?style=flat-square" />
    <img src="https://img.shields.io/pypi/l/minswap-py?style=flat-square"/>
    <img src="https://img.shields.io/pypi/v/minswap-py?style=flat-square"/>
    <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

`minswap-py` is a tool to interact with [Minswap](https://minswap.org/).  The current version has feature parity with the minswap [blockfrost-adapter](https://github.com/minswap/sdk), except for the ability to remove liquidity.

Documentation and additional features coming soon.


## Changelog

Be sure to check out the `CHANGELOG.md` for a complete history of changes. This section
only contains patch updates for the current minor version and patches.

### 0.4.0-dev0

* Fixed a bug for non-ADA pools that was improperly ordering assets in the pool.

### 0.3.3

* Fixed a bug in how the NFT policy IDs were being checked when restoring a PoolState from JSON.

### 0.3.2

* Small change to fetching pool data, removing hard coded pool addresses and how pool addresses are fetched by finding addresses that contain the Minswap DEX NFT.
* Small change to how swap exact in/out are calculated, where fees are now a PoolState property.

### 0.3.1

* Modified `PoolState` initialization so that `PoolDatum` is not automatically queried from blockfrost. This was causing a large number of calls to be generated to Blockfrost when using `pools.get_pools()` (one for each of the 3,000+ pools). Now, the `lp_total` and `root_k_last` are only retrieved from Blockfrost when requested.

### 0.3.0

Improvements:
1. Added wallet support, including easy methods to create a collateral, send funds, and consolidate UTxOs.
2. Swap transactions.
3. Cancel transactions.
4. Deposit liquidity (including zap in), but should be updated in the future. The Minswap team has not responded to requests for details on how to better estimate expected LP based on amount of token deposited.
https://github.com/minswap/sdk/pull/7#discussion_r1279439474

Changes:
1. There was an inconsistency in how time values were being cached. See the `examples/rename_time.py` for a way to translate previously cached data to the new standard.
2. Changed the way some of the underlying classes were managing amounts to use the `Assets` class. This makes combining assets from different UTxOs easier.

### Installation

In order to use this package:
1. Install with `pip install minswap-py`
2. Sign up for blockfrost and get an API key.
3. In your working directory, create a `.env` file. The `.env` should have the following fields:
```bash
# The blockfrost project id
PROJECT_ID=

# The maximum number of calls allowed to Blockfrost within a session.
MAX_CALLS=45000

# Must be one of mainnet or preprod
NETWORK=mainnet
```
4. Browse the `examples` folder for use cases.

### Setup your wallet

To use a wallet with `minswap-py`, you will need to supply a mnemonic in a file on your
hard drive. By default, the `Wallet` class will create a brand new wallet and store it
in `.wallet/{chain}_mnemonic.txt`. Then, this wallet will be used every time a new
`Wallet` object is created. The `chain` in the file name must either be `mainnet` or
`preprod` depending on what `NETWORK` you want to operate on (i.e.
`mainnet_mnemonic.txt` or `preprod_mnemonic.txt`).

If you want to supply your own wallet, you can either replace the mnemonic in
`.wallets/{chain}_mnemonic.txt` if it exists, or create it. You can also pass the
mnemonic directly into the `Wallet`. Alternatively, you can create your own `txt` file
with the mnemonic in it anywhere on disk, and pass that file to the `Wallet`
constructor.

Examples:
```python
from minswap.wallets import Wallet

# Initialize with a path to a file containing a mnemonic
wallet = Wallet(path="path/to/mnemonic.txt")

# Initialize with a mnemonic
wallet = Wallet(mnemonic="bert says buy flac")
```

## Have a question?

Reach out to me on the Minswap discord. You can usually find me on`#technical`, and I'm happy to respond to questions there.

https://discord.com/channels/829060079248343090/845208119729979402

## Support

If you find this project useful, please consider supporting the project by buying me a
beer in the form of ADA or MIN:

```bash
addr1q9hw8fuex09vr3rqwtn4fzh9qxjlzjzh8aww684ln0rv0cfu3f0de6qkmh7c7yysfz808978wwe6ll30wu8l3cgvgdjqa7egnl
```

## Use Cases

This tool was recently used to help generate data for the Minswap DAO Emissions and
Treasury report. You can read the report here:

https://minswap.org/storage/2023/06/31-3-2023_Emissions_and_Treasury_Report.pdf

# Contributors

A special thanks to Farmer, creator of Farmbot, for assisting me working through the
details of swaps and cancel orders. If you would like to learn more about Farmbot,
check out their discord:

https://discord.gg/zQHyJKrA7K

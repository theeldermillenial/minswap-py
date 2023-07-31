import os
import time

from dotenv import load_dotenv

from minswap.models import Assets
from minswap.pools import get_pool_by_id
from minswap.wallets import Wallet

load_dotenv()

NETWORK = os.environ["NETWORK"]

MIN_POLICY = "e16c2dc8ae937e8d3790c7fd7168d7b994621ba14ca11415f39fed724d494e"

if NETWORK == "mainnet":
    ADAMIN = "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2"
elif NETWORK == "preprod":
    ADAMIN = "3bb0079303c57812462dec9de8fb867cef8fd3768de7f12c77f6f0dd80381d0d"
else:
    raise ValueError(
        "The network environment variable must be one of ['main', 'preprod']"
    )
adamin_pool = get_pool_by_id(ADAMIN)
assert adamin_pool is not None

print(f"ADA/MIN price: {adamin_pool.price[0]}")

wallet = Wallet()

print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

print()
print(f"Swapping 50 ADA for MIN...")
in_asset = Assets(lovelace=50000000)
tx = wallet.swap(pool=ADAMIN, in_assets=in_asset)
signed_tx = wallet.sign(tx)
tx_hash = wallet.submit(signed_tx)

while tx_hash not in [utxo.tx_hash for utxo in wallet.utxos]:
    print("Waiting for transaction to be processed, waiting 10 seconds...")
    time.sleep(10)

"""Need to add a pause, or method to wait for swap to complete. For now, just wait."""
print()
print("Pausing for 60 seconds to wait for transaction to complete...")
time.sleep(60)

print()
print("UTXOs:")
in_asset = Assets(**{MIN_POLICY: 0})
for utxo in wallet.utxos:
    print(utxo.dict())
    in_asset.__root__[MIN_POLICY] += utxo.amount[MIN_POLICY]

print()
print(f"Swapping {in_asset[MIN_POLICY]} MIN for ADA...")
tx = wallet.swap(pool=ADAMIN, in_assets=in_asset)
signed_tx = wallet.sign(tx)
tx_hash = wallet.submit(signed_tx)

while tx_hash not in [utxo.tx_hash for utxo in wallet.utxos]:
    print("Swap has not been processed, waiting 10 seconds...")
    time.sleep(10)

print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

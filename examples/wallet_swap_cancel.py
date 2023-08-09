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
print(f"Swapping 50 ADA for 50 MIN...")
in_asset = Assets(lovelace=50000000)
out_asset = Assets(**{adamin_pool.unit_b: 50000000})
tx = wallet.swap(pool=ADAMIN, in_assets=in_asset, out_assets=out_asset)
signed_tx = wallet.sign(tx)
order = wallet.submit(signed_tx)

utxos = [utxo.tx_hash for utxo in wallet.utxos]
while str(order.transaction.id) not in utxos:
    print(utxos)
    print("Waiting for transaction to be processed, waiting 10 seconds...")
    time.sleep(10)
    utxos = [utxo.tx_hash for utxo in wallet.utxos]

print()
print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

print()
print(f"Cancelling swap...")
tx = wallet.cancel(order=order)
signed_tx = wallet.sign(tx)
tx_hash = wallet.submit(signed_tx)

while str(tx_hash.transaction.id) not in [utxo.tx_hash for utxo in wallet.utxos]:
    print("Cancel has not been processed, waiting 10 seconds...")
    time.sleep(10)

print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

import os
import time

from dotenv import load_dotenv

from minswap.models import Assets
from minswap.pools import get_pool_by_id
from minswap.wallets import Wallet

load_dotenv()

NETWORK = os.environ["NETWORK"]

if NETWORK == "main":
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
collateral = wallet.collateral
print("Collateral (None if there is no collateral UTXO):")
print(collateral)

if collateral is None:
    print()
    print("Could not find collateral. Creating collateral generation transaction.")

    tx = wallet.make_collateral_tx()
    signed_tx = wallet.sign(tx)
    tx_hash = wallet.submit(signed_tx)

    while tx_hash not in [utxo.tx_hash for utxo in wallet.utxos]:
        print("Collateral is not yet available, checking again in 5s...")
        time.sleep(5)

    print("Newly created collateral:")
    print(wallet.collateral)

print()
print("Swapping ADA for MIN...")
in_asset = Assets(lovelace=50000000)
tx = wallet.swap(pool=ADAMIN, in_assets=in_asset)
signed_tx = wallet.sign(tx)
# print(signed_tx.transaction_body)
# print(signed_tx)
# quit()
tx_hash = wallet.submit(signed_tx)

while tx_hash not in [utxo.tx_hash for utxo in wallet.utxos]:
    print("Consolidated utxo has not been processed, waiting 5 seconds...")
    time.sleep(5)

print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

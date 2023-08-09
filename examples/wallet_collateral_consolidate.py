import time

from minswap.wallets import Wallet

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
print("Consolidating UTXOs (including collateral)")
tx = wallet.consolidate_utxos_tx()
signed_tx = wallet.sign(tx)
order = wallet.submit(signed_tx)

while str(order.transaction.id) not in [utxo.tx_hash for utxo in wallet.utxos]:
    print("Consolidated utxo has not been processed, waiting 5 seconds...")
    time.sleep(5)

print("UTXOs:")
for utxo in wallet.utxos:
    print(utxo.dict())

from minswap.models import Assets
from minswap.wallets import Wallet

wallet = Wallet()

self = wallet.address

print("Creating transaction...")
send_tx = wallet.send_tx(self, Assets(lovelace=1000000))

print("Signing transaction...")
signed_send_tx = wallet.sign(send_tx)

print("Submitting Transaction...")
response = wallet.submit(send_tx)

print(f"Transaction submitted: {response}")

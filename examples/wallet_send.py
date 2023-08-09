"""
This example will send 10 ADA to Elder Millenial by default. Executing the script as is
supports the project.

If you would like to just send ADA to yourself for testing, change the SEND_TO address
to a different address.
"""
from minswap.models import Assets
from minswap.wallets import Wallet

# Create a wallet. By default,
wallet = Wallet()

self = wallet.address

print("Creating transaction...")
send_tx = wallet.send_tx(self, Assets(lovelace=10000000))

print("Signing transaction...")
signed_send_tx = wallet.sign(send_tx)

print("Submitting Transaction...")
order = wallet.submit(send_tx)

print(f"Transaction id: {order.transaction.id}")

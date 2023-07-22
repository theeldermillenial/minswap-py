"""Methods for wallets including building, signing, and submitting transactions."""
import os
from pathlib import Path
from typing import List, Optional, Union

import pycardano
from pydantic import BaseModel, Field, root_validator

import minswap.utils
from minswap.models import Address, AddressUtxoContent, AddressUtxoContentItem, Assets


class Wallet(BaseModel):
    """A wallet handling class."""

    mnemonic: str = Field(default_factory=pycardano.HDWallet.generate_mnemonic)
    path: Path = Path(
        f".wallet/{os.environ.get('NETWORK','mainnet').lower()}_mnemonic.txt"
    )
    hdwallet: Optional[pycardano.HDWallet]

    class Config:  # noqa
        arbitrary_types_allowed = True

    @root_validator
    def _validator(cls, values):
        if Path(values["path"]).exists():
            with open(Path(values["path"])) as fr:
                values["mnemonic"] = fr.read()
        else:
            Path(values["path"]).parent.mkdir(exist_ok=True)
            with open(Path(values["path"]), mode="w") as fw:
                fw.write(values["mnemonic"])

        values["hdwallet"] = pycardano.HDWallet.from_mnemonic(values["mnemonic"])

        return values

    @property
    def payment_signing_key(self):
        """The payment signing key."""
        hdwallet_spend = self.hdwallet.derive_from_path("m/1852'/1815'/0'/0/0")
        return pycardano.ExtendedSigningKey.from_hdwallet(hdwallet_spend)

    @property
    def payment_verification_key(self):
        """The payment verification key."""
        return self.payment_signing_key.to_verification_key()

    @property
    def stake_signing_key(self):
        """The stake signing key."""
        hdwallet_stake = self.hdwallet.derive_from_path("m/1852'/1815'/0'/2/0")
        return pycardano.ExtendedSigningKey.from_hdwallet(hdwallet_stake)

    @property
    def stake_verification_key(self):
        """The stake verification key."""
        return self.stake_signing_key.to_verification_key()

    @property
    def address(self) -> Address:
        """The first wallet address. Acts as a single address wallet."""
        net_env = os.environ.get("NETWORK", "mainnet").lower()
        if net_env == "mainnet":
            network = pycardano.Network.MAINNET
        else:
            network = pycardano.Network.TESTNET

        return Address(
            bech32=pycardano.Address(
                self.payment_verification_key.hash(),
                self.stake_verification_key.hash(),
                network=network,
            ).encode()
        )

    @property
    def utxos(self) -> AddressUtxoContent:
        """Get the UTXOs of the wallet."""
        utxos = AddressUtxoContent.parse_obj(
            minswap.utils.BlockfrostBackend.api().address_utxos(
                address=self.address.bech32, return_type="json"
            )
        )

        return utxos

    @property
    def collateral(self) -> Optional[AddressUtxoContentItem]:
        """Search for a UTXO that can be used for collateral. None if none available."""
        for utxo in self.utxos:
            if "lovelace" in utxo.amount and len(utxo.amount) == 1:
                if (
                    utxo.amount["lovelace"] >= 5000000
                    and utxo.amount["lovelace"] <= 20000000
                ):
                    return utxo

        return None

    def make_collateral_tx(self):
        """Create a collateral creation transaction."""
        return self.send(self.address, Assets(lovelace="5000000"), "create collateral")

    def consolidate_utxos_tx(self, ignore_collateral=True):
        """Create a UTXO collection tx.

        To help keep a tidy wallet, it is useful to send all UTXOs to the same address
        to merge the UTXOs. This is especially helpful when holding non-ADA tokens that
        contain locked ADA.

        By default, a collateral UTXO is identified and excluded from the consolidation
        process. This can be overriden with `ignore_collateral=True`. In general, it
        is recommended to keep a collateral, and this is largely a function input for
        testing purposes.

        Args:
            ignore_collateral: Ignore collateral when consolidating. Defaults to True.
        """
        collateral = self.collateral
        ignore_collateral = ignore_collateral & (collateral is not None)
        consolidated = Assets()
        for utxo in self.utxos:
            if (
                ignore_collateral
                and utxo.tx_hash == collateral.tx_hash
                and utxo.output_index == collateral.output_index
            ):
                continue
            consolidated += utxo.amount
        tx = self.send(
            self.address,
            consolidated,
            "consolidate utxos",
            ignore_collateral=ignore_collateral,
        )
        last_output = tx.transaction_body.outputs.pop()
        tx.transaction_body.outputs[0].amount.coin += last_output.amount.coin
        tx.transaction_witness_set = pycardano.TransactionWitnessSet(
            pycardano.txbuilder.FAKE_VKEY, pycardano.txbuilder.FAKE_TX_SIGNATURE
        )
        tx = self._build_and_check(tx)
        tx.transaction_witness_set = pycardano.TransactionWitnessSet()

        return tx

    def _msg(self, msg: List[Optional[str]] = []):
        """Create a metadata message.

        This follows CIP20.

        https://cips.cardano.org/cips/cip20/
        """
        message = {674: {"msg": [f"minswap-py: {minswap.__version__}"]}}
        for m in msg:
            if m is not None:
                message[674]["msg"].append(m)
        metadata = pycardano.AuxiliaryData(
            data=pycardano.AlonzoMetadata(metadata=pycardano.Metadata(message))
        )

        return metadata

    def _fee(self, tx: Union[bytes, int]):
        """Calculate the transaction fee."""
        if isinstance(tx, bytes):
            tx = len(tx)
        parameters = minswap.utils.BlockfrostBackend.protocol_parameters()

        return int(parameters.min_fee_a * tx + parameters.min_fee_b)

    def _max_fees(self):
        """Return the max possible transaction fee."""
        parameters = minswap.utils.BlockfrostBackend.protocol_parameters()
        return self._fee(parameters.max_tx_size)

    def _build_and_check(self, tx: pycardano.Transaction):
        """Build transaction and update transaction fee.

        Precisely calculate the transaction fee. Since the size of the fee could
        influence the fee itself, this recursively calculates the fee until the fee
        settles on the lowest possible fee.
        """
        fee = self._fee(tx.to_cbor("bytes"))
        tx_body = tx.transaction_body
        while fee != tx.transaction_body.fee:
            tx_body.outputs[-1].amount.coin += tx_body.fee
            tx_body.fee = fee
            tx_body.outputs[-1].amount.coin -= tx_body.fee
            fee = self._fee(tx.to_cbor("bytes"))

        return tx

    def send(
        self,
        address: Address,
        amount: Assets,
        msg: Optional[str] = None,
        ignore_collateral=True,
    ):
        """Create a send transaction. Does not actually submit the transaction.

        For now, this function only sends lovelace (ADA) to an address.
        """
        utxos = AddressUtxoContent.parse_obj(
            minswap.utils.BlockfrostBackend.api().address_utxos(
                self.address.bech32, return_type="json"
            )
        )

        message = self._msg(["send", msg])

        # Placeholder fee
        fee = self._max_fees()

        # Gather UTXOs for input
        tx_in = []
        send_assets = minswap.models.Assets(lovelace=-fee)
        collateral = self.collateral
        ignore_collateral = ignore_collateral & (collateral is not None)
        for utxo in utxos:
            if (
                ignore_collateral
                and utxo.tx_hash == collateral.tx_hash  # type: ignore
                and utxo.output_index == collateral.output_index  # type: ignore
            ):
                continue

            for unit, quantity in utxo.amount.items():
                if quantity > 0 and send_assets[unit] < amount[unit]:
                    send_assets += utxo.amount
                    tx_in.append(
                        pycardano.TransactionInput.from_primitive(
                            [utxo.tx_hash, utxo.output_index]
                        )
                    )
                    break
        send_assets.__root__["lovelace"] += fee

        # Create UTXOs
        tx_out: List[pycardano.TransactionOutput] = [
            pycardano.TransactionOutput(address.address, amount["lovelace"]),
            pycardano.TransactionOutput(
                self.address.address,
                int(send_assets["lovelace"] - amount["lovelace"] - fee),
            ),
        ]

        # Update fee
        tx_body = pycardano.TransactionBody(
            inputs=tx_in,
            outputs=tx_out,
            auxiliary_data_hash=message.hash(),
            fee=fee,
        )
        tx = pycardano.Transaction(
            tx_body,
            pycardano.TransactionWitnessSet(
                pycardano.txbuilder.FAKE_VKEY, pycardano.txbuilder.FAKE_TX_SIGNATURE
            ),
            auxiliary_data=message,
        )
        tx = self._build_and_check(tx)

        tx.transaction_witness_set = pycardano.TransactionWitnessSet()

        return tx

    def sign(self, tx: pycardano.Transaction):
        """Sign a transaction."""
        signature = self.payment_signing_key.sign(tx.transaction_body.hash())
        witness = pycardano.TransactionWitnessSet(
            [pycardano.VerificationKeyWitness(self.payment_verification_key, signature)]
        )

        # build transaction
        tx = pycardano.Transaction(
            tx.transaction_body, witness, auxiliary_data=tx.auxiliary_data
        )

        return tx

    def submit(self, tx: pycardano.Transaction):
        """Submit a transaction."""
        path = Path(".tx").joinpath(str(tx.id) + ".cbor")
        path.parent.mkdir(exist_ok=True)
        with open(path, "wb") as fw:
            fw.write(tx.to_cbor(encoding="bytes"))

        response = minswap.utils.BlockfrostBackend.api().transaction_submit(
            str(path), return_type="json"
        )

        return response
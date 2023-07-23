"""Methods for wallets including building, signing, and submitting transactions."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

import blockfrost
import pycardano
from pydantic import BaseModel, Field, root_validator

import minswap.addr
import minswap.pools
import minswap.utils
from minswap.models import Address, AddressUtxoContent, AddressUtxoContentItem, Assets

ORDER_SCRIPT = pycardano.PlutusV1Script(
    bytes.fromhex(
        "59014f59014c01000032323232323232322223232325333009300e30070021323233533300b33"
        + "70e9000180480109118011bae30100031225001232533300d3300e22533301300114a02a666"
        + "01e66ebcc04800400c5288980118070009bac3010300c300c300c300c300c300c300c007149"
        + "858dd48008b18060009baa300c300b3754601860166ea80184ccccc0288894ccc0400044008"
        + "4c8c94ccc038cd4ccc038c04cc030008488c008dd718098018912800919b8f0014891ce1317"
        + "b152faac13426e6a83e06ff88a4d62cce3c1634ab0a5ec133090014a0266008444a00226600"
        + "a446004602600a601a00626600a008601a006601e0026ea8c03cc038dd5180798071baa300f"
        + "300b300e3754601e00244a0026eb0c03000c92616300a001375400660106ea8c024c020dd50"
        + "00aab9d5744ae688c8c0088cc0080080048c0088cc00800800555cf2ba15573e6e1d200201"
    )
)


BATCHER_FEE = 2000000
DEPOSIT = 2000000


@dataclass
class PlutusPartAddress(pycardano.PlutusData):
    """Encode a plutus address part (i.e. payment, stake, etc)."""

    CONSTR_ID = 0
    address: bytes


@dataclass
class PlutusNone(pycardano.PlutusData):
    """Placeholder for a receiver datum."""

    CONSTR_ID = 1


@dataclass
class _PlutusConstrWrapper(pycardano.PlutusData):
    """Hidden wrapper to match Minswap stake address constructs."""

    CONSTR_ID = 0
    wrapped: Union["_PlutusConstrWrapper", PlutusPartAddress]


@dataclass
class PlutusFullAddress(pycardano.PlutusData):
    """A full address, including payment and staking keys."""

    CONSTR_ID = 0
    payment: PlutusPartAddress
    stake: _PlutusConstrWrapper

    @classmethod
    def from_address(cls, address: Address):
        """Parse an Address object to a PlutusFullAddress."""
        assert address.stake is not None
        assert address.payment is not None
        stake = _PlutusConstrWrapper(
            _PlutusConstrWrapper(
                PlutusPartAddress(bytes.fromhex(str(address.stake.staking_part)))
            )
        )
        return PlutusFullAddress(
            PlutusPartAddress(bytes.fromhex(str(address.payment.payment_part))),
            stake=stake,
        )


@dataclass
class AssetClass(pycardano.PlutusData):
    """An asset class. Separates out token policy and asset name."""

    policy: bytes
    asset_name: bytes

    @classmethod
    def from_assets(cls, asset: Assets):
        """Parse an Assets object into an AssetClass object."""
        assert len(asset) == 1

        return AssetClass(
            policy=bytes.fromhex(asset.unit()[:56]),
            asset_name=bytes.fromhex(asset.unit()[56:]),
        )


@dataclass
class SwapExactIn(pycardano.PlutusData):
    """Swap exact in order datum."""

    CONSTR_ID = 0
    desired_coin: AssetClass
    minimum_receive: int

    @classmethod
    def from_assets(cls, asset: Assets):
        """Parse an Assets object into a SwapExactIn datum."""
        assert len(asset) == 1

        return SwapExactIn(
            desired_coin=AssetClass.from_assets(asset), minimum_receive=asset.quantity()
        )


@dataclass
class SwapExactOut(pycardano.PlutusData):
    """Swap exact out order datum."""

    CONSTR_ID = 1
    desired_coin: AssetClass
    expected_receive: int

    @classmethod
    def from_assets(cls, asset: Assets):
        """Parse an Assets object into a SwapExactOut datum."""
        assert len(asset) == 1

        return SwapExactOut(
            desired_coin=AssetClass.from_assets(asset),
            expected_receive=asset.quantity(),
        )


@dataclass
class ReceiverDatum(pycardano.PlutusData):
    """The receiver address."""

    CONSTR_ID = 1
    datum_hash: Optional[pycardano.DatumHash]


@dataclass
class OrderDatum(pycardano.PlutusData):
    """An order datum."""

    sender: PlutusFullAddress
    receiver: PlutusFullAddress
    receiver_datum_hash: Optional[pycardano.DatumHash]
    step: Union[SwapExactIn, SwapExactOut]
    batcher_fee: int = BATCHER_FEE
    deposit: int = DEPOSIT


ORDER_METADATA = dict(
    DEPOSIT_ORDER="Deposit Order",
    CANCEL_ORDER="Cancel Order",
    ONE_SIDE_DEPOSIT_ORDER="Zap Order",
    SWAP_EXACT_IN_ORDER="Swap Exact In Order",
    SWAP_EXACT_IN_LIMIT_ORDER="Swap Exact In Limit Order",
    SWAP_EXACT_OUT_ORDER="Swap Exact Out Order",
    WITHDRAW_ORDER="Withdraw Order",
)


class Wallet(BaseModel):
    """A wallet handling class."""

    mnemonic: str = Field(default_factory=pycardano.HDWallet.generate_mnemonic)
    path: Path = Path(
        f".wallet/{os.environ.get('NETWORK','mainnet').lower()}_mnemonic.txt"
    )
    hdwallet: Optional[pycardano.HDWallet]

    context: pycardano.ChainContext = pycardano.BlockFrostChainContext(
        os.environ["PROJECT_ID"],
        base_url=getattr(blockfrost.ApiUrls, os.environ["NETWORK"]).value,
    )

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
        return self.send_tx(
            self.address, Assets(lovelace="5000000"), "create collateral"
        )

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
        tx = self.send_tx(
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

    def send_tx(
        self,
        address: Address,
        amount: Assets,
        msg: Optional[str] = None,
    ):
        """Create a send transaction. Does not actually submit the transaction.

        For now, this function only sends lovelace (ADA) to an address.
        """
        message = self._msg(["send", msg])

        tx_builder = pycardano.TransactionBuilder(self.context, auxiliary_data=message)
        tx_builder.add_input_address(self.address.address)

        # Create UTXOs
        tx_builder.add_output(
            pycardano.TransactionOutput(address.address, amount["lovelace"])
        )

        tx_body = tx_builder.build(change_address=self.address.address)

        tx = pycardano.Transaction(
            tx_body,
            pycardano.TransactionWitnessSet(
                vkey_witnesses=[
                    pycardano.VerificationKeyWitness(
                        pycardano.txbuilder.FAKE_VKEY,
                        pycardano.txbuilder.FAKE_TX_SIGNATURE,
                    )
                ],
            ),
            auxiliary_data=message,
        )
        tx = self._build_and_check(tx)
        tx.transaction_witness_set.vkey_witnesses = []

        return tx

    def swap(
        self,
        pool: Union[minswap.pools.PoolState, str],
        in_assets: Optional[Assets] = None,
        out_assets: Optional[Assets] = None,
        slippage=0.005,
        msg: Optional[str] = None,
    ):
        """Perform a swap on the designated pool.

        _extended_summary_

        Args:
            pool: _description_
            in_assets: _description_. Defaults to None.
            out_assets: _description_. Defaults to None.
            slippage: _description_. Defaults to 0.005.
            msg: _description_. Defaults to None.

        Raises:
            ValueError: _description_

        Returns:
            An unsigned transaction.
        """
        message = self._msg(["Swap: Exact In", msg])

        tx_builder = pycardano.TransactionBuilder(self.context, auxiliary_data=message)
        tx_builder.add_input_address(self.address.address)

        # Get latest pool state if not supplied
        if isinstance(pool, str):
            pool = minswap.pools.get_pool_by_id(pool)  # type: ignore

        assert pool is not None and isinstance(pool, minswap.pools.PoolState)

        # ExactSwapIn if input is supplied
        if in_assets is not None:
            if pool.unit_a in in_assets and pool.unit_b in in_assets:
                raise ValueError("Only one asset can be place in in_assets.")
            out_amount, _ = pool.get_amount_out(in_assets)
            out_amount.__root__[out_amount.unit()] = int(
                out_amount.__root__[out_amount.unit()] * (1 - slippage)
            )
            step = SwapExactIn.from_assets(out_amount)

            address = PlutusFullAddress.from_address(self.address)
            order_datum = OrderDatum(address, address, PlutusNone(), step)
            tx_builder.add_output(
                pycardano.TransactionOutput(
                    address=minswap.addr.STAKE_ORDER.address,
                    amount=in_assets["lovelace"]
                    + order_datum.batcher_fee
                    + order_datum.deposit,
                    datum=order_datum,
                ),
                datum=order_datum,
                add_datum_to_witness=True,
            )

        tx_body = tx_builder.build(change_address=self.address.address)

        tx = pycardano.Transaction(
            tx_body,
            pycardano.TransactionWitnessSet(
                vkey_witnesses=[
                    pycardano.VerificationKeyWitness(
                        pycardano.txbuilder.FAKE_VKEY,
                        pycardano.txbuilder.FAKE_TX_SIGNATURE,
                    )
                ],
                plutus_data=[order_datum],
            ),
            auxiliary_data=message,
        )
        tx = self._build_and_check(tx)
        tx.transaction_witness_set.vkey_witnesses = []

        return tx

    def sign(self, tx: pycardano.Transaction):
        """Sign a transaction."""
        signature = self.payment_signing_key.sign(tx.transaction_body.hash())
        witness = [
            pycardano.VerificationKeyWitness(self.payment_verification_key, signature)
        ]
        tx.transaction_witness_set.vkey_witnesses = witness

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

"""Methods for wallets including building, signing, and submitting transactions."""
import os
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import blockfrost
import pycardano
from pydantic import ConfigDict
from pydantic.v1 import BaseModel, Field, root_validator

import minswap.addr
import minswap.models.blockfrost_models
import minswap.pools
import minswap.utils
from minswap.models import (
    ORDER_SCRIPT,
    Address,
    AddressUtxoContent,
    AddressUtxoContentItem,
    Assets,
    CancelRedeemer,
    OrderDatum,
    PlutusFullAddress,
    PlutusNone,
    SwapExactIn,
    SwapExactOut,
    Transaction,
    TxContentUtxo,
    ZapIn,
    asset_to_value,
)


class OrderStatus(Enum):
    """Status of an order."""

    QUEUED = 0
    SUBMITTED = 1
    COMPLETED = 2


class InvalidOrderTx(Exception):
    """The transaction is not an order."""

    message: str = "The transaction is not an order or could not be found."


class InvalidCompleteTx(Exception):
    """The transaction is not an order."""

    message: str = (
        "The transaction is not the completion order. "
        + " The order does not have correct order datum hash."
    )


class CompletedOrder(Exception):
    """The transaction is not an order."""

    message: str = "The transaction is not an order."


class Order(BaseModel):
    """An order handler."""

    transaction: pycardano.Transaction
    submitted_tx: Optional[TxContentUtxo] = None
    completed_tx: Optional[TxContentUtxo] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def status(self) -> OrderStatus:
        """Status of an order."""
        if self.completed_tx is not None:
            return OrderStatus.COMPLETED

        elif self.submitted_tx is not None:
            return OrderStatus.SUBMITTED

        else:
            return OrderStatus.QUEUED

    def status_update(self, tx: Optional[Union[Transaction, TxContentUtxo]] = None):
        """Update and return the status."""
        if tx is not None:
            if isinstance(tx, Transaction):
                raw_tx = minswap.utils.BlockfrostBackend.api().transaction_utxos(
                    tx.hash, return_type="json"
                )
                tx = TxContentUtxo.model_validate(raw_tx)

            if not isinstance(tx, TxContentUtxo):
                raise InvalidOrderTx

            if tx.hash == self.submitted_tx:
                self.queued_tx = tx
            else:
                if not any(self.data_hash == inp.data_hash for inp in tx.inputs):
                    raise InvalidCompleteTx(
                        "The supplied transaction is not the completion of this order."
                    )

                self.completed_tx = tx

        elif self.submitted_tx is None:
            try:
                raw_tx = minswap.utils.BlockfrostBackend.api().transaction_utxos(
                    str(self.transaction.id), return_type="json"
                )
                self.submitted_tx = TxContentUtxo.model_validate(raw_tx)
            except blockfrost.ApiError as e:
                if e.status_code == 404:
                    raise InvalidOrderTx(
                        f"Could not find order: {str(self.transaction.id)}"
                    )
                else:
                    raise

        return self.status

    def cancel_datum(self):
        """Create the cancel redeemer."""
        return pycardano.Redeemer(CancelRedeemer())


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
    model_config = ConfigDict(arbitrary_types_allowed=True)

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
        utxos = AddressUtxoContent.model_validate(
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

    @property
    def tx_path(self) -> Path:
        """A default path for saving transactions.

        The default path uses the cbor hex encoded address to create a hierarchical
        folder structure. This is useful in applications where many wallets are used
        to prevent too many files/folders collecting in a single director.
        """
        address_cbor = self.address.address.to_cbor_hex()
        path = (
            Path(".tx")
            .joinpath(address_cbor[:2])
            .joinpath(address_cbor[2:4])
            .joinpath(address_cbor[4:10])
        )
        path.mkdir(parents=True, exist_ok=True)

        return path

    def make_collateral_tx(self):
        """Create a collateral creation transaction."""
        return self.send_tx(
            self.address, Assets(lovelace="5000000"), "Create Collateral."
        )

    def consolidate_utxos_tx(self):
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
        message = self._msg(["Consolidate UTxOs"])

        tx_builder = pycardano.TransactionBuilder(self.context, auxiliary_data=message)
        tx_builder.add_input_address(self.address.address)
        collateral = self.collateral
        for utxo in self.utxos:
            assert isinstance(utxo, AddressUtxoContentItem)
            if (
                collateral is not None
                and collateral.tx_hash == utxo.tx_hash
                and collateral.tx_index == utxo.tx_index
            ):
                continue
            tx_builder.add_input(utxo.to_utxo())

        if self.collateral is not None:
            tx_builder.excluded_inputs.append(self.collateral.to_utxo())

        tx = tx_builder.build_and_sign([], change_address=self.address.address)

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

        assert parameters.min_fee_a is not None
        assert parameters.min_fee_b is not None

        return int(parameters.min_fee_a * tx + parameters.min_fee_b)

    def _build_and_check(self, tx: pycardano.Transaction):
        """Build transaction and update transaction fee.

        Precisely calculate the transaction fee. Since the size of the fee could
        influence the fee itself, this recursively calculates the fee until the fee
        settles on the lowest possible fee.
        """
        fee = self._fee(tx.to_cbor())
        tx_body = tx.transaction_body
        while fee != tx.transaction_body.fee:
            tx_body.outputs[-1].amount.coin += tx_body.fee
            tx_body.fee = fee
            tx_body.outputs[-1].amount.coin -= tx_body.fee
            fee = self._fee(tx.to_cbor())

        return tx

    def send_tx(
        self,
        address: Address,
        amount: Assets,
        msg: Optional[str] = None,
    ) -> pycardano.Transaction:
        """Create a send transaction. Does not actually submit the transaction.

        For now, this function only sends lovelace (ADA) to an address.

        TODO:
            Add ability to send to multiple addresses.

        Args:
            address: Address to send funds to.
            amount: The amount of assets to send.
            msg: An optuional metadata message to include in the transaction.
                Defaults to None.

        Returns:
            An unsigned `pycardano.Transaction`.
        """
        message = self._msg(["Send", msg])

        tx_builder = pycardano.TransactionBuilder(self.context, auxiliary_data=message)
        tx_builder.add_input_address(self.address.address)

        # Create UTXOs
        tx_builder.add_output(
            pycardano.TransactionOutput(address.address, amount["lovelace"])
        )

        if self.collateral is not None:
            tx_builder.excluded_inputs.append(self.collateral.to_utxo())

        tx = tx_builder.build_and_sign([], change_address=self.address.address)

        return tx

    def _order(
        self,
        order_datum: OrderDatum,
        in_assets: Assets,
        message: pycardano.AuxiliaryData,
    ):
        tx_builder = pycardano.TransactionBuilder(self.context, auxiliary_data=message)
        tx_builder.add_input_address(self.address.address)

        in_assets.__root__["lovelace"] = (
            in_assets["lovelace"] + order_datum.batcher_fee + order_datum.deposit
        )
        tx_builder.add_output(
            pycardano.TransactionOutput(
                address=minswap.addr.STAKE_ORDER.address,
                amount=asset_to_value(in_assets),
                datum_hash=order_datum.hash(),
            ),
            datum=order_datum,
            add_datum_to_witness=True,
        )
        if self.collateral is not None:
            tx_builder.excluded_inputs.append(self.collateral.to_utxo())

        tx = tx_builder.build_and_sign([], change_address=self.address.address)

        return tx

    def swap(
        self,
        in_assets: Optional[Assets] = None,
        out_assets: Optional[Assets] = None,
        pool: Optional[Union[minswap.pools.PoolState, str]] = None,
        slippage=0.005,
        msg: Optional[str] = None,
    ):
        """Perform a swap on the designated pool.

        This function performs three different tasks based on what inputs are provided:
        1. SwapExactIn - When only `in_assets` is provided
        2. SwapExactOut - When only the `out_assets` is provided.
        3. LimitOrder - When both `in_assets` and `out_assets` are provided.

        Note:
            Remember, each transaction requires a batcher fee and deposit (where the
            deposit is returned when the swap is completed). So, spending 1000 ADA to
            buy MIN will required 1004 ADA to submit the transaction.

        When only input assets are provided (`in_assets`), a SwapExactIn order is
        executed. The SwapExactIn order requests that the smart contract buy as many of
        the token as possible with the provided input assets. The SwapExactIn order also
        contains a minimum expected receive value, and this is calculated by the current
        expected output based on the current price minus the slippage ratio. For
        example, if the price of 1 MIN is 1ADA, assuming an exact swap then 1 ADA will
        buy 1 MIN coin. However, the requested minimum will by default be set to 0.995
        MIN since the default allowed slippage is 0.005 (0.5%). If the price changes so
        that you cannot receive the requested minimum, it will delay executing until the
        order is cancelled or the price moves back into range.

        When only output assets are provided (`out_assets`), a SwapExactOut order is
        executed. The SwapExactOut order requests that the smart contract buy a specific
        amount of token with the provided input assets. The SwapExactOut order also
        contains an expected receive value, and will use the supplied funds to buy the
        specified amount of token (if possible). Thus, slippage in this case is applied
        to the quantity of input token, providing more than the needed amount of token
        to buy the specified token. Using the same example as before where the price of
        1 MIN is 1 ADA, the input amount of ADA will be 1.005 in order to buy 1 MIN when
        the slippage is set to 0.005 (0.5%). Any remaining input tokens after the swap
        are returned as change.

        When both the input and output assets are specified, then a limit order is
        executed. The amount of supplied token is set to `in_assets` and the minimum
        amount received is set to `out_assets`. The price is set by the inherent ratio
        of output to input tokens.

        Args:
            in_assets: The input assets for the swap. Defaults to None.
            out_assets: The output assets for the swap. Defaults to None.
            pool: The pool to submit the transaction to. Only needed when submitting
                swaps other than limit orders. Defaults to None.
            slippage: Ratio used to modify either input or output tokens. Not used when
                both input and output tokens are specified. Defaults to 0.005.
            msg: Optional message to include in the transaction. Defaults to None.

        Returns:
            An unsigned transaction.
        """
        # Basic checks
        for asset in [in_assets, out_assets]:
            if asset is not None:
                assert len(asset) == 1

        # If both are specified, use a limit order
        if in_assets is not None and out_assets is not None:
            message = self._msg(["Swap: Limit Order", msg])
            step = SwapExactIn.from_assets(in_assets)

        # If in_assets defined, swap in. If out_assets defined, swap out.
        elif in_assets is not None or out_assets is not None:
            assert pool is not None
            if isinstance(pool, str):
                pool = minswap.pools.get_pool_by_id(pool)  # type: ignore

            assert isinstance(pool, minswap.pools.PoolState)

            if in_assets is not None:
                message = self._msg(["Swap: Exact In", msg])
                out_assets, _ = pool.get_amount_out(in_assets)
                out_assets.__root__[out_assets.unit()] = int(
                    out_assets.__root__[out_assets.unit()] * (1 - slippage)
                )
                step = SwapExactIn.from_assets(out_assets)
            elif out_assets is not None:
                message = self._msg(["Swap: Exact Out", msg])
                in_assets, _ = pool.get_amount_in(out_assets)
                in_assets.__root__[in_assets.unit()] = int(
                    in_assets.__root__[in_assets.unit()] * (1 + slippage)
                )
                step = SwapExactOut.from_assets(out_assets)
            else:
                raise ValueError(
                    "Something went wrong. Neither in_assets nor out_assets were set."
                )
        else:
            raise ValueError("Either in_assets, out_assets, or both must be defined.")

        address = PlutusFullAddress.from_address(self.address)
        order_datum = OrderDatum(address, address, PlutusNone(), step)

        tx = self._order(
            order_datum=order_datum,
            in_assets=in_assets,
            message=message,
        )

        return tx

    def deposit(
        self,
        assets: Assets,
        pool: Optional[Union[minswap.pools.PoolState, str]] = None,
        slippage=0.005,
        msg: Optional[str] = None,
    ):
        """Perform a swap on the designated pool.

        This function performs two different tasks based on what inputs are provided:
        1. ZapIn - When only one asset is supplied, a zap in deposit is executed
        2. Deposit - When two assets are supplied, a deposit order is created that tries
            to deposit as many coins as possible.

        Note:
            Remember, each transaction requires a batcher fee and deposit (where the
            deposit is returned when the deposit is completed).

        Supplying only one asset will zap in the supplied asset for as much LP as
        possible. Supplying two assets will deposit as much as possible.

        The slippage argument is a modifier for the requested output LP because things
        can change a bit between when the order is submitted and when it is executed.

        Args:
            pool: The pool to submit the transaction to. Only needed when submitting
                swaps other than limit orders. Defaults to None.
            in_assets: The input assets for the swap. Defaults to None.
            out_assets: The output assets for the swap. Defaults to None.
            slippage: Ratio used to modify either input or output tokens. Not used when
                both input and output tokens are specified. Defaults to 0.005.
            msg: Optional message to include in the transaction. Defaults to None.

        Returns:
            An unsigned transaction.
        """
        # If only one asset, perform a zap
        if pool is None:
            if len(assets) == 1:
                # a pool must be supplied
                raise ValueError(
                    "When only one asset is supplied, a pool must be specified."
                )
            pools = minswap.pools.get_pools()
            asset_units = list(assets)
            for p in pools:
                assert isinstance(p, minswap.pools.PoolState)
                if p.asset_a in asset_units and p.asset_b in asset_units:
                    pool = p
                    break

        elif isinstance(pool, str):
            pool = minswap.pools.get_pool_by_id(pool)

        # When there's only one asset, perform a zap
        if len(assets) == 1:
            assert pool is not None
            asset_out, _ = pool.get_zap_in_lp(assets)
            asset_out.__root__[pool.lp_token] = int(
                asset_out[pool.lp_token] * (1 - slippage)
            )
            step = ZapIn.from_assets(asset_out)
            message = self._msg(["Deposit: Zap in", msg])
        else:
            raise NotImplementedError("Deposit both tokens is not available.")

        address = PlutusFullAddress.from_address(self.address)
        order_datum = OrderDatum(address, address, PlutusNone(), step)

        tx = self._order(
            order_datum=order_datum,
            in_assets=assets,
            message=message,
        )

        return tx

    def cancel(
        self, order: Order, msg: Optional[str] = None
    ) -> Optional[pycardano.Transaction]:
        """Cancel an order."""
        assert order.status_update() == OrderStatus.SUBMITTED

        message = self._msg(["Cancel order", msg])

        tx_builder = pycardano.TransactionBuilder(
            self.context,
            auxiliary_data=message,
            required_signers=[self.payment_verification_key.hash()],
        )
        for output in order.transaction.transaction_body.outputs:
            if output.address.encode() == minswap.addr.STAKE_ORDER.address.encode():
                if output.datum_hash is None:
                    output.datum_hash = pycardano.datum_hash(output.datum)
                t_in = pycardano.TransactionInput(order.transaction.id, 0)
                amount = pycardano.Value(output.amount.coin, output.amount.multi_asset)
                t_out = pycardano.TransactionOutput(
                    output.address, amount, datum_hash=output.datum_hash
                )
                utxo = pycardano.UTxO(input=t_in, output=t_out)
                tx_builder.add_script_input(
                    utxo=utxo,
                    script=ORDER_SCRIPT,
                    datum=order.transaction.transaction_witness_set.plutus_data[0],
                    redeemer=order.cancel_datum(),
                )

        if self.collateral is None:
            raise ValueError("No UTxOs available for collateral.")
        tx_builder.excluded_inputs.append(self.collateral.to_utxo())
        tx_builder.collaterals.append(self.collateral.to_utxo())

        tx = tx_builder.build_and_sign(
            signing_keys=[],
            change_address=self.address.address,
        )

        return tx

    def sign(self, tx: pycardano.Transaction):
        """Sign a transaction."""
        signature = self.payment_signing_key.sign(tx.transaction_body.hash())
        witness = [
            pycardano.VerificationKeyWitness(self.payment_verification_key, signature)
        ]
        tx.transaction_witness_set.vkey_witnesses = witness

        return tx

    def submit(self, tx: pycardano.Transaction, path: Optional[Path] = None) -> Order:
        """Submit a transaction."""
        if path is None:
            path = self.tx_path
        path = path.joinpath(str(tx.id) + ".cbor")
        with open(path, "wb") as fw:
            fw.write(tx.to_cbor())

        try:
            minswap.utils.BlockfrostBackend.api().transaction_submit(
                str(path), return_type="json"
            )
        except Exception:
            path.unlink()
            raise

        order = Order(transaction=tx)

        return order

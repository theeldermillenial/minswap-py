"""Standard models and model conversion functions.

This module contains common models used throughout minswap-py as well as utility
functions for converting data types.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

import pycardano
from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
    model_validator,
)

from minswap.models import blockfrost_models

load_dotenv()


class PoolHistory(BaseModel):
    """A historical point in the pool."""

    tx_hash: str
    tx_index: int
    block_height: int
    time: datetime


class PoolTransactionReference(BaseModel):
    """A reference to a pool transaction state."""

    tx_index: int
    tx_hash: str
    block_height: int
    block_time: datetime

    @model_validator(mode="before")
    @classmethod
    def _validator(cls, values):
        values["block_time"] = datetime.utcfromtimestamp(values["block_time"])

        return values

    model_config = ConfigDict(frozen=True, extra="forbid")


class Transaction(blockfrost_models.TxContent):
    """Transaction Content."""

    block_time: datetime = blockfrost_models.TxContent.model_fields[
        "block_time"
    ]  # type: ignore

    @model_validator(mode="before")
    @classmethod
    def _validator(cls, values):
        values["block_time"] = datetime.utcfromtimestamp(values["block_time"])

        return values


class AssetHistoryReference(BaseModel):
    """A reference to a pool transaction state."""

    tx_hash: str
    action: str
    amount: int
    model_config = ConfigDict(frozen=True, extra="forbid")


class BaseList(RootModel):
    """Utility class for list models."""

    def __iter__(self):  # noqa
        return iter(self.root)

    def __getitem__(self, item):  # noqa
        return self.root[item]

    def __len__(self):  # noqa
        return len(self.root)


class BaseDict(BaseList):
    """Utility class for dict models."""

    def items(self):
        """Return iterable of key-value pairs."""
        return self.root.items()

    def keys(self):
        """Return iterable of keys."""
        return self.root.keys()

    def values(self):
        """Return iterable of values."""
        return self.root.values()

    def __getitem__(self, item):  # noqa
        return self.root.get(item, 0)


class TxIn(BaseModel):
    """A quantity of a blockchain asset."""

    tx_hash: str
    tx_index: int


def _unit_alias(unit: str) -> str:
    """Rename unit alias.

    If a unit alias is input, it changes it to "unit". Otherwise, passes the value
    through.

    Arg:
        unit: The unit alias, or passthrough.
    """
    if unit in ["asset"]:
        return "unit"
    else:
        return unit


class Assets(BaseDict):
    """Contains all tokens and quantities."""

    root: Dict[str, int]

    def unit(self, index: int = 0) -> str:
        """Units of asset at `index`."""
        return list(self.keys())[index]

    def quantity(self, index: int = 0) -> int:
        """Quantity of the asset at `index`."""
        return list(self.values())[index]

    @model_validator(mode="before")
    @classmethod
    def _digest_assets(cls, values):
        if "root" in values:
            root = values["root"]
        elif "values" in values and isinstance(values["values"], list):
            root = {v.unit: v.quantity for v in values["values"]}
        else:
            root = {k: int(v) for k, v in values.items()}
        root = dict(
            sorted(root.items(), key=lambda x: "" if x[0] == "lovelace" else x[0])
        )

        return root

    def __add__(a, b):
        """Add two assets."""
        intersection = set(a.keys()) | set(b.keys())

        result = {key: a[key] + b[key] for key in intersection}

        return Assets(**result)

    def __sub__(a, b):
        """Subtract two assets."""
        intersection = set(a.keys()) | set(b.keys())

        result = {key: a[key] - b[key] for key in intersection}

        return Assets(**result)


class OnchainMetadata(blockfrost_models.AssetOnchainMetadataCip25):
    """Data class to hold on chain metadata for an asset."""


class Metadata(blockfrost_models.Metadata1):
    """Asset metadata.

    TODO: Remove this in the future.
    """


class OnchainMetadataStandard(Enum):
    """On chain metadata standard version.

    This class only exists because Genius Yield follows CIP68, and the blockfrost API
    doesn't handle it well.

    https://cips.cardano.org/cips/cip25/
    https://cips.cardano.org/cips/cip68/
    """

    CIP25v1 = "CIP25v1"
    CIP25v2 = "CIP25v2"
    CIP68v1 = "CIP68v1"


class AssetIdentity(blockfrost_models.Asset1):
    """A blockchain asset."""

    @model_validator(mode="before")
    @classmethod
    def _validate_asset_name(cls, values):
        """Handle missing asset metadata.

        Liqwid's qADA did not include asset_name in their metadata. This attempts to
        find the asset name in another piece of metadata.

        qADA policy: a04ce7a52545e5e33c2867e148898d9e667a69602285f6a1298f9d68
        """
        if values["asset_name"] is None:
            if values["metadata"] is not None:
                values["asset_name"] = values["metadata"]["name"]

        return values

    model_config = ConfigDict(use_enum_values=True)

    @property
    def decimals(self) -> int:
        """Decimal precision of the asset."""
        if self.metadata is not None and self.metadata.decimals is not None:
            return self.metadata.decimals
        else:
            return 0


class AddressUtxoContentItem(blockfrost_models.AddressUtxoContentItem):
    """An address UTxO item."""

    amount: Assets

    @field_validator("amount", mode="before")
    @classmethod
    def _to_assets(cls, value):
        if isinstance(value, list):
            return Assets(**{i["unit"]: i["quantity"] for i in value})
        else:
            return value

    def to_utxo(self) -> pycardano.UTxO:
        """Convert to a pycardano UTxO object."""
        inp = pycardano.TransactionInput.from_primitive([self.tx_hash, self.tx_index])
        address = pycardano.Address.decode(self.address)
        amount = asset_to_value(self.amount)
        out = pycardano.TransactionOutput(
            address=address, amount=amount, datum_hash=self.data_hash
        )

        return pycardano.UTxO(inp, out)


class AddressUtxoContent(blockfrost_models.AddressUtxoContent, BaseList):
    """An address UTxO list of items."""

    root: List[
        AddressUtxoContentItem
    ] = blockfrost_models.AddressUtxoContent.model_fields[
        "root"
    ]  # type:ignore


class Input(blockfrost_models.Input):
    """An input to a transaction."""

    amount: Assets = Field(
        None,
        examples=[
            [
                {"unit": "lovelace", "quantity": "42000000"},
                {
                    "unit": "b0d07d45fe9514f80213f4020e5a61241458be626841cde717cb38a76e7574636f696e",  # noqa
                    "quantity": "12",
                },
            ]
        ],
    )

    @field_validator("amount", mode="before")
    @classmethod
    def _to_assets(cls, value):
        if isinstance(value, list):
            return Assets(**{i["unit"]: i["quantity"] for i in value})
        else:
            return value


class Output(blockfrost_models.Output):
    """An output to a transaction."""

    amount: Assets = Field(
        None,
        examples=[
            [
                {"unit": "lovelace", "quantity": "42000000"},
                {
                    "unit": "b0d07d45fe9514f80213f4020e5a61241458be626841cde717cb38a76e7574636f696e",  # noqa
                    "quantity": "12",
                },
            ]
        ],
    )

    @field_validator("amount", mode="before")
    @classmethod
    def _to_assets(cls, value):
        if isinstance(value, list):
            return Assets(**{i["unit"]: i["quantity"] for i in value})
        else:
            return value


class TxContentUtxo(blockfrost_models.TxContentUtxo):
    """A Transaction, containing all inputs and outputs."""

    inputs: List[Input]
    outputs: List[Output]


class Address(BaseModel):
    """A Cardano address.

    This class holds Cardano address information, including payment, stake, and script
    addresses. The input should be the `bech32` encoded address.
    """

    bech32: str
    address: pycardano.Address
    payment: Optional[pycardano.Address]
    stake: Optional[pycardano.Address]
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="before")
    @classmethod
    def translate_address(cls, values):  # noqa: D102
        assert "bech32" in values

        values["address"] = pycardano.Address.decode(values["bech32"])

        if values["address"].staking_part is not None:
            values["stake"] = pycardano.Address(
                staking_part=values["address"].staking_part
            )
        else:
            values["stake"] = None

        if values["address"].payment_part is not None:
            values["payment"] = pycardano.Address(
                payment_part=values["address"].payment_part
            )
        else:
            values["payment"] = None

        return values


ORDER_SCRIPT: pycardano.PlutusV1Script = pycardano.PlutusV1Script(
    bytes.fromhex(
        "59014c01000032323232323232322223232325333009300e30070021323233533300b33"
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

    CONSTR_ID = 0

    policy: bytes
    asset_name: bytes

    @classmethod
    def from_assets(cls, asset: Assets):
        """Parse an Assets object into an AssetClass object."""
        assert len(asset) == 1

        if asset.unit() == "lovelace":
            return AssetClass(
                policy=b"",
                asset_name=b"",
            )
        else:
            return AssetClass(
                policy=bytes.fromhex(asset.unit()[:56]),
                asset_name=bytes.fromhex(asset.unit()[56:]),
            )


def asset_to_value(assets: Assets) -> pycardano.Value:
    """Convert an Assets object to a pycardano.Value."""
    coin = assets["lovelace"]
    cnts = {}
    for unit, quantity in assets.items():
        if unit == "lovelace":
            continue
        policy = bytes.fromhex(unit[:56])
        asset_name = bytes.fromhex(unit[56:])
        if policy not in cnts:
            cnts[policy] = {asset_name: quantity}
        else:
            cnts[policy][asset_name] = quantity

    if len(cnts) == 0:
        return pycardano.Value.from_primitive([coin])
    else:
        return pycardano.Value.from_primitive([coin, cnts])


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
class Deposit(pycardano.PlutusData):
    """Swap exact out order datum."""

    CONSTR_ID = 2
    minimum_lp: int


@dataclass
class Withdraw(pycardano.PlutusData):
    """Swap exact out order datum."""

    CONSTR_ID = 3
    minimum_asset_a: int
    minimum_asset_b: int


@dataclass
class ZapIn(pycardano.PlutusData):
    """Swap exact out order datum."""

    CONSTR_ID = 4
    desired_coin: AssetClass
    minimum_receive: int

    @classmethod
    def from_assets(cls, asset: Assets):
        """Parse an Assets object into a SwapExactOut datum."""
        assert len(asset) == 1

        return ZapIn(
            desired_coin=AssetClass.from_assets(asset),
            minimum_receive=asset.quantity(),
        )


@dataclass
class ReceiverDatum(pycardano.PlutusData):
    """The receiver address."""

    CONSTR_ID = 1
    datum_hash: Optional[pycardano.DatumHash]


@dataclass
class OrderDatum(pycardano.PlutusData):
    """An order datum."""

    CONSTR_ID = 0

    sender: PlutusFullAddress
    receiver: PlutusFullAddress
    receiver_datum_hash: Union[pycardano.DatumHash, PlutusNone]
    step: Union[SwapExactIn, SwapExactOut]
    batcher_fee: int = BATCHER_FEE
    deposit: int = DEPOSIT


@dataclass
class FeeDatumHash(pycardano.PlutusData):
    """Fee datum hash."""

    CONSTR_ID = 0
    fee_hash: str


@dataclass
class FeeSwitchOn(pycardano.PlutusData):
    """Pool Fee Sharing On."""

    CONSTR_ID = 0
    fee_to: PlutusFullAddress
    fee_to_datum_hash: Union[PlutusNone, FeeDatumHash]


@dataclass
class _EmptyFeeSwitchWrapper(pycardano.PlutusData):
    CONSTR_ID = 0
    fee_sharing: Union[FeeSwitchOn, PlutusNone]


@dataclass
class PoolDatum(pycardano.PlutusData):
    """Pool Datum."""

    CONSTR_ID = 0

    asset_a: AssetClass
    asset_b: AssetClass
    total_liquidity: int
    root_k_last: int
    fee_sharing: _EmptyFeeSwitchWrapper


@dataclass
class CancelRedeemer(pycardano.PlutusData):
    """Cancel datum."""

    CONSTR_ID = 1

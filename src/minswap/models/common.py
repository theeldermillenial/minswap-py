"""Standard models and model conversion functions.

This module contains common models used throughout minswap-py as well as utility
functions for converting data types.
"""

from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pycardano
from blockfrost import Namespace
from pydantic import BaseModel, root_validator

from minswap.models import blockfrost_models


def to_dict(
    values: Union[Namespace, List[Any], Dict[str, Any]]
) -> Union[List[Any], Dict[str, Any]]:
    """Traverse a dictionary and convert blockfrost.Namespace objects to dictionaries.

    When using `blockfrost.Namespace.to_dict` function to convert a `Namespace` object
    to a `dict`, child values that are `Namespace` objects are not converted to a
    `dict`. This function traverse a `Namespace`, `List`, or `Dict` and converts any
    child values from `Namespace` to `dict`. This is necessary for casting to
    `pydantic` models for proper error checking and validation.

    Traversel of the input is recursive, ensuring all children are converted.

    Args:
        values: A `Namespace`, `List`, or `Dict` to be recursively traversed.

    Returns:
        Union[List[Any], Dict[Any, Any], Dict[str, Any]]
    """
    iterator: Optional[Iterable] = None

    if isinstance(values, (Namespace)):
        values = values.to_dict()

    if isinstance(values, list):
        iterator = enumerate(values)
    elif isinstance(values, dict):
        iterator = values.items()

    if iterator is not None:
        for k, v in iterator:
            if isinstance(v, Namespace):
                values[k] = to_dict(v.to_dict())
            elif isinstance(v, (list, dict)):
                values[k] = to_dict(v)

        assert isinstance(values, (list, dict)), f"Error: {values}"

    return values


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

    @root_validator(pre=True)
    def _validator(cls, values):
        values["block_time"] = datetime.utcfromtimestamp(values["block_time"])

        return values

    class Config:  # noqa: D106
        allow_mutation = False
        extra = "forbid"


class Transaction(blockfrost_models.TxContent):
    """Transaction Content."""

    block_time: datetime = blockfrost_models.TxContent.__fields__[
        "block_time"
    ]  # type: ignore

    @root_validator(pre=True)
    def _validator(cls, values):
        values["block_time"] = datetime.utcfromtimestamp(values["block_time"])

        return values


class AssetHistoryReference(BaseModel):
    """A reference to a pool transaction state."""

    tx_hash: str
    action: str
    amount: int

    class Config:  # noqa: D106
        allow_mutation = False
        extra = "forbid"


class BaseList(BaseModel):
    """Utility class for list models."""

    def __iter__(self):  # noqa
        return iter(self.__root__)

    def __getitem__(self, item):  # noqa
        return self.__root__[item]

    def __len__(self):  # noqa
        return len(self.__root__)


class BaseDict(BaseList):
    """Utility class for dict models."""

    def items(self):
        """Return iterable of key-value pairs."""
        return self.__root__.items()

    def keys(self):
        """Return iterable of keys."""
        return self.__root__.keys()

    def values(self):
        """Return iterable of values."""
        return self.__root__.values()

    def __getitem__(self, item):  # noqa
        return self.__root__.get(item, 0)


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

    __root__: Dict[str, int]

    def unit(self, index: int = 0):
        """Units of asset at `index`."""
        return list(self.keys())[index]

    def quantity(self, index: int = 0):
        """Quantity of the asset at `index`."""
        return list(self.values())[index]

    @root_validator(pre=True)
    def _digest_assets(cls, values):
        if "__root__" in values:
            root = values["__root__"]
        elif "values" in values and isinstance(values["values"], list):
            root = {v.unit: v.quantity for v in values["values"]}
        else:
            root = {k: v for k, v in values.items()}
        root = dict(
            sorted(root.items(), key=lambda x: "" if x[0] == "lovelace" else x[0])
        )

        return {"__root__": root}

    def __add__(a, b):
        """Add two assets."""
        intersection = set(a.keys()) | set(b.keys())

        result = {key: a[key] + b[key] for key in intersection}

        return Assets(**result)

    def __sub__(a, b):
        """Add two assets."""
        intersection = set(a.keys()) | set(b.keys())

        result = {key: a[key] - b[key] for key in intersection}

        return Assets(**result)


class Address(BaseModel):
    """A Cardano address.

    This class holds Cardano address information, including payment, stake, and script
    addresses. The input should be the `bech32` encoded address.
    """

    bech32: str
    address: pycardano.Address
    payment: Optional[pycardano.Address]
    stake: Optional[pycardano.Address]

    class Config:  # noqa: D106
        arbitrary_types_allowed = True

    @root_validator(pre=True)
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

    @root_validator(pre=True)
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

    class Config:  # noqa: D106
        use_enum_values = True

    @property
    def decimals(self) -> int:
        """Decimal precision of the asset."""
        if self.metadata is not None and self.metadata.decimals is not None:
            return self.metadata.decimals
        else:
            return 0


class AddressUtxoContentItem(blockfrost_models.AddressUtxoContentItem):
    """An address UTxO item."""


class AddressUtxoContent(blockfrost_models.AddressUtxoContent, BaseList):
    """An address UTxO list of items."""

    __root__: List[
        AddressUtxoContentItem
    ] = blockfrost_models.AddressUtxoContent.__fields__[
        "__root__"
    ]  # type:ignore


class Input(blockfrost_models.Input):
    """An input to a transaction."""


class Output(blockfrost_models.Output):
    """An output to a transaction."""


class TxContentUtxo(blockfrost_models.TxContentUtxo):
    """A Transaction, containing all inputs and outputs."""

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
from pydantic import BaseModel, Field, root_validator, validator

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
        try:
            super().__getitem__(item)
        except KeyError:
            self.__root__.items()[item]


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

        root: Dict[str, int] = {}
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

    _files_val = validator("files", pre=True, allow_reuse=True)(to_dict)
    image: Union[str, List[str], None] = Field(
        None, example="ipfs://ipfs/QmfKyJ4tuvHowwKQCbCHj4L5T3fSj8cjs7Aau8V7BWv226"
    )  # type:ignore
    name: Optional[str] = Field(None, example="My NFT token")  # type:ignore


class Metadata(blockfrost_models.Metadata1):
    """Asset metadata.

    The blockfrost autogenerated model does not conform to the metadata asset definition
    for the cardano token registry, so this modifies it to be in compliance.

    https://developers.cardano.org/docs/native-tokens/cardano-token-registry/
    """

    decimals: Optional[int] = blockfrost_models.Metadata1.__fields__[
        "decimals"
    ]  # type: ignore
    logo: Optional[str] = blockfrost_models.Metadata1.__fields__["logo"]  # type: ignore
    ticker: Optional[str] = blockfrost_models.Metadata1.__fields__[
        "ticker"
    ]  # type: ignore
    url: Optional[str] = blockfrost_models.Metadata1.__fields__["url"]  # type: ignore


class OnchainMetadataStandard(Enum):
    """On chain metadata standard version.

    This class only exists because Genius Yield follow CIP68, and the blockfrost API
    doesn't handle it well.

    https://cips.cardano.org/cips/cip25/
    https://cips.cardano.org/cips/cip68/
    """

    CIP25v1 = "CIP25v1"
    CIP25v2 = "CIP25v2"
    CIP68v1 = "CIP68v1"


class AssetIdentity(blockfrost_models.Asset1):
    """A blockchain asset."""

    onchain_metadata: Optional[OnchainMetadata] = blockfrost_models.Asset1.__fields__[
        "onchain_metadata"
    ]  # type: ignore

    onchain_metadata_standard: Optional[OnchainMetadataStandard]  # type: ignore

    metadata: Optional[Metadata] = blockfrost_models.Asset1.__fields__[
        "metadata"
    ]  # type: ignore

    _onchain_metadata_val = validator("onchain_metadata", pre=True, allow_reuse=True)(
        to_dict
    )

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

    reference_script_hash: Optional[
        str
    ] = blockfrost_models.AddressUtxoContentItem.__fields__[
        "reference_script_hash"
    ]  # type: ignore

    inline_datum: Optional[str] = blockfrost_models.AddressUtxoContentItem.__fields__[
        "inline_datum"
    ]  # type: ignore

    data_hash: Optional[str] = blockfrost_models.AddressUtxoContentItem.__fields__[
        "inline_datum"
    ]  # type: ignore


class AddressUtxoContent(blockfrost_models.AddressUtxoContent, BaseList):
    """An address UTxO list of items."""

    __root__: List[
        AddressUtxoContentItem
    ] = blockfrost_models.AddressUtxoContent.__fields__[
        "__root__"
    ]  # type:ignore


class Input(blockfrost_models.Input):
    """An input to a transaction."""

    inline_datum: Optional[str] = Field(None, example="19a6aa")  # type: ignore
    """
    CBOR encoded inline datum
    """
    reference_script_hash: Optional[str] = Field(
        None, example="13a3efd825703a352a8f71f4e2758d08c28c564e8dfcce9f77776ad1"
    )  # type: ignore
    """
    The hash of the reference script of the input
    """
    data_hash: Optional[str] = Field(
        None, example="9e478573ab81ea7a8e31891ce0648b81229f408d596a3483e6f4f9b92d3cf710"
    )  # type: ignore
    """
    The hash of the transaction output datum
    """


class Output(blockfrost_models.Output):
    """An output to a transaction."""

    inline_datum: Optional[str] = Field(None, example="19a6aa")  # type: ignore
    """
    CBOR encoded inline datum
    """
    reference_script_hash: Optional[str] = Field(
        None, example="13a3efd825703a352a8f71f4e2758d08c28c564e8dfcce9f77776ad1"
    )  # type: ignore
    """
    The hash of the reference script of the input
    """
    data_hash: Optional[str] = Field(
        None, example="9e478573ab81ea7a8e31891ce0648b81229f408d596a3483e6f4f9b92d3cf710"
    )  # type: ignore
    """
    The hash of the transaction output datum
    """


class TxContentUtxo(blockfrost_models.TxContentUtxo):
    """A Transaction, containing all inputs and outputs."""

    inputs: List[Input]  # type: ignore
    outputs: List[Output]  # type: ignore

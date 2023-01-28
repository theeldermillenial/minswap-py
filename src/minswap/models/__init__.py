"""Standard models and model conversion functions.

This module contains common models used throughout minswap-py as well as utility
functions for converting data types.

- `to_dict(values)` - Traverse a dictionary and converts Namespace objects to dicts.
- `Address(bech32)` - Takes a bech32 address and generates associated addresses.
- `Asset` - A data class for Minswap asset.
- `OnchainMetadata` - A data class for on chain metadata associated with an asset.
"""

from collections.abc import Iterable
from typing import Any, Dict, List, Optional, Union

import pycardano
from blockfrost import Namespace
from pydantic import BaseModel, root_validator, validator

from . import blockfrost


def to_dict(
    values: Union[Namespace, List[Any], Dict[str, Any]]
) -> Union[List[Any], Dict[Any, Any], Dict[str, Any]]:
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
    iterator: Iterable

    if isinstance(values, (Namespace)):
        values = values.to_dict()

    if isinstance(values, list):
        iterator = enumerate(values)
    elif isinstance(values, dict):
        iterator = values.items()

    for k, v in iterator:
        if isinstance(v, Namespace):
            values[k] = to_dict(v.to_dict())
        elif isinstance(v, (list, dict)):
            values[k] = to_dict(v)

    assert isinstance(values, (list, dict))

    return values


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


class OnchainMetadata(blockfrost.AssetOnchainMetadataCip25):
    """Data class to hold on chain metadata for an asset."""

    _files_val = validator("files", pre=True, allow_reuse=True)(to_dict)


class Asset(blockfrost.Asset1):
    """A blockchain asset."""

    onchain_metadata: OnchainMetadata = blockfrost.Asset1.__fields__[
        "onchain_metadata"
    ]  # type: ignore

    metadata: Optional[blockfrost.Metadata1] = blockfrost.Asset1.__fields__[
        "metadata"
    ]  # type: ignore

    _onchain_metadata_val = validator("onchain_metadata", pre=True, allow_reuse=True)(
        to_dict
    )

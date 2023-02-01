"""Functions for processing minswap pools.

- `get_pools()` - Get a list of all pools.
"""
import logging
from datetime import datetime
from typing import List, Tuple, Union

import blockfrost
from dotenv import dotenv_values
from pydantic import BaseModel, root_validator

from minswap import addr
from minswap.models import (  # type: ignore[attr-defined]
    AddressUtxoContent,
    AddressUtxoContentItem,
    AssetIdentity,
    Quantity,
    TxIn,
    Value,
)

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("minswap.pools")


def normalize_assets(a: Quantity, b: Quantity) -> Tuple[Quantity, Quantity]:
    """Sort assets by ADA first, then by lexicographical order.

    Args:
        a: The first token.
        b: The second token.

    Raises:
        ValueError: If neither token is ADA or cannot be sorted lexicographically.

    Returns:
        Tuple[Value, Value]: _description_
    """
    if a.unit == "lovelace":
        return (a, b)
    elif b.unit == "lovelace":
        return (b, a)
    elif a.unit > b.unit:
        return (a, b)
    elif a.unit < b.unit:
        return (b, a)
    else:
        raise ValueError("Incompatible values.")


def check_valid_pool_output(utxo: AddressUtxoContentItem):
    """Determine if the pool address is valid.

    Args:
        utxo: A list of UTxOs.

    Raises:
        ValueError: Invalid `address`.
        ValueError: No factory token found in utxos.
    """
    # Check to make sure the pool address is correct
    correct_address: bool = utxo.address == addr.POOL.address.encode()
    if not correct_address:
        message = f"Invalid pool address. Expected {addr.POOL}"
        logger.debug(message)
        raise ValueError(message)

    # Check to make sure the pool has 1 factory token
    for asset in utxo.amount:
        has_factory: bool = (
            f"{addr.FACTORY_POLICY_ID}{addr.FACTORY_ASSET_NAME}" == asset.unit
        )
        if has_factory:
            break
    if not has_factory:
        message = "Pool must have 1 factory token."
        logger.debug(message)
        logger.debug(f"asset.unit={asset.unit}")
        logger.debug(f"factory={addr.FACTORY_POLICY_ID}{addr.FACTORY_ASSET_NAME}")
        raise ValueError(message)


def is_valid_pool_output(utxo: AddressUtxoContentItem):
    """Determine if a utxo contains a pool identifier."""
    try:
        check_valid_pool_output(utxo)
        return True
    except ValueError:
        return False


class PoolState(BaseModel):
    """A particular pool state, either current of historical."""

    tx_in: TxIn
    value: Value
    datum_hash: str
    asset_a: Quantity
    asset_b: Quantity

    class Config:  # noqa: D106
        allow_mutation = False

    @root_validator(pre=True)
    def translate_address(cls, values):  # noqa: D102

        # Find the NFT token for the pool
        value: Value = values["value"]
        nfts = [
            asset for asset in value if asset.unit.startswith(addr.POOL_NFT_POLICY_ID)
        ]
        if len(nfts) != 1:
            raise ValueError("A pool must have one pool NFT token.")
        nft = nfts[0]
        pool_id = nft.unit[56:]

        relevant_assets = [
            asset
            for asset in value
            if not asset.unit.startswith(addr.FACTORY_POLICY_ID)
            and not asset.unit.endswith(pool_id)
        ]

        non_ada_assets = [a for a in relevant_assets if a.unit != "lovelace"]

        if len(relevant_assets) == 2:
            # ADA pair
            assert len(non_ada_assets) == 1, "Pool must only have 1 non-ADA asset."
            values["asset_a"] = [a for a in value if a.unit == "lovelace"][0]
            values["asset_b"] = non_ada_assets[0]
        elif len(relevant_assets) == 3:
            # Non-ADA pair
            assert len(non_ada_assets) == 2, "Pool must only have 2 non-ADA assets."
            values["asset_a"], values["asset_b"] = normalize_assets(*non_ada_assets)
        else:
            raise ValueError(
                "Pool must have 2 or 3 assets except factor, NFT, and LP tokens."
            )

        return values

    @property
    def nft(self) -> Quantity:
        """Get the pool nft asset."""
        nfts = [a for a in self.value if a.unit.startswith(addr.POOL_NFT_POLICY_ID)]
        if len(nfts) != 1:
            raise ValueError("A pool must have one pool NFT token.")
        return nfts[0]

    @property
    def id(self) -> str:
        """Pool id."""
        return self.nft.unit[len(addr.POOL_NFT_POLICY_ID) :]

    @property
    def pool_lp(self) -> str:
        """Pool liquidity provider token."""
        return f"{addr.LP_POLICY_ID}{self.id}"

    @property
    def unit_a(self) -> str:
        """Token name of asset A."""
        return self.asset_a.unit

    @property
    def unit_b(self) -> str:
        """Token name of asset b."""
        return self.asset_b.unit

    @property
    def reserve_a(self) -> int:
        """Reserve amount of asset A."""
        return self.asset_a.quantity

    @property
    def reserve_b(self) -> int:
        """Reserve amount of asset B."""
        return self.asset_b.quantity

    def _get_asset_name(self, value: str) -> str:
        logger.debug(f"_get_asset_info: {value}")
        if value == "lovelace":
            return "lovelace"
        env = dotenv_values()
        api = blockfrost.BlockFrostApi(env["PROJECT_ID"])
        info = api.asset(value, return_type="json")
        return bytes.fromhex(AssetIdentity.parse_obj(info).asset_name).decode()

    @property
    def asset_a_name(self) -> str:
        """Information about asset A."""
        return self._get_asset_name(self.unit_a)

    @property
    def asset_b_name(self) -> str:
        """Information about asset B."""
        return self._get_asset_name(self.unit_b)

    def get_amount_out(self, asset: Quantity) -> Quantity:
        """Get the output asset amount given an input asset amount.

        Args:
            asset: An asset with a defined quantity.

        Returns:
            The estimated asset returned from the swap.
        """
        assert asset.unit in [
            self.unit_a,
            self.unit_b,
        ], f"Asset {asset.unit} is invalid for pool {self.unit_a}-{self.unit_b}"
        if asset.unit == self.unit_a:
            reserve_in, reserve_out = self.reserve_a, self.reserve_b
            unit_out = self.unit_b
        else:
            reserve_in, reserve_out = self.reserve_b, self.reserve_a
            unit_out = self.unit_a

        numerator: int = asset.quantity * 997 * reserve_out
        denominator: int = asset.quantity * 997 + reserve_in * 1000

        return Quantity(unit=unit_out, quantity=numerator // denominator)

    def get_amount_in(self, asset: Quantity) -> Quantity:
        """Get the input asset amount given a desired output asset amount.

        Args:
            asset: An asset with a defined quantity.

        Returns:
            The estimated asset needed for input in the swap.
        """
        assert asset.unit in [
            self.unit_a,
            self.unit_b,
        ], f"Asset {asset.unit} is invalid for pool {self.unit_a}-{self.unit_b}"
        if asset.unit == self.unit_b:
            reserve_in, reserve_out = self.reserve_a, self.reserve_b
            unit_out = self.unit_a
        else:
            reserve_in, reserve_out = self.reserve_b, self.reserve_a
            unit_out = self.unit_b

        numerator: int = asset.quantity * 1000 * reserve_in
        denominator: int = (reserve_out - asset.quantity) * 997

        return Quantity(unit=unit_out, quantity=numerator // denominator)


class PoolHistory(BaseModel):
    """A historical point in the pool."""

    tx_hash: str
    tx_index: int
    block_height: int
    time: datetime


def get_pools(
    return_non_pools: bool = False,
) -> Union[List[PoolState], Tuple[List[PoolState], List[AddressUtxoContentItem]]]:
    """Get a list of all pools.

    Args:
        return_non_pools: If True, returns UTxOs not belonging to pools as a second
            output. Default is False.

    Returns:
        _type_: _description_
    """
    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    utxos_raw = api.address_utxos(
        addr.POOL.address.encode(), gather_pages=True, order="asc", return_type="json"
    )

    utxos = AddressUtxoContent.parse_obj(utxos_raw)

    pools: List[PoolState] = []
    non_pools: List[AddressUtxoContentItem] = []

    for utxo in utxos:
        if is_valid_pool_output(utxo):
            pools.append(
                PoolState(
                    tx_in=TxIn(transaction_id=utxo.tx_hash, index=utxo.output_index),
                    value=Value(__root__=[a.dict() for a in utxo.amount]),
                    datum_hash=utxo.data_hash,
                )
            )
        else:
            non_pools.append(utxo)

    if return_non_pools:
        return pools, non_pools
    else:
        return pools

"""Functions for processing minswap pools."""
import logging
from decimal import Decimal
from typing import List, Optional, Tuple, Union

import blockfrost
from dotenv import dotenv_values
from pydantic import BaseModel, root_validator

from minswap import addr
from minswap.assets import naturalize_assets
from minswap.models import AddressUtxoContent  # type: ignore[attr-defined]
from minswap.models import (
    AddressUtxoContentItem,
    AssetIdentity,
    Assets,
    AssetTransaction,
    Output,
    TxContentUtxo,
    TxIn,
)

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("minswap.pools")


def check_valid_pool_output(utxo: Union[AddressUtxoContentItem, Output]):
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
    assets: Assets
    pool_nft: Assets
    minswap_nft: Assets
    datum_hash: str

    class Config:  # noqa: D106
        allow_mutation = False

    @root_validator(pre=True)
    def translate_address(cls, values):  # noqa: D102

        assets: Assets = values["assets"]

        # Find the NFT that assigns the pool a unique id
        nfts = [asset for asset in assets if asset.startswith(addr.POOL_NFT_POLICY_ID)]
        if len(nfts) != 1:
            raise ValueError("A pool must have one pool NFT token.")
        pool_nft = Assets(**{nfts[0]: assets.__root__.pop(nfts[0])})
        values["pool_nft"] = pool_nft

        # Find the Minswap NFT token
        nfts = [asset for asset in assets if asset.startswith(addr.FACTORY_POLICY_ID)]
        if len(nfts) != 1:
            raise ValueError("A pool must have one Minswap NFT token.")
        minswap_nft = Assets(**{nfts[0]: assets.__root__.pop(nfts[0])})
        values["minswap_nft"] = minswap_nft

        # Sometimes LP tokens for the pool are in the pool...so remove them
        pool_id = pool_nft.unit()[len(addr.POOL_NFT_POLICY_ID) :]
        lps = [asset for asset in assets if asset.endswith(pool_id)]
        for lp in lps:
            assets.__root__.pop(lp)

        non_ada_assets = [a for a in assets if a != "lovelace"]

        if len(assets) == 2:
            # ADA pair
            assert len(non_ada_assets) == 1, "Pool must only have 1 non-ADA asset."

        elif len(assets) == 3:
            # Non-ADA pair
            assert len(non_ada_assets) == 2, "Pool must only have 2 non-ADA assets."

            # Send the ADA token to the end
            values["assets"].__root__["lovelace"] = values["assets"].__root__.pop(
                "lovelace"
            )

        else:
            raise ValueError(
                "Pool must have 2 or 3 assets except factor, NFT, and LP tokens."
            )

        return values

    @property
    def id(self) -> str:
        """Pool id."""
        return self.pool_nft.unit()[len(addr.POOL_NFT_POLICY_ID) :]

    @property
    def lp_token(self) -> str:
        """Pool liquidity provider token."""
        return f"{addr.LP_POLICY_ID}{self.id}"

    @property
    def unit_a(self) -> str:
        """Token name of asset A."""
        return self.assets.unit(0)

    @property
    def unit_b(self) -> str:
        """Token name of asset b."""
        return self.assets.unit(1)

    @property
    def reserve_a(self) -> int:
        """Reserve amount of asset A."""
        return self.assets.quantity(0)

    @property
    def reserve_b(self) -> int:
        """Reserve amount of asset B."""
        return self.assets.quantity(1)

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

    @property
    def price(self) -> Tuple[Decimal, Decimal]:
        """Price of assets.

        Returns:
            A `Tuple[Decimal, Decimal] where the first `Decimal` is the price to buy
                1 of token B in units of token A, and the `Decimal` is the price to buy
                1 of token A in units of token B.
        """
        nat_assets = naturalize_assets(self.assets)

        prices = (
            (nat_assets[self.unit_a] / nat_assets[self.unit_b]),
            (nat_assets[self.unit_b] / nat_assets[self.unit_a]),
        )

        return prices

    def get_amount_out(self, asset: Assets) -> Assets:
        """Get the output asset amount given an input asset amount.

        Args:
            asset: An asset with a defined quantity.

        Returns:
            The estimated asset returned from the swap.
        """
        assert len(asset) == 1, "Asset should only have one token."
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

        numerator: int = asset.quantity() * 997 * reserve_out
        denominator: int = asset.quantity() * 997 + reserve_in * 1000

        return Assets(unit=unit_out, quantity=numerator // denominator)

    def get_amount_in(self, asset: Assets) -> Assets:
        """Get the input asset amount given a desired output asset amount.

        Args:
            asset: An asset with a defined quantity.

        Returns:
            The estimated asset needed for input in the swap.
        """
        assert len(asset) == 1, "Asset should only have one token."
        assert asset.unit() in [
            self.unit_a,
            self.unit_b,
        ], f"Asset {asset.unit} is invalid for pool {self.unit_a}-{self.unit_b}"
        if asset.unit == self.unit_b:
            reserve_in, reserve_out = self.reserve_a, self.reserve_b
            unit_out = self.unit_a
        else:
            reserve_in, reserve_out = self.reserve_b, self.reserve_a
            unit_out = self.unit_b

        numerator: int = asset.quantity() * 1000 * reserve_in
        denominator: int = (reserve_out - asset.quantity()) * 997

        return Assets(unit=unit_out, quantity=numerator // denominator)


def get_pools(
    return_non_pools: bool = False,
) -> Union[List[PoolState], Tuple[List[PoolState], List[AddressUtxoContentItem]]]:
    """Get a list of all pools.

    Args:
        return_non_pools: If True, returns UTxOs not belonging to pools as a second
            output. Default is False.

    Returns:
        A list of pools, and a list of non-pool UTxOs
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
                    tx_in=TxIn(tx_hash=utxo.tx_hash, tx_index=utxo.output_index),
                    assets=Assets(values=utxo.amount),
                    datum_hash=utxo.data_hash,
                )
            )
        else:
            non_pools.append(utxo)

    if return_non_pools:
        return pools, non_pools
    else:
        return pools


def get_pool_in_tx(tx_hash: str) -> Optional[PoolState]:
    """Get the pool state from a transaction.

    Find the pool UTxO in the transaction outputs and generate a `PoolState`.

    Args:
        tx_hash: The transaction hash.

    Returns:
        A `PoolState` if a pool is token is found, and `None` otherwise.
    """
    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    pool_tx = api.transaction_utxos(tx_hash, return_type="json")
    pool_utxo = None
    for utxo in TxContentUtxo.parse_obj(pool_tx).outputs:
        if utxo.address == addr.POOL.bech32:
            pool_utxo = utxo
            break

    if pool_utxo is None:
        return None

    check_valid_pool_output(pool_utxo)

    return PoolState(
        tx_in=TxIn(tx_hash=tx_hash, tx_index=utxo.output_index),
        assets=Assets(values=utxo.amount),
        datum_hash=utxo.data_hash,
    )


def get_pool_by_id(pool_id: str) -> Optional[PoolState]:
    """Latest `PoolState` of a pool.

    Args:
        pool_id: The unique id of the pool.

    Returns:
        A `PoolState` if the pool can be found, and `None` otherwise.
    """
    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    nft = f"{addr.POOL_NFT_POLICY_ID}{pool_id}"
    nft_txs = api.asset_transactions(
        nft, count=1, page=1, order="desc", return_type="json"
    )

    if len(nft_txs) == 0:
        return None

    nft_txs = AssetTransaction.parse_obj(nft_txs[0])

    return get_pool_in_tx(tx_hash=nft_txs.tx_hash)

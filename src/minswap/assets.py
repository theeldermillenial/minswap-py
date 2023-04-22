"""Functions for getting cardano assets."""
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Dict, MutableSet, Optional, Tuple, Union

import blockfrost
from dotenv import dotenv_values

from minswap.models import AssetIdentity, Assets
from minswap.utils import save_timestamp

ASSET_CACHE_PATH = Path(__file__).parent.joinpath("data/assets")
ASSET_CACHE_PATH.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger("minswap.assets")


@save_timestamp(ASSET_CACHE_PATH, 0, "asset")
def update_asset_info(asset: str) -> Optional[AssetIdentity]:
    """Get the latest asset information from the Cardano chain.

    Args:
        asset: The policy id plus hex encoded name of an asset.

    Returns:
        AssetIdentity
    """
    asset_path = ASSET_CACHE_PATH.joinpath(asset)

    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])
    info = api.asset(asset, return_type="json")

    asset_id = AssetIdentity.parse_obj(info)

    with open(asset_path.joinpath("asset.json"), "w") as fw:
        json.dump(asset_id.dict(), fw, indent=2)

    return asset_id


def get_asset_info(asset: str, update_cache=False) -> Optional[AssetIdentity]:
    """Get the asset information.

    Return the asset information. This will use cached information if available, and
    will fetch the asset information from the blockchain if either the cache does not
    exist of `update_cache=True`.

    Args:
        asset: The policy id plus hex encoded name of an asset.
        update_cache: If `True`, fetches the latest asset information from the
            blockchain. Default is `False`.

    Returns:
        The asset identity if available, else `None`.
    """
    asset_path = ASSET_CACHE_PATH.joinpath(asset).joinpath("asset.json")
    if update_cache or not asset_path.exists():
        return update_asset_info(asset)
    else:
        return AssetIdentity.parse_file(asset_path)


def update_assets(assets: Union[MutableSet[str], Assets]) -> None:
    """Update asset information cache.

    Args:
        assets: A list of policy ids plus hex encoded names of assets.
    """
    with ThreadPoolExecutor() as executor:
        for _ in executor.map(update_asset_info, assets):
            pass


def asset_decimals(unit: str) -> int:
    """Asset decimals.

    All asset quantities are stored as integers. The decimals indicates a scaling factor
    for the purposes of human readability of asset denominations.

    For example, ADA has 6 decimals. This means every 10**6 units (lovelace) is 1 ADA.

    Args:
        unit: The policy id plus hex encoded name of an asset.

    Returns:
        The decimals for the asset.
    """
    if unit == "lovelace":
        return 6
    else:
        info = get_asset_info(unit)
        decimals = 0 if info is None else info.decimals
        return decimals


def naturalize_assets(assets: Assets) -> Dict[str, Decimal]:
    """Get the number of decimals associated with an asset.

    This returns a `Decimal` with the proper precision context.

    Args:
        asset: The policy id plus hex encoded name of an asset.

    Returns:
        A dictionary where assets are keys and values are `Decimal` objects containing
            exact quantities of the asset, accounting for asset decimals.
    """
    nat_assets = {}
    for unit, quantity in assets.items():
        if unit == "lovelace":
            nat_assets["lovelace"] = Decimal(quantity) / Decimal(10**6)
        else:
            nat_assets[unit] = Decimal(quantity) / Decimal(10 ** asset_decimals(unit))

    return nat_assets


def asset_ticker(unit: str) -> str:
    """Ticker symbol for an asset.

    This function is designed to always return a value. If a `ticker` is available in
    the asset metadata, it is returned. Otherwise, the human readable asset name is
    returned.

    Args:
        unit: The policy id plus hex encoded name of an asset.

    Returns:
        The ticker or human readable name of an asset.
    """
    if unit == "lovelace":
        asset_name = "ADA"
    else:
        info = get_asset_info(unit)
        if info is None:
            raise ValueError("Could not find asset.")
        if info.metadata is not None and info.metadata.ticker is not None:
            asset_name = info.metadata.ticker
            logger.debug(f"Found ticker for {asset_name}.")
        elif (
            info.onchain_metadata is not None
            and hasattr(info.onchain_metadata, "symbol")
            and info.onchain_metadata.symbol is not None
        ):
            asset_name = info.onchain_metadata.symbol
            logger.debug(f"Found symbol for {asset_name}")
        else:
            try:
                asset_name = bytes.fromhex(info.asset_name).decode()
                logger.debug(
                    f"Could not find ticker for asset ({asset_name}), "
                    + "returning the name."
                )
            except UnicodeDecodeError:
                logger.debug(
                    "Could not find ticker, symbol, and asset_name was not decodable. "
                    + "Returning raw asset_name."
                )
                asset_name = info.asset_name

    return asset_name


def circulating_asset(
    asset: str = "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494e",
) -> Tuple[Assets, Assets]:
    """Calculate the amount of an asset in circulation.

    This identifies all addresses original minting transactions were sent to. Then
    the number of coins remaining in these minting addresses is subtracted from the
    total number of coins minted.

    Note:
        There is a discrepancy between the amount of circulating MIN calculated by this
        function and what is displayed on Minswap.org. This is mostly likely due to
        the token vesting schedule, and not accounting for the unlocking schedule.

    Returns:
        The first output is the amount of circulating asset, the second output is the
            total minted asset.
    """
    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    asset_updates = api.asset_history(asset, return_type="json")

    addresses = set()
    minted = 0
    for h in asset_updates:
        utxos = api.transaction_utxos(h["tx_hash"], return_type="json")

        for out_utxo in utxos["outputs"]:
            for token in out_utxo["amount"]:
                if token["unit"] == asset:
                    if h["action"] == "minted":
                        minted += int(token["quantity"])
                    else:
                        minted -= int(token["quantity"])
                    addresses.add(out_utxo["address"])
                    break

    circulating = 0
    for address in addresses:
        amounts = api.address(address, return_type="json")["amount"]

        for amount in amounts:
            if amount["unit"] == asset:
                circulating += int(amount["quantity"])

    total_asset = Assets(**{asset: minted})
    circ_asset = Assets(**{asset: circulating})

    return circ_asset, total_asset

from datetime import datetime
import json
from pathlib import Path
from typing import List, Tuple
import pandas as pd

from portfolio_tracking.class_order import Order
from portfolio_tracking.class_asset import Asset
from portfolio_tracking.portfolio_management import Portfolio


def normalize_name(name: str) -> str:
    return name.replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')


def load_assets_json_file(assets_jsonfile: Path) -> List[Asset]:
    """Charge les actifs depuis le fichier JSON
    et reconstruit l'arboressence en respectant les classes de chaque objet"""
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets_data = json.load(asset_file)

    return rebuild_assets_structure(assets_data)


def write_assets_json_file(Portfolio: Portfolio, assets_jsonfile: Path):
    with open(assets_jsonfile, 'w', encoding='utf-8') as asset_file:
        json.dump(Portfolio.to_dict(), asset_file, indent=4)


def find_asset_by_ticker(assets: List[Asset], new_asset: Asset) -> Tuple[bool, Asset]:
    """Rechercher un actif dans assets avec son ticker"""
    for asset in assets:
        if asset.ticker == new_asset.ticker:
            return True, asset
    return False, new_asset


def is_valid_date(date_str: str) -> bool:
    try:
        # Tenter de convertir la chaîne en objet datetime selon le format AAAA-MM-DD
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        # Si une erreur est levée, la date n'est pas valide
        return False


def get_first_date_from_csv(file_path: Path) -> str:
    # try:
    #     first_row = pd.read_csv(file_path, usecols=["Date"]).head(1)
    #     return str(first_row['Date'].values[0]) if not first_row.empty else None
    # except pd.errors.EmptyDataError:
    #     return None
    """
    Récupère la première date du fichier CSV.
    """
    try:
        first_row = pd.read_csv(file_path, usecols=["Date"]).head(1)
        if first_row.empty:
            raise ValueError(f"Le fichier '{file_path}' est vide ou ne contient aucune date valide.")

        first_date = str(first_row['Date'].values[0])  # Get the first date

        # TODO : Check if file has at leas one date before to try to read it,
        # if it doesn't, take self.orders[0].date as first_date

        if not is_valid_date(first_date):
            raise ValueError(f"La première date '{first_date}' dans le fichier '{file_path}' n'est pas valide.")

    except pd.errors.EmptyDataError:
        raise ValueError(f"Le fichier '{file_path}' est vide ou ne contient aucune date valide.")

    return first_date


def get_last_date_from_csv(file_path: Path) -> str:
    # try:
    #     last_row = pd.read_csv(file_path, usecols=["Date"]).tail(1)
    #     return str(last_row['Date'].values[0]) if not last_row.empty else None
    # except pd.errors.EmptyDataError:
    #     return None

    """
    Récupère la dernière date du fichier CSV.
    """

    try:
        last_row = pd.read_csv(file_path, usecols=["Date"]).tail(1)
        if last_row.empty:
            raise ValueError(f"Le fichier '{file_path}' est vide ou ne contient aucune date valide.")

        last_date = str(last_row['Date'].values[0])  # Get the last date

        if not is_valid_date(last_date):
            raise ValueError(f"La dernière date '{last_date}' dans le fichier '{file_path}' n'est pas valide.")

    except pd.errors.EmptyDataError:
        raise ValueError(f"Le fichier '{file_path}' est vide ou ne contient aucune date valide.")

    return last_date
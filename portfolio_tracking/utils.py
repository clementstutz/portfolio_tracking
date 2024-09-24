from datetime import datetime
from typing import Tuple


def normalize_name(name: str) -> str:
    return name.replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')


def is_valid_date(date_str: str) -> bool:
    try:
        # Tenter de convertir la chaîne en objet datetime selon le format AAAA-MM-DD
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        # Si une erreur est levée, la date n'est pas valide
        return False


def check_dates_boundaries(start: datetime, end: datetime, lower_bound: datetime, upper_bound: datetime) -> Tuple[datetime, datetime]:
    if lower_bound > upper_bound:
        print("WARNING: lower_bound must be lower that upper_bound!")
        lower_bound, upper_bound = upper_bound, lower_bound
    if start > end:
        start, end = end, start
    start = max(start, lower_bound)
    end = min(end, upper_bound)
    return start, end


# def write_assets_json_file(wallet: Wallet, assets_jsonfile: Path):
#     with open(assets_jsonfile, 'w', encoding='utf-8') as asset_file:
#         json.dump(wallet.to_dict(), asset_file, indent=4)


# def find_asset_by_ticker(assets: List[Asset], new_asset: Asset) -> Tuple[bool, Asset]:
#     """Rechercher un actif dans assets avec son ticker"""
#     for asset in assets:
#         if asset.ticker == new_asset.ticker:
#             return True, asset
#     return False, new_asset

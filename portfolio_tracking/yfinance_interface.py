import csv
from pathlib import Path
from typing import Dict, List
from pandas import DataFrame
import yfinance as yf


FILENAME_SUFIX = '_history.csv'


class Order:
    def __init__(self, date: str, quantity: float, price: float) -> None:
        self.date = date
        self.quantity = quantity
        self.price = price
    
    def to_dict(self) -> Dict:
        return {
            "date": self.date,
            "quantity": self.quantity,
            "price": self.price
        }


class Asset:
    def __init__(self, short_name: str, name: str, ticker: str, broker: str, devise: str, list_of_orders: List[Order]=None) -> None:
        self.short_name = short_name
        self.name = name
        self.ticker = ticker
        self.broker = broker
        self.devise = devise
        self.orders = [] if list_of_orders is None else list_of_orders
        self.dates = []
        self.closes = []

    def add_orders(self, list_of_orders: List[Order]) -> None:
        self.orders.extend(list_of_orders)
    
    def add_dates(self, list_of_dates: List) -> None:
        self.dates.extend(list_of_dates)
    
    def add_closes(self, list_of_closes: List) -> None:
        self.closes.extend(list_of_closes)

    def to_dict(self) -> Dict:
        return {
            "short_name": self.short_name,
            "name": self.name,
            "ticker": self.ticker,
            "broker": self.broker,
            "devise": self.devise,
            "orders": [order.to_dict() for order in self.orders],
            "dates": self.dates,
            "closes": self.closes
        }
    
    def _get_history(self, end_date: str, save_dir: Path, filename_sufix: str, interval: str='1d') -> DataFrame:
        # TODO : Add a function that checks if history already exist, and if it does, download only missing datas.
        # TODO : Why not use yf.Ticker("the_ticker").history() ?
        data = yf.download(tickers=self.ticker,
                           start=self.orders[0].date,
                           end=end_date,
                           interval=interval)
        
        # Réorganiser le DataFrame
        data.reset_index(inplace=True)

        # Sauvegarder au format CSV
        file_path = save_dir / Path(f"{_normalized_name(self.short_name)}{filename_sufix}")
        data.to_csv(file_path, float_format="%.4f", index=False)
        print(f'Le fichier CSV a ete telecharge avec succes et enregistre sous {file_path}')
        return data

    def download_history(self, end_date: str, save_dir: Path, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)
        
        self._get_history(end_date,
                          save_dir,
                          filename_sufix,
                          interval)
        #     if asset['devise'] != 'EUR':
        #         dowload_devise(asset, end_date)

    def load_history(self, save_dir: str, filename_sufix: str=FILENAME_SUFIX) -> None:
        dates = []
        close = []
        csv_filename = f"{_normalized_name(self.short_name)}{filename_sufix}"
        with open(Path(save_dir) / csv_filename, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)
            # Parcourir les lignes du fichier CSV
            for row in csvreader:
                if not 'null' in row['Close']:
                    # row est un dictionnaire où les clés sont les noms de colonnes
                    dates.append(row['Date'])
                    close.append(float(row['Close']))
                else:
                    print(f'ERROR in file: {csv_filename}, row = {row}')
                    # TODO: Trouver mieux que ça...
                    dates.append(row['Date'])
                    close.append(close[-1])
        self.add_dates(dates)
        self.add_closes(close)


class Assets:
    def __init__(self, list_of_assets: List[Asset]=None) -> None:
        self.assets = [] if list_of_assets is None else list_of_assets
    
    def add_asset(self, asset: Asset) -> None:
        self.assets.append(asset)
    
    def to_dict(self) -> Dict:
        return {
            "assets": [asset.to_dict() for asset in self.assets]
        }

    def download_histories(self, end_date: str, save_dir: Path, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)
        
        for asset in self.assets:
            asset._get_history(end_date,
                               save_dir,
                               filename_sufix,
                               interval)
        #     if asset['devise'] != 'EUR':
        #         dowload_devise(asset, end_date)

    def load_histories(self, save_dir: str, filename_sufix: str=FILENAME_SUFIX) -> None:
        for asset in self.assets:
            asset.load_history(save_dir, filename_sufix)

    def get_dates(self) -> List:  # OK !
        dates_temp = set()
        for asset in self.assets:
            if asset.dates == []:
                print('ERROR: asset.dates must have been defined. Please call load_histories().')
                return 1
            dates_temp.update(asset.dates)
        return list(sorted(dates_temp))


def _normalized_name(name: str) -> str:
    return name.replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')\


def rebuild_assets_structure(assets_data) -> Assets:
        """Recréer les objets à partir des données lues depuis le fichier JSON"""
        # TODO : Ajouter dates et closes ???
        restored_assets = Assets([Asset(
            asset_data["short_name"],
            asset_data["name"],
            asset_data["ticker"],
            asset_data["broker"],
            asset_data["devise"],
            [Order(
                order_data["date"],
                order_data["quantity"],
                order_data["price"]
            ) for order_data in asset_data["orders"]]
        ) for asset_data in assets_data["assets"]])
        return restored_assets

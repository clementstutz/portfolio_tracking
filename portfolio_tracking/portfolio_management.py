from typing import List, Dict, Callable
import numpy as np
import yfinance as yf
import pandas as pd
from pathlib import Path

from portfolio_tracking.data_import import DataImport
from portfolio_tracking.data_storage import DataStorage


DICT_CURRENCY = {"EURUSD": "EURUSD=X",
                 "EURGBP": "EURGBP=X"}


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
    def __init__(self, short_name: str, name: str, ticker: str, broker: str, currency: str, list_of_orders: List[Order]=None) -> None:
        self.short_name = short_name
        self.name = name
        self.ticker = ticker
        self.broker = broker
        self.currency = currency
        self.orders = [] if list_of_orders is None else list_of_orders
        self.quantity = 0
        self.dates = []
        self.closes = []

    def add_orders(self, list_of_orders: List[Order]) -> None:
        self.orders.extend(list_of_orders)

    def add_dates(self, list_of_dates: List[str]) -> None:
        self.dates.extend(list_of_dates)

    def add_closes(self, list_of_closes: List[float]) -> None:
        self.closes.extend(list_of_closes)

    def to_dict(self) -> Dict:
        return {
            "short_name": self.short_name,
            "name": self.name,
            "ticker": self.ticker,
            "broker": self.broker,
            "currency": self.currency,
            "orders": [order.to_dict() for order in self.orders],
            "dates": self.dates,
            "closes": self.closes
        }


class Portfolio:
    def __init__(self, currency: str):
        self.currency = currency
        self.assets: List[Asset] = []

    def add_assets(self, list_of_assets: List[Asset]) -> None:
        self.assets.extend(list_of_assets)

    def remove_asset(self, ticker: str) -> None:
        self.assets = [asset for asset in self.assets if asset.ticker != ticker]

    def to_dict(self) -> Dict:
        return {
            "currency": self.currency,
            "assets": [asset.to_dict() for asset in self.assets]
        }

    def get_total_value(self, date: str, data_storage: DataStorage) -> float:
        total_value = 0.0
        for asset in self.assets:
            data = data_storage.load_data(f"{asset.ticker}.csv")
            if date in data.index:
                close_price = data.loc[date, 'Close']
                total_quantity = sum(order.quantity for order in asset.orders if order.date <= date)
                total_value += close_price * total_quantity
        return total_value

    def download_histories(self) -> None:
        for asset in self.assets:
            asset.download_history(end_date,
                                   save_dir,
                                   filename_sufix,
                                   interval)


class CurrencyConverter:
    def __init__(self, target_currency: str, data_storage: DataStorage):
        self.target_currency = target_currency
        self.data_storage = data_storage

    def download_currency_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        data_importer = DataImport(ticker=ticker)
        return data_importer.download_history(start_date="2020-01-01", end_date=end_date)

    def convert_currency(self, portfolio: Portfolio, asset_ticker: str, asset_currency: str, end_date: str):
        if asset_currency != portfolio.currency:
            conversion_ticker = f"{asset_currency}{portfolio.currency}=X"
            conversion_data = self.download_currency_data(conversion_ticker, start_date="2020-01-01", end_date=end_date)
            DataStorage.save_data(conversion_data, filename=f"{conversion_ticker}_conversion.csv")
            print(f"Currency conversion data for {conversion_ticker} updated until {end_date}.")

            # Load asset data and conversion rates
            asset_data = DataStorage.load_data(filename=f"{asset_ticker}.csv")
            conversion_rates = DataStorage.load_data(filename=f"{conversion_ticker}_conversion.csv")

            # Assuming conversion rates have a 'Close' column representing the rate
            asset_data['Converted_Close'] = asset_data['Close'] * conversion_rates['Close']
            DataStorage.save_data(asset_data, filename=f"{asset_ticker}_converted.csv")
            print(f"Asset {asset_ticker} converted to {self.target_currency} and saved.")

    def _convert_to_another_currency(self, price_data: pd.DataFrame, currency_data: pd.DataFrame, currency: str) -> pd.DataFrame:
        """
        Convertit les prix d'un actif dans la devise locale vers l'euro, en utilisant l'historique des taux de change.
        """
        # Synchroniser les index des deux DataFrames (les dates doivent correspondre)
        price_data.index = pd.to_datetime(price_data.index)
        currency_data.index = pd.to_datetime(currency_data.index)

        # Assurez-vous que les deux séries ont les mêmes dates
        price_data_aligned = price_data.reindex(currency_data.index, method='ffill')

        # Convertir les colonnes pertinentes en dans la devise cible (on applique la conversion au prix de fermeture par exemple)
        for elem in COLUMNS_ORDER :
            if elem != "Volume":
                price_data_aligned[elem] = price_data_aligned[elem] / currency_data[elem]

        self.currency = currency
        return price_data_aligned

    def _dowload_currency(self, wallet_currency: str, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> pd.DataFrame:
        # TODO: Vérifier si la paire existe
        if wallet_currency + self.currency in DICT_CURRENCY:
            currency_name = wallet_currency + self.currency
        elif self.currency + wallet_currency in DICT_CURRENCY:
            currency_name = self.currency + wallet_currency
        else :
            #TODO: raise a warning!
            print(f"WARNING! Paire de devise non trouvée pour convertir {self.currency} en {wallet_currency}.")
            return None

        currency_ticker = DICT_CURRENCY.get(currency_name)

        currency = Asset(short_name=currency_name,
                         name=currency_name,
                         ticker=currency_ticker,
                         broker="None",
                         currency="",
                         list_of_orders=[Order(self.orders[0].date, 1, 1)])

        # Télécharger l'historique de la paire de devises
        currency_data = currency._get_history(end_date,
                                              save_dir,
                                              filename_sufix,
                                              interval)

        return currency_data

    def _convert_history(self, currency: str, price_data: pd.DataFrame, end_date: str, save_dir: Path, filename_sufix: str, interval: str):
        """
        Télécharge et applique les taux de change si nécessaire pour convertir dans la devise cible.
        """
        # TODO: vérifier si le fichier de la valeur converti existe déjà et appliquer la même logique qu'a _get_history
        # Télécharger la paire de devise correspondante
        currency_data = self._dowload_currency(currency,
                                               end_date,
                                               save_dir,
                                               filename_sufix,
                                               interval)

        if currency_data is not None:
            # Convertir les prix de l'actif dans la devise cible
            converted_data = self._convert_to_another_currency(price_data, currency_data, currency)

            # Sauvegarder les données converties
            file_path = save_dir / Path(f"{normalize_name(self.short_name)}_{currency}_{filename_sufix}")
            converted_data.to_csv(file_path, float_format="%.4f", index=True)
            print(f"L'historique de {self.ticker} a été converti en {currency} et enregistré sous {file_path}.")
        else:
            print(f"Conversion non effectuée pour {self.ticker}.")


class IndicatorRegistry:
    def __init__(self):
        self.indicators = {}

    def register_indicator(self, name: str, indicator_func: Callable):
        self.indicators[name] = indicator_func

    def calculate(self, name: str, *args):
        if name in self.indicators:
            return self.indicators[name](*args)
        raise ValueError(f"Indicator {name} not found.")


def download_histories(assets, end_date: str, save_dir: Path, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
    # Path.mkdir(save_dir, parents=True, exist_ok=True)
    for asset in assets:
        asset.download_history(end_date,
                               save_dir,
                               filename_sufix,
                               interval)


def load_histories(assets, end_date: str, save_dir: str, filename_sufix: str=FILENAME_SUFIX) -> None:
    for asset in assets:
        asset.load_history(save_dir, filename_sufix)


def get_dates(assets) -> List:  # OK !
    dates_temp = set()
    for asset in assets:
        if asset.dates == []:
            print('ERROR: asset.dates must have been defined. Please call load_histories().')
            # return 1
        dates_temp.update(asset.dates)
    return list(sorted(dates_temp))

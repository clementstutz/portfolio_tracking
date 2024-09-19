from typing import List, Dict
import pandas as pd

from portfolio_tracking.class_order import Order

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

    def get_first_detention_date(self) :
        return self.orders[0].date

    def get_last_detention_date(self, date: str) :
        self.quantity = 0
        for order in self.orders:
            self.quantity += order.quantity

        if self.quantity == 0 :
            if pd.to_datetime(self.orders[-1].date) + pd.Timedelta(days=1) <= pd.to_datetime(date) :
                return pd.to_datetime(self.orders[-1].date) + pd.Timedelta(days=1)
        return date



class Assets:
    def __init__(self, list_of_assets: List[Asset]=None) -> None:
        self.assets = [] if list_of_assets is None else list_of_assets

    def add_asset(self, asset: Asset) -> None:
        self.assets.append(asset)

    def remove_asset(self, asset_to_remove: Asset) -> None:
        """
        Supprime un asset de la liste des assets.

        :param asset_to_remove: L'asset à supprimer
        """
        self.assets = [asset for asset in self.assets if asset != asset_to_remove]

    def to_dict(self) -> Dict:
        return {
            "assets": [asset.to_dict() for asset in self.assets]
        }

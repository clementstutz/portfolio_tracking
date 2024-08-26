import csv
from datetime import datetime
import json
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import yfinance as yf


STOCKS_HISTORIES_DIR = Path(__file__).parent.absolute() / "histories"
ASSETS_JSON_FILENAME = "assets_real.json"
ARCHIVES_FILENAME = "Archives"
FILENAME_SUFIX = 'history.csv'
COLUMNS_ORDER = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
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
            "currency": self.currency,
            "orders": [order.to_dict() for order in self.orders],
            "dates": self.dates,
            "closes": self.closes
        }

    def _get_first_date_from_csv(self, file_path: Path) -> str:
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

    def _get_last_date_from_csv(self, file_path: Path) -> str:
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

    def _get_data_from_archives(self, start_date: str, end_date: str, file_path: Path) -> pd.DataFrame:
        # TODO: vérifier que les dates demandes sont bien dans l'archive!

        archived_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)

        # Filtrer les données archivées pour la période demandée
        archived_data_filtered = archived_data.loc[start_date:end_date]

        # Fusionner les données archivées avec les nouvelles données
        data = archived_data_filtered
        return data

    def _download_data(self, start_date: str, end_date: str, interval: str, save_dir, filename_sufix) -> pd.DataFrame:
        """
        Télécharge les données de bourse pour la période donnée.
        """
        # TODO : Why not use yf.Ticker("the_ticker").history() ?
        data = yf.download(tickers=self.ticker,
                           start=start_date,
                           end=end_date,
                           interval=interval)
        if data.empty:
            # If the download failed, check the Archives directory if there is data for this asset.
            archives_dir_path = Path(save_dir / ARCHIVES_FILENAME)
            file_path = archives_dir_path / f"{_normalized_name(self.short_name)}_{self.currency}_{filename_sufix}"
            if file_path.is_file() :
                return self._get_data_from_archives(start_date, end_date, file_path)
            else :
                # TODO: Try another method to get the datas!
                # could be done with another API (ex. Investing API (investpy on PyPi))
                # return data
                pass
            # raise ValueError(f"Aucune donnée n'a été téléchargée pour {self.ticker} entre {start_date} et {end_date}")
        return data

    def _append_new_data(self, existing_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
        """
        Combine les nouvelles données avec les données existantes et les trie par date.
        """
        if new_data.empty:
            # Si les nouvelles données sont vides, retourner simplement les données existantes
            return existing_data[COLUMNS_ORDER].sort_index()

        # Si aucune des deux n'est vide, les concaténer
        combined_data = pd.concat([existing_data, new_data])
        return combined_data[COLUMNS_ORDER].sort_index()

    def _update_with_old_data(self, file_path: Path, first_date: str, interval: str, save_dir, filename_sufix) -> None:
        """
        Télécharge et ajoute les données manquantes antérieures à la première date du fichier existant.
        """
        old_data = self._download_data(start_date=self.orders[0].date,
                                       end_date=pd.to_datetime(first_date).strftime('%Y-%m-%d'),
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)
        existing_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)
        combined_data = self._append_new_data(old_data, existing_data)
        combined_data.to_csv(file_path, float_format="%.4f", index=True)
        print(f"Le fichier CSV a été mis à jour avec d'anciennes données et sauvegardé sous {file_path}")

    def _update_with_new_data(self, file_path: Path, last_date: str, end_date: str, interval: str, save_dir, filename_sufix) -> None:
        """
        Télécharge et ajoute les données manquantes postérieures à la dernière date du fichier existant.
        """
        new_data = self._download_data(start_date=pd.to_datetime(last_date) + pd.Timedelta(days=1),
                                       end_date=end_date,
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)
        existing_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)
        combined_data = self._append_new_data(existing_data, new_data)
        combined_data.to_csv(file_path, float_format="%.4f", index=True)
        print(f'Le fichier CSV a été mis à jour avec de nouvelles données et sauvegardé sous {file_path}')

    def _initialize_new_file(self, file_path: Path, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> None:
        """
        Télécharge toutes les données et crée un nouveau fichier CSV si celui-ci n'existe pas.
        """
        data = self._download_data(start_date=self.orders[0].date,
                                   end_date=end_date,
                                   interval=interval,
                                   save_dir=save_dir,
                                   filename_sufix=filename_sufix)
        # Réordonner les colonnes et s'assurer qu'elles sont ordonnées par date
        data = data[COLUMNS_ORDER].sort_index()
        data.to_csv(file_path, float_format="%.4f", index=True)
        print(f'Le fichier CSV a été sauvegardé avec succès sous {file_path}')

    def _get_first_detention_date(self) :
        return self.orders[0].date

    def _get_last_detention_date(self, date) :
        self.quantity = 0
        for order in self.orders:
            self.quantity += order.quantity

        if self.quantity == 0 :
            if pd.to_datetime(self.orders[-1].date) + pd.Timedelta(days=1) <= pd.to_datetime(date) :
                return pd.to_datetime(self.orders[-1].date) + pd.Timedelta(days=1)
        return date

    def _update_history(self, file_path: Path, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> pd.DataFrame:
        # FIXME : Ne fonctionne surement pas avec les jour fériers !
        first_date = self._get_first_date_from_csv(file_path)
        last_date = self._get_last_date_from_csv(file_path)

        # Vérifier si des données plus anciennes doivent être téléchargées
        if pd.to_datetime(self.orders[0].date) < pd.to_datetime(first_date) :
            self._update_with_old_data(file_path, first_date, interval, save_dir, filename_sufix)

        # Vérifier si des données plus récentes doivent être téléchargées
        end_date = self._get_last_detention_date(end_date)
        # TODO: change the value "2" of pd.Timedelta(days=2) to "1", but need to manage case of dalayed data
        if pd.to_datetime(last_date) + pd.Timedelta(days=2) < pd.to_datetime(end_date):
            self._update_with_new_data(file_path, last_date, end_date, interval, save_dir, filename_sufix)

        else :
            print(f"Aucune nouvelle donnée à télécharger pour {self.short_name}, les données sont déjà à jour.")

        return pd.read_csv(file_path, index_col='Date', parse_dates=True)

    def _get_history(self, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> pd.DataFrame:
        """
        Télécharge les données boursières et met à jour le fichier CSV avec les nouvelles données.
        """
        file_path = save_dir / Path(f"{_normalized_name(self.short_name)}_{self.currency}_{filename_sufix}")
        end_date = self._get_last_detention_date(end_date)

        if not file_path.is_file() :
            # Créer un nouveau fichier avec toutes les données si le fichier n'existe pas
            self._initialize_new_file(file_path, end_date, save_dir, filename_sufix, interval)
            self._update_history(file_path, end_date, save_dir, filename_sufix, interval)
        else :
            self._update_history(file_path, end_date, save_dir, filename_sufix, interval)

        # Charger les données existantes et les renvoie
        return pd.read_csv(file_path, index_col='Date', parse_dates=True)

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
            file_path = save_dir / Path(f"{_normalized_name(self.short_name)}_{currency}_{filename_sufix}")
            converted_data.to_csv(file_path, float_format="%.4f", index=True)
            print(f"L'historique de {self.ticker} a été converti en {currency} et enregistré sous {file_path}.")
        else:
            print(f"Conversion non effectuée pour {self.ticker}.")

    def download_history(self, end_date: str, save_dir: Path, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)

        last_detention_date = self._get_last_detention_date(end_date)
        price_data = self._get_history(last_detention_date,
                                       save_dir,
                                       filename_sufix,
                                       interval)
        if not price_data.empty :
            # TODO: get wallet currency instead
            if self.currency != "EUR":
                self._convert_history("EUR",
                                      price_data,
                                      last_detention_date,
                                      save_dir,
                                      filename_sufix,
                                      interval)

    def load_history(self, save_dir: Path, filename_sufix: str=FILENAME_SUFIX) -> None:
        """
        Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
        """
        csv_filename = Path(f"{_normalized_name(self.short_name)}_{self.currency}_{filename_sufix}")
        file_path = save_dir / csv_filename

        try:
            # Lire le fichier CSV en utilisant pandas
            df = pd.read_csv(file_path, usecols=["Date", "Close"], parse_dates=["Date"])

            # Remplacer les valeurs "null" ou NaN dans la colonne 'Close'
            df['Close'] = df['Close'].replace('null', pd.NA)
            df['Close'] = df['Close'].ffill()  # Utiliser la méthode forward fill pour remplacer les NaN

            if df['Close'].isna().any():
                raise ValueError(f"Le fichier {csv_filename} contient des valeurs 'Close' manquantes ou invalides.")

            # Extraire les dates et les prix de clôture
            self.add_dates(df['Date'].dt.strftime('%Y-%m-%d').tolist())
            self.add_closes(df['Close'].astype(float).tolist())

        except FileNotFoundError:
            print(f"Le fichier {csv_filename} n'a pas été trouvé dans le répertoire {save_dir}.")
        except pd.errors.EmptyDataError:
            print(f"Le fichier {csv_filename} est vide ou ne contient aucune donnée valide.")
        except ValueError as ve:
            print(f"Erreur lors du chargement des données depuis {csv_filename} : {ve}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue lors du chargement de {csv_filename} : {e}")



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

    def download_histories(self, end_date: str, save_dir: Path, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
        # Path.mkdir(save_dir, parents=True, exist_ok=True)

        for asset in self.assets:
            asset.download_history(end_date,
                                   save_dir,
                                   filename_sufix,
                                   interval)

    def load_histories(self, save_dir: str, filename_sufix: str=FILENAME_SUFIX) -> None:
        for asset in self.assets:
            asset.load_history(save_dir, filename_sufix)

    def get_dates(self) -> List:  # OK !
        dates_temp = set()
        for asset in self.assets:
            if asset.dates == []:
                print('ERROR: asset.dates must have been defined. Please call load_histories().')
                # return 1
            dates_temp.update(asset.dates)
        return list(sorted(dates_temp))


def _normalized_name(name: str) -> str:
    return name.replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')


def rebuild_assets_structure(assets_data) -> Assets:
        """Recréer les objets à partir des données lues depuis le fichier JSON"""
        # TODO : Ajouter dates et closes ???
        restored_assets = Assets([Asset(
            asset_data["short_name"],
            asset_data["name"],
            asset_data["ticker"],
            asset_data["broker"],
            asset_data["currency"],
            [Order(
                order_data["date"],
                order_data["quantity"],
                order_data["price"]
            ) for order_data in asset_data["orders"]]
        ) for asset_data in assets_data["assets"]])
        return restored_assets


def is_valid_date(date_str: str) -> bool:
    try:
        # Tenter de convertir la chaîne en objet datetime selon le format AAAA-MM-DD
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        # Si une erreur est levée, la date n'est pas valide
        return False


def load_assets_json_file(assets_jsonfile: Path) -> Assets:
    """Charge les actifs depuis le fichier JSON
    et reconstruit l'arboressence en respectant les classes de chaque objet"""
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets_data = json.load(asset_file)

    return rebuild_assets_structure(assets_data)


def write_assets_json_file(assets: Assets, assets_jsonfile: Path):
    with open(assets_jsonfile, 'w', encoding='utf-8') as asset_file:
        json.dump(assets.to_dict(), asset_file, indent=4)


def find_asset_by_ticker(assets: Assets, new_asset: Asset) -> Tuple[bool, Asset]:
    """Rechercher un actif dans assets avec son ticker"""
    for asset in assets.assets:
        if asset.ticker == new_asset.ticker:
            return True, asset
    return False, new_asset

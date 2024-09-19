from collections import defaultdict
import csv
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Dict, List, Tuple
import pandas as pd
import yfinance as yf


HISTORIES_DIR_PATH = Path(__file__).parent.absolute() / "histories"
ARCHIVES_DIR_NAME = "Archives"
ASSETS_JSON_FILENAME = "assets_real.json"
HISTORY_FILENAME_SUFIX = 'history.csv'
COLUMNS_ORDER = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DICT_CURRENCY = {"EURUSD": "EURUSD=X",
                 "EURGBP": "EURGBP=X"}

class DatabaseManager:
    def __init__(self, db_path: Path=HISTORIES_DIR_PATH/"data_base.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS Assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_name TEXT NOT NULL,
                name TEXT NOT NULL,
                ticker TEXT NOT NULL UNIQUE,  -- Contrainte d'unicité sur le ticker
                broker TEXT,
                currency TEXT NOT NULL
            );
            """)
            self.conn.execute("""CREATE TABLE IF NOT EXISTS Orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                FOREIGN KEY(asset_id) REFERENCES Assets(id),
                UNIQUE(asset_id, date, quantity, price)  -- Contrainte d'unicité de l'ordre
            );
            """)
            self.conn.execute("""CREATE INDEX IF NOT EXISTS idx_orders_date ON Orders(date);
                -- Créer un index sur la colonne "date" pour améliorer les performances des requêtes basées sur des dates specifiques""")
            self.conn.execute("""CREATE INDEX IF NOT EXISTS idx_orders_asset_id ON Orders(asset_id);
                -- Créer un index sur la colonne "asset_id" pour améliorer les performances des requêtes basées sur un asset specifique""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS Dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE  -- Stocke chaque date une seule fois
            );
            """)
            self.conn.execute("""CREATE INDEX IF NOT EXISTS idx_dates_date ON Dates(date);
                -- Créer un index sur la colonne "date" pour améliorer les performances des requêtes basées sur des dates specifiques""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                date_id INTEGER NOT NULL,
                open REAL,
                close REAL,
                open_other_currency REAL,
                close_other_currency REAL,
                FOREIGN KEY(asset_id) REFERENCES Assets(id),
                FOREIGN KEY(date_id) REFERENCES Dates(id),
                UNIQUE(asset_id, date_id)  -- Unicité par actif et date
            );
            """)
            self.conn.execute("""CREATE TABLE IF NOT EXISTS CurrencyRates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                currency_pair TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL NOT NULL,
                close REAL NOT NULL,
                UNIQUE(currency_pair, date)  -- Contrainte d'unicité des cotation
            );
            """)

    def execute_query(self, query: str, params: tuple=()):
        with self.conn:
            cursor = self.conn.execute(query, params)
        return cursor

    def execute_many_query(self, query: str, params: List=[]):  # TODO: v"rifier si ça fonctionne bien !
        with self.conn:
            cursor = self.conn.executemany(query, params)
            self.conn.commit()
        return cursor

    def get_dates(self, start_date: str, end_date: str) -> List[Tuple[str]]:
        """
        Args:
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme 'YYYY-MM-DD'.
        Returns:
            List[str]: tel que [date].
        """
        query = """
        SELECT DISTINCT d.date
        FROM Dates d
        WHERE d.date BETWEEN ? AND ?
        ORDER BY d.date ASC
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (start_date, end_date))
        return cursor.fetchall()

    def get_all_assets_prices_between_dates(self, start_date: str, end_date: str) -> List[Tuple[str, int, float]]:
        """
        Args:
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme 'YYYY-MM-DD'.
        Returns:
            List[Tuple[str, int, float]]: tel que [[date, asset_id, close]].
        """
        query = """
        SELECT d.date, p.asset_id, p.close
        FROM Prices p
        JOIN Dates d ON p.date_id = d.id
        JOIN Assets a ON p.asset_id = a.id
        WHERE d.date BETWEEN ? AND ?
        ORDER BY d.date ASC
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (start_date, end_date))
        return cursor.fetchall()

    def get_one_asset_prices_between_dates(self, asset_id: int, start_date: str, end_date: str) -> List[Tuple[str, float]]:
        """
        Args:
            asset_id (int): Id de l'asset pour lequel on souhaite les prix.
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme 'YYYY-MM-DD'.
        Returns:
            List[Tuple[str, float]]: Liste des assets détenus avec leurs quantités, tel que [[date, close]].
        """
        query = """
        SELECT d.date, p.close
        FROM Prices p
        JOIN Dates d ON p.date_id = d.id
        WHERE p.asset_id = ? AND d.date BETWEEN ? AND ?
        ORDER BY d.date ASC
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (asset_id, start_date, end_date))
        return cursor.fetchall()

    def get_one_asset_one_price(self, asset_id: int, date: str) -> List[Tuple[str, float]]:
        """
        Args:
            asset_id (int): Id de l'asset pour lequel on souhaite le prix.
            date (str): La date (sous forme 'YYYY-MM-DD') pour laquelle on souhaite le prix.
        Returns:
            List[Tuple[str, float]]: tel que [[date, close]].
        """
        query = """
        SELECT d.date, p.close
        FROM Prices p
        JOIN Dates d ON p.date_id = d.id
        WHERE p.asset_id = ? AND d.date = ?
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (asset_id, date))
        return cursor.fetchall()

    def get_assets_held_between_dates(self, start_date: str, end_date: str) -> List[Tuple[int, str, float]]:
        """
        Récupère la liste des assets détenus ou qui ont été détenus entre deux dates données.
        Args:
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme 'YYYY-MM-DD'.
        Returns:
            List[Tuple[int, str, float]]: Liste des assets détenus avec leurs quantités, tel que [[asset_id, asset_short_name, asset_total_quantity]].
        """
        #TODO : faire en sorte de comptabiliser aussi les asset qui ont été détenus sur la periode, mais qui ne le sont plus à la fin.
        query = """
        SELECT a.id, a.short_name, SUM(o.quantity) as total_quantity
        FROM Orders o
        JOIN Assets a ON o.asset_id = a.id
        WHERE o.date <= ?  -- Date de fin de la période (cette ligne récupère les ordres jusqu'à la date de fin)
        GROUP BY o.asset_id
        HAVING SUM(o.quantity) > 0  -- On vérifie que la quantité totale d'actions est positive, ce qui signifie qu'on détient encore cet asset
        AND EXISTS (
            SELECT 1
            FROM Orders o2
            WHERE o2.asset_id = o.asset_id
            AND o2.date >= ?   -- Date de début de la période
        )
        ORDER BY a.name ASC;
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (end_date, start_date))
        return cursor.fetchall()

    def get_asset_total_quantity_at_date(self, asset_id: int, date: str) -> float:
        """
        Récupère le nombre total d'actions détenues pour un asset donné à une date spécifique.
        Args:
            asset_id (int): L'identifiant de l'asset.
            date (str): La date jusqu'à laquelle on veut connaître le nombre d'actions (format 'YYYY-MM-DD').
        Returns:
            float: Le nombre total d'actions détenues jusqu'à la date donnée.
        """
        query = """
        SELECT SUM(o.quantity)
        FROM Orders o
        WHERE o.asset_id = ? AND o.date <= ?;
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (asset_id, date))
        result = cursor.fetchone()

        # Si le résultat est None, cela signifie qu'il n'y a pas eu de transactions pour cet asset jusqu'à cette date
        total_quantity = result[0] if result[0] is not None else 0

        return total_quantity

    def get_all_assets_quantities_between_dates(self, start_date: str, end_date: str) -> List[Tuple[str, int, float]]:
        #TODO : à retravailler pour que si on veut les quantités entre deux date (ex 2023-01-01 au 2023-12-31)
        # mais que si à la date de début il n'y à pas d'ordre ce jour-là, la méthode ne retourne pas 0.
        """
        Récupérer toutes les transactions (achats/ventes) pour tous les assets jusqu'à la date de fin spécifiée.

        Args:
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme de chaîne (format 'YYYY-MM-DD').

        Returns:
            List[Tuple[str, int, float]]: tel que [[date, asset_id, total_quantity]].
        """
        # Optimized query to fetch dates, assets, and running totals
        # query_combined = """
        # SELECT d.date, a.id, SUM(COALESCE(o.quantity, 0)) OVER (PARTITION BY a.id ORDER BY d.date) as total_quantity
        # FROM Dates d
        # CROSS JOIN Assets a
        # LEFT JOIN Orders o ON d.date = o.date AND a.id = o.asset_id
        # WHERE d.date BETWEEN ? AND ?
        # ORDER BY d.date ASC;
        # """
        # cursor_combined = self.execute_query(query_combined, (start_date, end_date))
        # return cursor_combined.fetchall()

        # Fetch the last known quantities before the start date
        query_initial = """
        SELECT a.id, SUM(COALESCE(o.quantity, 0)) as initial_quantity
        FROM Assets a
        LEFT JOIN Orders o ON a.id = o.asset_id AND o.date < ?
        GROUP BY a.id;
        """
        cursor_initial = self.execute_query(query_initial, (start_date,))
        initial_quantities = {row[0]: row[1] for row in cursor_initial.fetchall()}

        # Optimized query to fetch dates, assets, and total quantities
        query_combined = """
        SELECT d.date, a.id, COALESCE(SUM(o.quantity), 0) as total_quantity
        FROM Dates d
        CROSS JOIN Assets a
        LEFT JOIN Orders o ON d.date = o.date AND a.id = o.asset_id
        WHERE d.date BETWEEN ? AND ?
        GROUP BY d.date, a.id
        ORDER BY d.date ASC;
        """
        cursor_combined = self.execute_query(query_combined, (start_date, end_date))
        combined_data = cursor_combined.fetchall()

        results = []
        asset_quantities = defaultdict(float, initial_quantities)  # Start with initial quantities

        # Calculate running totals
        for date, asset_id, total_quantity in combined_data:
            # Update the asset quantity based on the total quantity for the current date
            asset_quantities[asset_id] += total_quantity

            # If the date is the start date and there are no orders, use the initial quantity
            if date == start_date and asset_quantities[asset_id] == 0:
                asset_quantities[asset_id] = initial_quantities.get(asset_id, 0)

            results.append((date, asset_id, asset_quantities[asset_id]))

        return results

    def get_asset_id_by_ticker(self, ticker: str) -> int:
        """
        Args:
            ticker (str): Le ticker de l'asset.
        Returns:
            int: L'id de l'asset.
        """
        query = """
        SELECT id FROM Assets WHERE ticker = ?
        """
        # Exécuter la requête pour récupérer les cours de clôture dans la plage de dates
        cursor = self.execute_query(query, (ticker,))
        result = cursor.fetchone()

        if result is None:
            raise ValueError(f"Asset with ticker {ticker} not found")

        return result[0]

    def add_order(self, ticker: str, date: str, quantity: float, price: float) -> None:
        """
        Args:
            ticker (str): Le ticker de l'asset.
            date (str): la date de l'ordre.
            quantity (float): La quantité acheté (valeur positive) ou vendue (valeur négative).
            price (float): Le prix d'éxécution de l'ordre.
        Returns:
            None
        """
        query = """
        INSERT OR IGNORE INTO Orders (asset_id, date, quantity, price)
        VALUES (?, ?, ?, ?)
        """
        self.execute_query(query, (self.get_asset_id_by_ticker(ticker), date, quantity, price))

    def insert_dates_batch(self, dates: List[str]) -> None:
        """
        Args:
            dates (List[str]): Liste des dates à insérer au format 'YYYY-MM-DD'
        Returns:
            None
        """
        query = """
        INSERT OR IGNORE INTO Dates (date) VALUES (?)
        """
        self.execute_many_query(query, [(date,) for date in dates])

    def get_dates_ids(self, dates: List[str]) -> Dict[str, int]:
        """
        Utiliser une requête pour récupérer tous les IDs des dates en une seule fois
        Args:
            dates (List[str]): Liste des dates au format 'YYYY-MM-DD' pour lesquelles on veut l'ID.
        Returns:
            Dict[str, int]: Retourn un dico tel que {date: id}
        """
        query = """
        SELECT id, date FROM Dates WHERE date IN ({})
        """.format(",".join("?" for _ in dates))

        cursor = self.execute_query(query, (dates))

        # Créer un dictionnaire {date: id}
        return {row[1]: row[0] for row in cursor.fetchall()}

    def insert_prices_batch(self, asset_id: int, date_ids: Dict[str, int], list_of_entries: List[Tuple[str, float, float]]) -> None:
        """
        Args:
            asset_id (int): Id del'asset pour lequel on veut insérer des prix.
            date_ids (Dict[str, int]): Dictionnaire contenant les ids des dates pour lesquelles on veut insérer des prix.
            list_of_entries (List[Tuple[str, float, float]]): Liste de Tuples avec les dates au format 'YYYY-MM-DD' et les prix d'ouverture et de cloture.
        Returns:
            None
        """
        query = """
        INSERT OR IGNORE INTO Prices (asset_id, date_id, open, close)
        VALUES (?, ?, ?, ?)
        """
        # Préparer les données pour l'insertion
        data_to_insert = [(asset_id, date_ids[date], open_price, close_price) for date, open_price, close_price in list_of_entries]

        # Insérer les prix en une seule opération
        self.execute_many_query(query, data_to_insert)

    def insert_one_asset(self, short_name: str, name: str, ticker: str, broker: str, currency: str) -> None:
        """
        Args:
            short_name (str): short_name of the asset.
            name (str): short_name of the asset.
            ticker (str): ticker of the asset.
            broker (str): broker where you hold the asset.
            currency (str): currency of the asset.
        Returns:
            None
        """
        query = """
        INSERT OR IGNORE INTO Assets (short_name, name, ticker, broker, currency)
        VALUES (?, ?, ?, ?, ?)
        """
        self.execute_query(query, (short_name, name, ticker, broker, currency))

    # def get_close_price_of_a_day(self, asset_id: int, date: str) -> float:  # OK !
    #     if date in asset.dates:
    #         index = asset.dates.index(date)
    #         return asset.closes[index]

    #     previous_index = self.dates.index(date) -1
    #     while previous_index > 0 :
    #         previous_date = self.dates[previous_index]
    #         if previous_date in asset.dates:
    #             index = asset.dates.index(previous_date)
    #             return asset.closes[index]
    #         else :
    #             previous_index -= 1

    #     print('ERROR: try to get the price for a date before your first order for this asset.')
    #     return 1

    def get_all_cashflows_between_dates(self, start_date: str, end_date: str) -> List[Tuple[str, float]]:
        """
        Calculate the total cashflows for each days between strat and end date.

        Args:
            start_date (str): La date de début sous forme 'YYYY-MM-DD'.
            end_date (str): La date de fin sous forme de chaîne (format 'YYYY-MM-DD').

        Returns:
            List[Tuple[str, float]]: tel que [[date, cashflows]].
        """
        query = """
        SELECT d.date, COALESCE(SUM(o.quantity * o.price), 0) AS total_cashflow
        FROM Dates d
        LEFT JOIN Orders o ON d.date = o.date
        WHERE d.date BETWEEN ? AND ?
        GROUP BY d.date
        ORDER BY d.date ASC;
        """
        cursor = self.execute_query(query, (start_date, end_date))
        cashflows_data  = cursor.fetchall()
        cursor.close()

        return cashflows_data

    def get_first_date(self) -> str:
        """
        Retrieves the earliest transaction date from the orders.
        This method queries the database to find the minimum date from the Orders table,
        which represents the start date of the portfolio.
        If no transactions are found, it returns the current date in 'YYYY-MM-DD' format.
        Args:
            self: The instance of the class.
        Returns:
            str: The earliest transaction date or the current date if no transactions exist.
        """
        # Ici, on suppose que la première transaction représente le début du portefeuille
        query = """SELECT MIN(date) FROM Orders;"""
        cursor = self.execute_query(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result[0] is not None else datetime.now().strftime('%Y-%m-%d')

    def close(self):
        self.conn.close()


class Order:
    def __init__(self, date: str, quantity: float, price: float) -> None:
        self.date = date
        self.quantity = quantity
        self.price = price

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Order):
            these_order = self.date, self.quantity, self.price
            other_order = other.date, other.quantity, other.price
            return these_order == other_order
        return NotImplemented # important, you don't want to return None

    def to_dict(self) -> Dict:
        return {
            "date": self.date,
            "quantity": self.quantity,
            "price": self.price
        }

    def __repr__(self, indent=0):
        indentation = " " * indent
        return f"{indentation}{{'date': '{self.date}', 'quantity': {self.quantity}, 'price': {self.price}}}"

class Asset:
    def __init__(self, short_name: str, name: str, ticker: str, broker: str, currency: str, list_of_orders: List[Order]=None) -> None:
        """Constructor.

        Parameters
        ----------
        short_name : str
            Short name of the asset.
        name : str
            Official long name of the asset.
        ticker : str
            Ticker of the asset.
        broker : str
            Broker where you hold the asset.
        currency : str
            Asset currency.
        list_of_orders : List[Order]=None
            List of orders placed on this asset.
        """
        self.short_name = short_name
        self.name = name
        self.ticker = ticker
        self.broker = broker
        self.currency = currency
        self.orders: List[Order] = [] if list_of_orders is None else list_of_orders

    def _order_already_exist(self, order: Order) -> bool:
        # TODO: Améliorer cette méthode pour plus de granularité
        if order in self.orders:
            return True
        return False

    def add_orders(self, db_manager: DatabaseManager, list_of_orders: List[Order]) -> None:
        for order in list_of_orders:
            db_manager.add_order(self.ticker, order.date, order.quantity, order.price)    # On peut l'optimiser avec un executmany en utilisant directement la liste
            if not self._order_already_exist(order):
                self.orders.append(order)

    def to_dict(self) -> Dict:
        return {
            "short_name": self.short_name,
            "name": self.name,
            "ticker": self.ticker,
            "broker": self.broker,
            "currency": self.currency,
            "orders": [order.to_dict() for order in self.orders]
        }

    def __repr__(self, indent=0):
        indentation = " " * indent
        orders_str = "["
        for order in self.orders:
            orders_str += f"\n{order.__repr__(indent + 4)},"
        orders_str += "]"

        return f"{indentation}{{'short_name': '{self.short_name}', 'name': '{self.name}', 'ticker': '{self.ticker}', 'broker': '{self.broker}', 'currency': '{self.currency}', 'orders': {orders_str}}}"

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


    def _get_data_from_archives(self, file_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
        # TODO: vérifier que les dates demandes sont bien dans l'archive!
        archived_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)

        # Filtrer les données archivées pour la période demandée
        return archived_data.loc[start_date:end_date]

    def _reorgenize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Réordonner les colonnes et les trie par date.
        """
        return df[COLUMNS_ORDER].sort_index()

    def _download_data(self, start_date: str, end_date: str, interval: str, save_dir: Path, filename_sufix: str) -> pd.DataFrame:
        """
        Télécharge les données de bourse pour la période donnée.
        """
        # TODO : Why not use yf.Ticker("the_ticker").history() ?
        data:pd.DataFrame = yf.download(tickers=self.ticker,
                                        start=start_date,
                                        end=end_date,
                                        interval=interval)

        if data.empty:
            # If the download failed, check the Archives directory if there is data for this asset.
            archives_dir_path = Path(save_dir / ARCHIVES_DIR_NAME)
            file_path = archives_dir_path / f"{_normalized_name(self.short_name)}_{self.currency}_{filename_sufix}"
            if file_path.is_file() :
                return self._get_data_from_archives(file_path=file_path,
                                                    start_date=start_date,
                                                    end_date=end_date)
            # else :
            #     # TODO: Try another method to get the datas!
            #     # could be done with another API (ex. Investing API (investpy on PyPi))
            #     # return data
            #     pass
            # raise ValueError(f"Aucune donnée n'a été téléchargée pour {self.ticker} entre {start_date} et {end_date}")
        return self._reorgenize_data(data)

    def _concat_data(self, first_dataframe: pd.DataFrame, second_dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Combine les nouvelles données avec les données existantes.
        """
        if first_dataframe.empty and second_dataframe.empty:
            # Si les deux entrées sont vides, retourner un warning et un dataframe vide.
            print("WARNIGN: first_dataframe and second_dataframe are empty.")
            return first_dataframe

        elif first_dataframe.empty:
            print("WARNIGN: first_dataframe is empty.")
            return second_dataframe

        elif second_dataframe.empty:
            print("WARNIGN: second_dataframe is empty.")
            return first_dataframe
        else:
            return pd.concat([first_dataframe, second_dataframe])

    def _initialize_new_file(self, file_path: Path, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> None:
        """
        Télécharge toutes les données et crée un nouveau fichier CSV si celui-ci n'existe pas.
        """
        data = self._download_data(start_date=self.orders[0].date,
                                   end_date=end_date,
                                   interval=interval,
                                   save_dir=save_dir,
                                   filename_sufix=filename_sufix)
        data.to_csv(file_path, float_format="%.4f", index=True)
        print(f'Le fichier CSV a été sauvegardé avec succès sous {file_path}')

    def _update_with_old_data(self, file_path: Path, start_date: str, end_date: str, interval: str, save_dir: Path, filename_sufix: str) -> None:
        """
        Télécharge et ajoute les données manquantes antérieures à la première date du fichier existant.
        """
        old_data = self._download_data(start_date=start_date,
                                       end_date=pd.to_datetime(end_date).strftime('%Y-%m-%d'),
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)
        existing_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)
        combined_data = self._concat_data(first_dataframe=old_data, second_dataframe=existing_data)
        combined_data.to_csv(file_path, float_format="%.4f", index=True)
        print(f"Le fichier CSV a été mis à jour avec d'anciennes données et sauvegardé sous {file_path}")

    def _update_with_new_data(self, file_path: Path, start_date: str, end_date: str, interval: str, save_dir: Path, filename_sufix: str) -> None:
        """
        Télécharge et ajoute les données manquantes postérieures à la dernière date du fichier existant.
        """
        new_data = self._download_data(start_date=pd.to_datetime(start_date) + pd.Timedelta(days=1),
                                       end_date=end_date,
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)
        existing_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)
        combined_data = self._concat_data(first_dataframe=existing_data, second_dataframe=new_data)
        combined_data.to_csv(file_path, float_format="%.4f", index=True)
        print(f'Le fichier CSV a été mis à jour avec de nouvelles données et sauvegardé sous {file_path}')

    def _update_history(self, file_path: Path, start_date: str, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> pd.DataFrame:
        # FIXME : Ne fonctionne surement pas avec les jour fériers !
        first_date = self._get_first_date_from_csv(file_path)
        last_date = self._get_last_date_from_csv(file_path)

        need_older_data: bool = pd.to_datetime(start_date) < pd.to_datetime(first_date)
        need_newer_data: bool = pd.to_datetime(last_date) + pd.Timedelta(days=2) < pd.to_datetime(end_date)

        # Vérifier si des données plus anciennes doivent être téléchargées
        if need_older_data :
            self._update_with_old_data(file_path=file_path,
                                       start_date=start_date,
                                       end_date=first_date,
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)

        # Vérifier si des données plus récentes doivent être téléchargées
        # TODO: change the value "2" of pd.Timedelta(days=2) to "1", but need to manage case of dalayed data
        if need_newer_data :
            self._update_with_new_data(file_path=file_path,
                                       start_date=last_date,
                                       end_date=end_date,
                                       interval=interval,
                                       save_dir=save_dir,
                                       filename_sufix=filename_sufix)

        else :
            print(f"Aucune nouvelle donnée à télécharger pour {self.short_name}, les données sont déjà à jour.")

        return pd.read_csv(file_path, index_col='Date', parse_dates=True)

    def _get_history(self, end_date: str, save_dir: Path, filename_sufix: str, interval: str) -> pd.DataFrame:
        """
        Télécharge les données boursières et met à jour le fichier CSV avec les nouvelles données.
        """
        file_path = save_dir / Path(f"{_normalized_name(self.short_name)}_{self.currency}_{filename_sufix}")

        if not file_path.is_file() :
            # Créer un nouveau fichier avec toutes les données si le fichier n'existe pas
            self._initialize_new_file(file_path=file_path,
                                      end_date=end_date,
                                      save_dir=save_dir,
                                      filename_sufix=filename_sufix,
                                      interval=interval)

        self._update_history(file_path=file_path,
                             start_date=self.orders[0].date,
                             end_date=end_date,
                             save_dir=save_dir,
                             filename_sufix=filename_sufix,
                             interval=interval)

        # Charger les données existantes et les renvoie
        return pd.read_csv(file_path, index_col='Date', parse_dates=True)

    def _convert_to_another_currency(self, price_data: pd.DataFrame, currency_data: pd.DataFrame, currency: str) -> pd.DataFrame:
        """
        Convertit les prix d'un actif dans la devise locale vers l'euro, en utilisant l'historique des taux de change.
        """
        # Synchroniser les index des deux DataFrames (les dates doivent correspondre)
        price_data.index = pd.to_datetime(price_data.index)
        currency_data.index = pd.to_datetime(currency_data.index)

        # Filtrer currency_data pour ne garder que les dates présentes dans price_data
        currency_data_filtered = currency_data.reindex(price_data.index, method='ffill')

        # Convertir les colonnes pertinentes dans la devise cible
        for elem in COLUMNS_ORDER :
            if elem != "Volume":
                price_data[elem] = price_data[elem] / currency_data_filtered[elem]

        # Conversion des prix dans les ordres
        for order in self.orders :
            order_date = pd.to_datetime(order.date)
            if order_date in currency_data_filtered.index:
                order.price = order.price / float(currency_data_filtered.loc[order_date, 'Close'])  # Supposons que le taux de change est dans la colonne 'Close'
            else:
                # Si la date de l'ordre n'est pas dans les données de change, utilisez la dernière valeur connue
                order.price = order.price / float(currency_data_filtered.iloc[-1]['Close'])

        self.currency = currency
        return price_data

    def _dowload_currency(self, wallet_currency: str, end_date: str, save_dir: Path, filename_sufix: str, interval: str, db_manager: DatabaseManager) -> pd.DataFrame:
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

    def _convert_history(self, currency: str, price_data: pd.DataFrame, end_date: str, save_dir: Path, filename_sufix: str, interval: str, db_manager: DatabaseManager):
        """
        Télécharge et applique les taux de change si nécessaire pour convertir dans la devise cible.
        """
        # TODO: vérifier si le fichier de la valeur converti existe déjà et appliquer la même logique qu'a _get_history
        # Télécharger la paire de devise correspondante
        currency_data = self._dowload_currency(currency,
                                               end_date,
                                               save_dir,
                                               filename_sufix,
                                               interval,
                                               db_manager)

        if currency_data is not None:
            # Convertir les prix de l'actif dans la devise cible
            converted_data = self._convert_to_another_currency(price_data, currency_data, currency)

            # Sauvegarder les données converties
            file_path = save_dir / Path(f"{_normalized_name(self.short_name)}_{currency}_{filename_sufix}")
            converted_data.to_csv(file_path, float_format="%.4f", index=True)
            print(f"L'historique de {self.ticker} a été converti en {currency} et enregistré sous {file_path}.")
        else:
            print(f"Conversion non effectuée pour {self.ticker}.")

    def download_history(self, end_date: str, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX, interval: str='1d', db_manager: DatabaseManager=None) -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)

        last_detention_date = self._get_last_detention_date(end_date)

        data = self._get_history(end_date=last_detention_date,
                                 save_dir=save_dir,
                                 filename_sufix=filename_sufix,
                                 interval=interval)
        if db_manager != None:
            data.reset_index(inplace=True)  # Réinitialiser l'index pour convertir la date en une colonne normale
            # Convertir la colonne Date en format texte 'YYYY-MM-DD'
            data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
            # Remplacer les valeurs "null" ou NaN dans la colonne 'Open'
            data['Open'] = data['Open'].replace('null', pd.NA)
            data['Open'] = data['Open'].ffill()  # Utiliser la méthode forward fill pour remplacer les NaN
            # Remplacer les valeurs "null" ou NaN dans la colonne 'Close'
            data['Close'] = data['Close'].replace('null', pd.NA)
            data['Close'] = data['Close'].ffill()  # Utiliser la méthode forward fill pour remplacer les NaN
            # Filtrer les colonnes d'intérêt : 'Date', 'Open', 'Close'
            data_to_insert = data[['Date', 'Open', 'Close']]
            # Convertir le DataFrame en une liste de tuples
            records = data_to_insert.to_records(index=False)  # Convertit le DataFrame en une liste de tuples sans l'index
            list_of_records = list(records)  # Transformation en une liste pour être utilisé avec SQLite
            # Extraire les dates
            unique_dates = list(set([date for date, _, _ in list_of_records]))
            # Étape 1 : Insérer toutes les dates en une seule fois
            db_manager.insert_dates_batch(unique_dates)
            # Étape 2 : Récupérer les IDs des dates
            date_ids = db_manager.get_dates_ids(unique_dates)
            db_manager.insert_prices_batch(db_manager.get_asset_id_by_ticker(self.ticker), date_ids, list_of_records)

        if not data.empty :
            # TODO: get wallet currency instead
            if self.currency != "EUR":
                self._convert_history("EUR",
                                      data,
                                      last_detention_date,
                                      save_dir,
                                      filename_sufix,
                                      interval,
                                      db_manager)

    # def load_history(self, db_manager: DatabaseManager, asset_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    #     db_manager.execute_query("""SELECT * FROM Prices (asset_id, date, quantity, price)
    #         VALUES (?, ?, ?, ?)
    #         """, (db_manager.get_asset_id_by_ticker(self.ticker), order.date, order.quantity, order.price))

    def load_history(self, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX) -> None:
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



# class Assets:
#     def __init__(self, list_of_assets: List[Asset]=None) -> None:
#         self.assets = [] if list_of_assets is None else list_of_assets
#         self.db_manager = DatabaseManager()

#     def add_asset(self, asset: Asset) -> None:
#         self.assets.append(asset)
#         self.db_manager.insert_one_asset(asset.short_name, asset.name, asset.ticker, asset.broker, asset.currency)

#     def remove_asset(self, asset_to_remove: Asset) -> None:
#         """
#         Supprime un asset de la liste des assets.

#         :param asset_to_remove: L'asset à supprimer
#         """
#         self.assets = [asset for asset in self.assets if asset != asset_to_remove]

#     def to_dict(self) -> Dict:
#         return {
#             "assets": [asset.to_dict() for asset in self.assets]
#         }

#     def download_histories(self, end_date: str, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX, interval: str='1d') -> None:
#         # Path.mkdir(save_dir, parents=True, exist_ok=True)

#         for asset in self.assets:
#             asset.download_history(end_date,
#                                    save_dir,
#                                    filename_sufix,
#                                    interval)

#     def load_histories(self, save_dir: str, filename_sufix: str=HISTORY_FILENAME_SUFIX) -> None:
#         for asset in self.assets:
#             asset.load_history(save_dir, filename_sufix)

#     def get_dates(self) -> List:  # OK !
#         dates_temp = set()
#         for asset in self.assets:
#             if asset.dates == []:
#                 print('ERROR: asset.dates must have been defined. Please call load_histories().')
#                 # return 1
#             dates_temp.update(asset.dates)
#         return list(sorted(dates_temp))


def _normalized_name(name: str) -> str:
    return name.replace(' ', '_')\
        .replace('-', '_')\
        .replace('.', '_')


def rebuild_assets_structure(assets_data) -> List[Asset]:
        """Recréer les objets à partir des données lues depuis le fichier JSON"""
        # TODO : Ajouter dates et closes ???
        return [Asset(
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
        ) for asset_data in assets_data["assets"]]


def is_valid_date(date_str: str) -> bool:
    try:
        # Tenter de convertir la chaîne en objet datetime selon le format AAAA-MM-DD
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        # Si une erreur est levée, la date n'est pas valide
        return False


def load_assets_json_file(assets_jsonfile: Path) -> List[Asset]:
    """Charge les actifs depuis le fichier JSON
    et reconstruit l'arboressence en respectant les classes de chaque objet"""
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets_data = json.load(asset_file)

    return rebuild_assets_structure(assets_data)


# def write_assets_json_file(assets: Assets, assets_jsonfile: Path):
#     with open(assets_jsonfile, 'w', encoding='utf-8') as asset_file:
#         json.dump(assets.to_dict(), asset_file, indent=4)


def find_asset_by_ticker(list_of_assets: List[Asset], new_asset: Asset) -> Tuple[bool, Asset]:
    """Rechercher un actif dans assets avec son ticker"""
    for asset in list_of_assets:
        if asset.ticker == new_asset.ticker:
            return True, asset
    return False, new_asset

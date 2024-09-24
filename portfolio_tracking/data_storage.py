from collections import defaultdict
from datetime import datetime
from pathlib import Path
import sqlite3
import pandas as pd
from typing import Dict, List, Tuple

import utils


HISTORIES_DIR_PATH = Path(__file__).parent.absolute() / "histories"
ARCHIVES_DIR_NAME = "Archives"
ASSETS_JSON_FILENAME = "assets_real.json"
HISTORY_FILENAME_SUFIX = 'history.csv'
COLUMNS_ORDER = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def get_first_date_from_csv(file_path: Path) -> str:
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

        if not utils.is_valid_date(first_date):
            raise ValueError(f"La première date '{first_date}' dans le fichier '{file_path}' n'est pas valide.")

    except pd.errors.EmptyDataError as e:
        raise ValueError(
            f"Le fichier '{file_path}' est vide ou ne contient aucune date valide."
        ) from e

    return first_date


def get_last_date_from_csv(file_path: Path) -> str:
    """
    Récupère la dernière date du fichier CSV.
    """

    try:
        last_row = pd.read_csv(file_path, usecols=["Date"]).tail(1)
        if last_row.empty:
            raise ValueError(f"Le fichier '{file_path}' est vide ou ne contient aucune date valide.")

        last_date = str(last_row['Date'].values[0])  # Get the last date

        if not utils.is_valid_date(last_date):
            raise ValueError(f"La dernière date '{last_date}' dans le fichier '{file_path}' n'est pas valide.")

    except pd.errors.EmptyDataError as e:
        raise ValueError(
            f"Le fichier '{file_path}' est vide ou ne contient aucune date valide."
        ) from e

    return last_date


# class DataStorage:
#     def __init__(self):
#         pass

#     def save_data(self, asset: Asset, df: pd.DataFrame) -> None:
#         file_name = Path(f"{utils.normalize_name(asset.short_name)}_{asset.currency}_{HISTORY_FILENAME_SUFIX}")
#         file_path = HISTORIES_DIR_PATH / file_name
#         df.to_csv(file_path, float_format="%.4f", index=True)
#         print(f'Le fichier CSV a été sauvegardé avec succès sous {file_path}')

#     def load_data(self, filename: str) -> pd.DataFrame:
#         """
#         Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
#         """
#         filepath = HISTORIES_DIR_PATH / filename
#         try:
#             # Lire le fichier CSV en utilisant pandas
#             return pd.read_csv(filepath, parse_dates=["Date"])

#         except FileNotFoundError:
#             print(f"Le fichier {filename} n'a pas été trouvé dans le répertoire {self.directory}.")

#     def data_already_exist(self, asset: Asset) -> bool:
#         file_name = Path(f"{utils.normalize_name(asset.short_name)}_{asset.currency}_{HISTORY_FILENAME_SUFIX}")
#         file_path = HISTORIES_DIR_PATH / file_name
#         if file_path.is_file() :
#             return True
#         else :
#             return False


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

    def execute_many_query(self, query: str, params: List=None):    # TODO: vérifier si ça fonctionne bien !
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
        query = """
        SELECT a.id, a.short_name, SUM(o.quantity) as total_quantity
        FROM Orders o
        JOIN Assets a ON o.asset_id = a.id
        WHERE o.date BETWEEN ? AND ?  -- Récupère les ordres entre les deux dates
        GROUP BY o.asset_id
        ORDER BY a.short_name ASC;
        """

        # Exécuter la requête pour récupérer les actifs dans la plage de dates
        cursor = self.execute_query(query, (start_date, end_date))
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

    def insert_one_order(self, ticker: str, date: str, quantity: float, price: float) -> None:
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
        which represents the start date of the wallet.
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

    def get_first_date_of_asset(self, asset_id: int) -> str:
        """
        Retrieves the earliest transaction date from the orders.
        This method queries the database to find the minimum date from the Orders table,
        which represents the start date of the wallet.
        If no transactions are found, it returns the current date in 'YYYY-MM-DD' format.
        Args:
            self: The instance of the class.
        Returns:
            str: The earliest transaction date or the current date if no transactions exist.
        """
        # Ici, on suppose que la première transaction représente le début du portefeuille
        query = """SELECT MIN(date) FROM Orders WHERE asset_id = ?;"""
        cursor = self.execute_query(query, (asset_id,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result[0] is not None else datetime.now().strftime('%Y-%m-%d')

    def _get_soldout_date(self, asset_id: int) -> str:
        query = """
            SELECT MAX(o.date)
            FROM Orders o
            WHERE o.asset_id = ?
        """
        cursor = self.execute_query(query, (asset_id,))
        return cursor.fetchone()

    def get_last_detention_date(self, asset_id: int, date: str) -> str:
        """
        Retrieves the last detention date,
        i.e the late date coresponding to the laste day we held that asset,
        or today (meaning that we stil hold the asset).
        Args:
            asset_id (int): The id of the asset.
        Returns:
            str: The last known date at wich we were stil holding the asset.
        """
        """
        Retrieves the last detention date of a specified asset.
        Args:
            asset_id (int): The unique identifier of the asset.

        Returns:
            str: The last detention date in 'YYYY-MM-DD' format.
        """

        query = """
            SELECT SUM(o.quantity)
            FROM Orders o
            WHERE o.asset_id = ?
        """
        cursor = self.execute_query(query, (asset_id,))
        total_quantity  = cursor.fetchone()
        cursor.close()

        return self._get_soldout_date(asset_id)[0] if total_quantity[0] == 0 else date

    def close(self):
        self.conn.close()

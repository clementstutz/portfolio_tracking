from pathlib import Path
import pandas as pd

from portfolio_tracking.portfolio_management import Asset
from portfolio_tracking.utils import normalize_name


STOCKS_HISTORIES_DIR = Path(__file__).parent.absolute() / "histories"
ASSETS_JSON_FILENAME = "assets_real.json"
ARCHIVES_FILENAME = "Archives"
FILENAME_SUFIX = 'history.csv'
COLUMNS_ORDER = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


class DataStorage:
    def __init__(self):
        self.directory = STOCKS_HISTORIES_DIR

    def save_data(self, data: pd.DataFrame, filename: str) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        filepath = self.directory / filename
        data.to_csv(filepath, float_format="%.4f", index=True)

    def load_data(self, filename: str) -> pd.DataFrame:
        """
        Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
        """
        filepath = self.directory / filename
        try:
            # Lire le fichier CSV en utilisant pandas
            return pd.read_csv(filepath, parse_dates=["Date"])

        except FileNotFoundError:
            print(f"Le fichier {filename} n'a pas été trouvé dans le répertoire {self.directory}.")

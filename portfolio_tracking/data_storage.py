from pathlib import Path
import pandas as pd

from portfolio_tracking.class_asset import Asset
from portfolio_tracking.utils import normalize_name


HISTORIES_DIR_PATH = Path(__file__).parent.absolute() / "histories"
ARCHIVES_DIR_NAME = "Archives"
ASSETS_JSON_FILENAME = "assets_real.json"
HISTORY_FILENAME_SUFIX = 'history.csv'
COLUMNS_ORDER = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


class DataStorage:
    def __init__(self):
        pass

    def save_data(self, asset: Asset, df: pd.DataFrame) -> None:
        file_name = Path(f"{normalize_name(asset.short_name)}_{asset.currency}_{HISTORY_FILENAME_SUFIX}")
        file_path = HISTORIES_DIR_PATH / file_name
        df.to_csv(file_path, float_format="%.4f", index=True)
        print(f'Le fichier CSV a été sauvegardé avec succès sous {file_path}')

    def load_data(self, filename: str) -> pd.DataFrame:
        """
        Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
        """
        filepath = HISTORIES_DIR_PATH / filename
        try:
            # Lire le fichier CSV en utilisant pandas
            return pd.read_csv(filepath, parse_dates=["Date"])

        except FileNotFoundError:
            print(f"Le fichier {filename} n'a pas été trouvé dans le répertoire {self.directory}.")

    def data_already_exist(self, asset: Asset) -> bool:
        file_name = Path(f"{normalize_name(asset.short_name)}_{asset.currency}_{HISTORY_FILENAME_SUFIX}")
        file_path = HISTORIES_DIR_PATH / file_name
        if file_path.is_file() :
            return True
        else :
            return False

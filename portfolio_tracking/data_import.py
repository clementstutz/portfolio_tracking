from pathlib import Path
import yfinance as yf
import pandas as pd

from portfolio_tracking import utils
from portfolio_tracking.portfolio_management import Portfolio
from portfolio_tracking.utils import normalize_name
from portfolio_tracking.data_storage import ARCHIVES_FILENAME, COLUMNS_ORDER, FILENAME_SUFIX, STOCKS_HISTORIES_DIR, DataStorage

class DataImport:
    def __init__(self, ticker: str):
        self.ticker = ticker

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
            file_path = archives_dir_path / f"{normalize_name(self.short_name)}_{self.currency}_{filename_sufix}"
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

    def _update_with_new_data(self, portfolio: Portfolio, file_path: Path, last_date: str, end_date: str, interval: str, save_dir, filename_sufix) -> None:
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
        filename = Path(f"{normalize_name(portfolio.short_name)}_{portfolio.currency}_{filename_sufix}")
        data_storage = DataStorage()
        data_storage.save_data(combined_data, filename)
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
        first_date = utils.get_first_date_from_csv(file_path)
        last_date = utils.get_last_date_from_csv(file_path)

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
        file_path = save_dir / Path(f"{normalize_name(self.short_name)}_{self.currency}_{filename_sufix}")
        end_date = self._get_last_detention_date(end_date)

        if not file_path.is_file() :
            # Créer un nouveau fichier avec toutes les données si le fichier n'existe pas
            self._initialize_new_file(file_path, end_date, save_dir, filename_sufix, interval)
            self._update_history(file_path, end_date, save_dir, filename_sufix, interval)
        else :
            self._update_history(file_path, end_date, save_dir, filename_sufix, interval)

        # Charger les données existantes et les renvoie
        return pd.read_csv(file_path, index_col='Date', parse_dates=True)

    def download_history(self, end_date: str, save_dir: Path=STOCKS_HISTORIES_DIR, filename_sufix: str=FILENAME_SUFIX, interval: str='1d') -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)

        data_importer = DataImport(ticker="AAPL")

        last_detention_date = self._get_last_detention_date(end_date)
        self._get_history(last_detention_date,
                          save_dir,
                          filename_sufix,
                          interval)

    def load_history(self, save_dir: Path, filename_sufix: str=FILENAME_SUFIX) -> None:
        """
        Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
        """
        csv_filename = Path(f"{normalize_name(self.short_name)}_{self.currency}_{filename_sufix}")
        file_path = save_dir / csv_filename

        try:
            # Lire le fichier CSV en utilisant pandas

            df = DataStorage.load_data(filename=file_path)

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


class DataUpdater:
    def __init__(self, portfolio: Portfolio, data_storage: DataStorage):
        self.portfolio = portfolio
        self.data_storage = data_storage

    def update_data(self, end_date: str):
        for asset in self.portfolio.assets:
            data_importer = DataImport(ticker=asset.ticker)
            data = data_importer.download_data(start_date=asset.orders[0].date, end_date=end_date)
            self.data_storage.save_data(data, filename=f"{asset.ticker}.csv")
            print(f"Data for {asset.ticker} updated until {end_date}.")
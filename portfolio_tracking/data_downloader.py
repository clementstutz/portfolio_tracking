from pathlib import Path
from typing import List
import pandas as pd
import yfinance as yf

import utils
from data_storage import HISTORIES_DIR_PATH, ARCHIVES_DIR_NAME, HISTORY_FILENAME_SUFIX, COLUMNS_ORDER


class DataDownloader:
    # ok
    def _get_data_from_archives(self, file_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
        # TODO: vérifier que les dates demandes sont bien dans l'archive!
        archived_data = pd.read_csv(file_path, index_col='Date', parse_dates=True)

        # Filtrer les données archivées pour la période demandée
        return archived_data.loc[start_date:end_date]

    # ok
    def _reorgenize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Réordonner les colonnes et les trie par date.
        """
        return df[COLUMNS_ORDER].sort_index()

    # ok
    def _download_history(self, asset_ticker: str, asset_short_name: str, asset_currency: str, start_date: str, end_date: str, interval: str) -> pd.DataFrame:
        """
        Télécharge les données de bourse pour la période donnée.
        """
        # TODO : Why not use yf.Ticker("the_ticker").history() ?
        df: pd.DataFrame = yf.download(tickers=asset_ticker,
                                       start=start_date,
                                       end=end_date,
                                       interval=interval)

        if df.empty:
            # If the download failed, check the Archives directory if there is data for this asset.
            archives_dir_path = HISTORIES_DIR_PATH / ARCHIVES_DIR_NAME
            file_path = archives_dir_path / f"{utils.normalize_name(asset_short_name)}_{asset_currency}_{HISTORY_FILENAME_SUFIX}"
            if file_path.is_file() :
                df = self._get_data_from_archives(file_path=file_path,
                                                  start_date=start_date,
                                                  end_date=end_date)
            else :
                # TODO: Try another method to get the datas!
                # could be done with another API (ex. Investing API (investpy on PyPi))
                # return data
                pass
            # raise ValueError(f"Aucune donnée n'a été téléchargée pour {self.ticker} entre {start_date} et {end_date}")
        return self._reorgenize_data(df)

    # ok
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

    # ok
    def _update_with_old_data(self, asset_ticker: str, asset_short_name: str, asset_currency: str, df: pd.DataFrame, start_date: str, end_date: str, interval: str) -> pd.DataFrame:
        """
        Télécharge et ajoute les données manquantes antérieures à la première date du fichier existant.
        """
        new_df = self._download_history(asset_ticker=asset_ticker,
                                        asset_short_name=asset_short_name,
                                        asset_currency=asset_currency,
                                        start_date=start_date,
                                        end_date=pd.to_datetime(end_date).strftime('%Y-%m-%d'),
                                        interval=interval)
        return self._concat_data(first_dataframe=new_df, second_dataframe=df)

    # ok
    def _update_with_new_data(self, asset_ticker: str, asset_short_name: str, asset_currency: str, df: pd.DataFrame, start_date: str, end_date: str, interval: str) -> pd.DataFrame:
        """
        Télécharge et ajoute les données manquantes postérieures à la dernière date du fichier existant.
        """
        new_df = self._download_history(asset_ticker=asset_ticker,
                                        asset_short_name=asset_short_name,
                                        asset_currency=asset_currency,
                                        start_date=pd.to_datetime(start_date) + pd.Timedelta(days=1),
                                        end_date=end_date,
                                        interval=interval)
        return self._concat_data(first_dataframe=df, second_dataframe=new_df)

    # ok
    def _update_history(self, asset_ticker: str, asset_short_name: str, asset_currency: str, df: pd.DataFrame, start_date: str, end_date: str, interval: str) -> pd.DataFrame:
        # FIXME : Ne fonctionne surement pas avec les jour fériers !
        first_df_date: str = df.index[0] #get_first_date_from_csv(file_path)
        last_df_date: str = df.index[-1] #get_last_date_from_csv(file_path)

        need_older_data: bool = pd.to_datetime(start_date) < pd.to_datetime(first_df_date)
        need_newer_data: bool = pd.to_datetime(last_df_date) + pd.Timedelta(days=2) < pd.to_datetime(end_date)

        if not (need_older_data) and not(need_newer_data):
            print(f"Aucune nouvelle donnée à télécharger pour '{asset_ticker}', les données sont déjà à jour.")
            return df

        # Vérifier si des données plus anciennes doivent être téléchargées
        if need_older_data :
            df = self._update_with_old_data(asset_ticker=asset_ticker,
                                            asset_short_name=asset_short_name,
                                            asset_currency=asset_currency,
                                            df=df,
                                            start_date=start_date,
                                            end_date=first_df_date,
                                            interval=interval)
            print(f"Le Dataframe du ticker '{asset_ticker}' a été mis à jour avec d'anciennes données.")

        # Vérifier si des données plus récentes doivent être téléchargées
        # TODO: change the value "2" of pd.Timedelta(days=2) to "1", but need to manage case of dalayed data
        if need_newer_data:
            df = self._update_with_new_data(asset_ticker=asset_ticker,
                                            asset_short_name=asset_short_name,
                                            asset_currency=asset_currency,
                                            df=df,
                                            start_date=last_df_date,
                                            end_date=end_date,
                                            interval=interval)
            print(f"Le Dataframe du ticker '{asset_ticker}' a été mis à jour avec de nouvelles données.")

        return df

    # ok
    def get_history(self, asset_ticker: str, asset_short_name: str, asset_currency: str, start_date: str, last_detention_date: str, inclusive: bool=True, interval: str='1d') -> pd.DataFrame:
        if inclusive:
            last_detention_date = (pd.to_datetime(last_detention_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        df = self._download_history(asset_ticker=asset_ticker,
                                    asset_short_name=asset_short_name,
                                    asset_currency=asset_currency,
                                    start_date=start_date,
                                    end_date=last_detention_date,
                                    interval=interval)

        df = self._update_history(asset_ticker=asset_ticker,
                                  asset_short_name=asset_short_name,
                                  asset_currency=asset_currency,
                                  df=df,
                                  start_date=start_date,
                                  end_date=last_detention_date,
                                  interval=interval)

        return df


    # def download_histories(self, assets: List[Asset], end_date: str, interval: str='1d') -> pd.DataFrame:
    #     dfs = []
    #     for asset in assets:
    #         dfs.append(
    #             self.get_history(asset=asset,
    #                              end_date=end_date,
    #                              interval=interval))
    #     return dfs


    # def load_history(self, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX) -> None:
    #     """
    #     Charge l'historique des prix de l'action à partir d'un fichier CSV et met à jour les attributs `dates` et `closes`.
    #     """
    #     csv_filename = Path(f"{utils.normalize_name(self.short_name)}_{self.currency}_{filename_sufix}")
    #     file_path = save_dir / csv_filename

    #     try:
    #         # Lire le fichier CSV en utilisant pandas

    #         df = DataStorage.load_data(filename=file_path)

    #         # Remplacer les valeurs "null" ou NaN dans la colonne 'Close'
    #         df['Close'] = df['Close'].replace('null', pd.NA)
    #         df['Close'] = df['Close'].ffill()  # Utiliser la méthode forward fill pour remplacer les NaN

    #         if df['Close'].isna().any():
    #             raise ValueError(f"Le fichier {csv_filename} contient des valeurs 'Close' manquantes ou invalides.")

    #         # Extraire les dates et les prix de clôture
    #         self.add_dates(df['Date'].dt.strftime('%Y-%m-%d').tolist())
    #         self.add_closes(df['Close'].astype(float).tolist())

    #     except FileNotFoundError:
    #         print(f"Le fichier {csv_filename} n'a pas été trouvé dans le répertoire {save_dir}.")
    #     except pd.errors.EmptyDataError:
    #         print(f"Le fichier {csv_filename} est vide ou ne contient aucune donnée valide.")
    #     except ValueError as ve:
    #         print(f"Erreur lors du chargement des données depuis {csv_filename} : {ve}")
    #     except Exception as e:
    #         print(f"Une erreur inattendue est survenue lors du chargement de {csv_filename} : {e}")

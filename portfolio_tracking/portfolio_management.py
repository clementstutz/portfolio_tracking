from datetime import datetime
import json
from typing import List, Dict, Tuple
from pathlib import Path
import pandas as pd

import utils
from data_downloader import DataDownloader
from data_storage import DatabaseManager, COLUMNS_ORDER, HISTORY_FILENAME_SUFIX


DICT_CURRENCY = {"EURUSD": "EURUSD=X",
                 "EURGBP": "EURGBP=X"}
ROUNDING_VALUE = 10     #Should not be less than 5 for accuracy reasons


def _calculate_current_share_value(current_wallet_value: float, cash_flow: float, previous_wallet_value: float, previous_share_value: float) -> float:  # OK !
        return round((current_wallet_value - cash_flow) / previous_wallet_value * previous_share_value, ROUNDING_VALUE)


def _calculate_current_share_value_2(current_wallet_value: float, cash_flow: float, nb_share: float, current_share_value: float) -> Tuple[float, float]:  # OK !
        _nb_share = 0
        _current_wallet_value = 0

        if cash_flow == 0:
            _nb_share = nb_share
        elif nb_share == 0:
            _nb_share = 1
        else:
            _nb_share = current_wallet_value / current_share_value

        if _nb_share == 0:
            _current_wallet_value = 0
        else:
            _current_wallet_value = current_wallet_value / _nb_share

        return round(_current_wallet_value, ROUNDING_VALUE), round(_nb_share, ROUNDING_VALUE)


def _calculate_current_share_value_3(current_wallet_value: float, cash_flow: float, nb_share: float, current_share_value: float) -> Tuple[float, float]:  # OK !
    _nb_share = 0
    _current_wallet_value = 0

    if cash_flow == 0:
        _nb_share = nb_share
    elif nb_share == 0:
        _nb_share = 1
    else:
        _nb_share = current_wallet_value / current_share_value

    if _nb_share == 0:
        _current_wallet_value = 0
    else:
        _current_wallet_value = current_wallet_value / _nb_share

    return round(_current_wallet_value, ROUNDING_VALUE), round(_nb_share, ROUNDING_VALUE)


def _calculate_TWRR_for_sub_period(previous_wallet_value: float, current_wallet_value: float, cash_flow: float) -> float:    # OK ..?
        """
        Calculates the Time-Weighted Rate of Return (TWRR) for a sub-period based on previous and current wallet values and cash flows.
        This function computes the TWRR by evaluating the change in value relative to the previous value adjusted for any cash flows.
        If the sum of the previous value and cash flow is zero, it returns a default value of 1 to avoid division by zero errors.
        https://www.investopedia.com/terms/t/time-weightedror.asp
        TWR=[(1+HP_1)x(1+HP_2)×⋯×(1+HP_n)]-1
        where:
        TWR= Time-weighted return
        n= Number of sub-periods
        HP= (End Value-(Initial Value+Cash Flow)) / (Initial Value+Cash Flow)
        HP_n= Return for sub-period n

        Args:
            previous_value (float): The value of the wallet at the end of the previous period.
            current_value (float): The value of the wallet at the end of the current period.
            cash_flow (float): The cash flow that occurred during the period.

        Returns:
            float: The calculated TWRR for the sub-period.
        """
        if previous_wallet_value + cash_flow == 0:
            print("ERREUR: Division par zero car previous_wallet_value + cash_flow = 0")
            return -1
        if current_wallet_value != 0:
            return round((current_wallet_value - (previous_wallet_value + cash_flow)) / (previous_wallet_value + cash_flow), ROUNDING_VALUE)
        else:
            return 0.0


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

    def __repr__(self, indent=0):
        indentation = " " * indent
        return f"{indentation}{{'date': '{self.date}', 'quantity': {self.quantity}, 'price': {self.price}}}"

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

    def __repr__(self, indent=0):
        indentation = " " * indent
        orders_str = "["
        for order in self.orders:
            orders_str += f"\n{order.__repr__(indent + 4)},"
        orders_str += "]"

        return f"{indentation}{{'short_name': '{self.short_name}', 'name': '{self.name}', 'ticker': '{self.ticker}', 'broker': '{self.broker}', 'currency': '{self.currency}', 'orders': {orders_str}}}"

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

    def add_orders(self, db_manager: DatabaseManager, list_of_orders: List[Order]) -> None:
        for order in list_of_orders:
            db_manager.insert_one_order(self.ticker, order.date, order.quantity, order.price)    #TODO: On peut l'optimiser avec un executmany en utilisant directement la liste
            if order not in self.orders:
                self.orders.append(order)

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

    def _convert_history(self, wallet_currency: str, price_data: pd.DataFrame, end_date: str, save_dir: Path, filename_sufix: str, interval: str, db_manager: DatabaseManager, data_downloader: DataDownloader):
        """
        Télécharge et applique les taux de change si nécessaire pour convertir dans la devise cible.
        """
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
        # Télécharger la paire de devise correspondante
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

        if currency_data is not None:
            # Convertir les prix de l'actif dans la devise cible
            converted_data = self._convert_to_another_currency(price_data, currency_data, currency)

            # Sauvegarder les données converties
            file_path = save_dir / Path(f"{utils.normalize_name(self.short_name)}_{currency}_{filename_sufix}")
            converted_data.to_csv(file_path, float_format="%.4f", index=True)
            print(f"L'historique de {self.ticker} a été converti en {currency} et enregistré sous {file_path}.")
        else:
            print(f"Conversion non effectuée pour {self.ticker}.")

    def download_history(self, end_date: str, save_dir: Path, db_manager: DatabaseManager, data_downloader: DataDownloader, filename_sufix: str=HISTORY_FILENAME_SUFIX, interval: str='1d') -> None:  # TODO: modify to use DataDownloader instead !!
        asset_id = db_manager.get_asset_id_by_ticker(self.ticker)
        last_detention_date = db_manager.get_last_detention_date(asset_id, end_date)
        print(f"asset_id = {asset_id}")
        print(f"last_detention_date = {last_detention_date}")

        data = data_downloader.get_history(asset_ticker=self.ticker,
                                           asset_short_name=self.short_name,
                                           asset_currency=self.currency,
                                           start_date=self.orders[0].date,
                                           last_detention_date=last_detention_date,
                                           interval=interval)
        if db_manager != None:
            self.save_history(data, db_manager)

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

    def save_history(self, data: pd.DataFrame, db_manager: DatabaseManager) -> None:
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
        unique_dates = list(set(date for date, _, _ in list_of_records))
        # Étape 1 : Insérer toutes les dates en une seule fois
        db_manager.insert_dates_batch(unique_dates)
        # Étape 2 : Récupérer les IDs des dates
        date_ids = db_manager.get_dates_ids(unique_dates)
        db_manager.insert_prices_batch(db_manager.get_asset_id_by_ticker(self.ticker), date_ids, list_of_records)


class Wallet:
    def __init__(self, currency: str="EUR") -> None:
        self.currency: str = currency
        self.assets: List[Asset] = []
        self.evaluation_dates: Tuple[str, str] = ()
        self.dates: List[str] = []
        self.valuations: List[float] = []
        self.db_manager = DatabaseManager()
        self.data_downloader = DataDownloader()

    def __repr__(self, indent=0):
        indentation = " " * indent
        assets_str = "["
        for asset in self.assets:
            assets_str += f"\n{asset.__repr__(indent + 4)},"
        assets_str += "]"

        # Représentation finale du portefeuille
        return f"{{'currency': '{self.currency}',\n" \
               f"'assets': {assets_str},\n" \
               f"'evaluation_dates': {self.evaluation_dates},\n" \
               f"'dates': {self.dates}}}"

    def _set_dates(self) -> None:
        self.dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

    def set_evaluation_dates(self, start_date: str = None, end_date: str = None) -> None:
        """
        Sets the evaluation dates for the wallet.
        This method allows to set ena strarting and ending date, that wwill be use later on to calculates valuation or other metrics of the wallet based on these specified dates.
        If start_date is None, it use the earliest date of the wallet as strarting date.
        If end_date is None, it use the current date.

        Args:
            start_date (str, optional): The start date in 'YYYY-MM-DD' format. If None, the earliest date of the wallet is used.
            end_date (str, optional): The end date in 'YYYY-MM-DD' format. If None, the current date is used.

        Returns:
            None
        """
        _start_date, _end_date = utils.check_dates_boundaries(
            start=datetime.strptime(start_date, "%Y-%m-%d").date(),
            end=datetime.strptime(end_date, "%Y-%m-%d").date(),
            lower_bound=datetime.strptime(self.db_manager.get_first_date(), "%Y-%m-%d").date(),
            upper_bound=datetime.now().date())

        self.evaluation_dates = (_start_date.strftime('%Y-%m-%d'), _end_date.strftime('%Y-%m-%d'))
        self._set_dates()

    def add_assets(self, list_of_assets: List[Asset]) -> None:
        for asset in list_of_assets:
            self.db_manager.insert_one_asset(asset.short_name, asset.name, asset.ticker, asset.broker, asset.currency)    #TODO: On peut l'optimiser avec un executmany en utilisant directement la liste
            if asset not in self.assets:
                self.assets.append(asset)
            if asset.orders != None :
                asset.add_orders(self.db_manager, asset.orders)

    def remove_asset(self, ticker: str) -> None:
        self.assets = [asset for asset in self.assets if asset.ticker != ticker]

    def download_histories(self, end_date: str, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX, interval: str='1d') -> None:
        Path.mkdir(save_dir, parents=True, exist_ok=True)
        for asset in self.assets:
            asset.download_history(end_date,
                                   save_dir,
                                   self.db_manager,
                                   self.data_downloader,
                                   filename_sufix,
                                   interval)

    def calculate(self, indicator_name: str, *args):
        return self.indicator_registry.calculate(indicator_name, self, *args)

    def calculate_wallet_valuation(self) -> List[float]:
        """
        Calculates the wallet valuation based on held assets and their prices over the periods defined with set_evaluation_dates().
        This method retrieves the relevant dates, assets held, price data, and quantities to compute the total valuation for each date.
        It returns a list of valuations, which represent the total value of the wallet at each date.

        Args:
            self: The instance of the class.

        Returns:
            List[float]: A list of valuations corresponding to each date in the specified range.
        """
        dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

        # Récupérer les actifs détenus entre les deux dates
        assets_held = self.db_manager.get_assets_held_between_dates(dates[0], dates[-1])
        if not assets_held:
            print(f"ERROR: No assets held between {dates[0]} and {dates[1]}.")
            return [], []

        price_data = self.db_manager.get_all_assets_prices_between_dates(dates[0], dates[-1])
        if not price_data:
            print("ERROR: No price data found for the specified date range.")
            return [], []

        # Retrieve all quantities in a single query
        quantities_data = self.db_manager.get_all_assets_quantities_between_dates(dates[0], dates[-1])

        # Convert price_data to a dictionary for quick access
        price_dict = {(row[0], row[1]): row[2] for row in price_data}
        quantity_dict = {(row[0], row[1]): row[2] for row in quantities_data}

        # Initialiser les listes pour les valorisations et les investissements
        valuations: List[float]= []
        # Boucler sur chaque date de la période
        for date in dates:
            total_valuation = sum(
                price_dict.get((date, asset_id), 0) * quantity_dict.get((date, asset_id), 0) for asset_id, _, _ in assets_held
            )
            # for asset_id, _, _ in assets_held:
            #     print(f"price_dict.get(({date}, {asset_id}), 0) = {price_dict.get((date, asset_id), 0)}")
            #     print(f"quantity_dict.get(({date}, {asset_id}), 0) = {quantity_dict.get((date, asset_id), 0)}")
            # Ajouter la valorisation et l'investissement total pour cette date
            valuations.append(round(total_valuation, ROUNDING_VALUE))
        self.valuations = valuations
        return self.valuations

    def calculate_wallet_share_value(self, init_share_value: float=100) -> List[float]:    # OK !
        """
        Calculates the share value of the wallet based on its valuations and cash flows.
        This method computes the share value by considering your wallet as a fund,
        containing a single share that was initially priced (default = 100), and calculates the value of that share over time.
        It retrieves the necessary dates and cash flows, then calculates the share value for each date based on the wallet's valuations.
        Args:
            init_share_value (float, optional): The initial value of the share for calculation purposes. Defaults to 100.

        Returns:
            List[float]: The list of calculated share values for each date in the specified range.
        """
        if not self.valuations:
            self.calculate_wallet_valuation()

        dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

        # Retrieve all cashflows
        cashflows_data = self.db_manager.get_all_cashflows_between_dates(dates[0], dates[-1])
        # Convert cashflows_data to a dictionary for quick access
        cashflows_dict = {row[0]: row[1] for row in cashflows_data}

        share_value: List[float] = [_calculate_current_share_value(
            current_wallet_value=self.valuations[0],
            cash_flow=0,
            previous_wallet_value=cashflows_dict.get(dates[0]),
            previous_share_value=init_share_value
            )
        ]

        for date_id, date in enumerate(dates[1:], 1):
            # Case where we are disinvested for the first day
            if self.valuations[date_id] == 0:
                share_value.append(0)
            # Case where we have been disinvested for several days
            elif self.valuations[date_id - 1] == 0:
                share_value.append(
                    _calculate_current_share_value(
                        current_wallet_value=self.valuations[date_id],
                        cash_flow=0,
                        previous_wallet_value=cashflows_dict.get(date),
                        previous_share_value=init_share_value,
                    )
                )
            # Case where we are invested
            else:
                share_value.append(
                    _calculate_current_share_value(
                        current_wallet_value=self.valuations[date_id],
                        cash_flow=cashflows_dict.get(date),
                        previous_wallet_value=self.valuations[date_id - 1],
                        previous_share_value=share_value[-1],
                    )
                )
        return share_value

    def calculate_wallet_share_value_2(self, init_nb_share: float=1) -> Tuple[List[float], List[float]]:    # OK !
        """
        Calculates the share value and number of shares for the wallet over a specified date range.
        This method considers your wallet as a fund, containing an initial number of shares,
        and calculates the value of those shares and their number, based on the valuations and cash flows of your wallet.
        It retrieves the necessary dates and cash flows, then calculates the share value and number of shares for each date, returning both as lists.
        Args:
            init_nb_share (float, optional): The initial number of shares for calculation purposes. Defaults to 1.

        Returns:
            Tuple[List[float], List[float]]: A tuple containing:
                - A list of calculated share values for each date.
                - A list of the number of shares corresponding to each date.
        """
        if not self.valuations:
            self.calculate_wallet_valuation()

        dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

        # Retrieve all cashflows
        cashflows_data = self.db_manager.get_all_cashflows_between_dates(dates[0], dates[-1])
        # Convert cashflows_data to a dictionary for quick access
        cashflows_dict = {row[0]: row[1] for row in cashflows_data}

        share_number_2: List[float] = [init_nb_share]
        share_value_2: List[float] = [self.valuations[0] / init_nb_share]

        for date_id, date in enumerate(dates[1:], 1):
            current_share_value, nb_share = _calculate_current_share_value_2(
                current_wallet_value=self.valuations[date_id],
                cash_flow=cashflows_dict.get(date),
                nb_share=share_number_2[-1],
                current_share_value=share_value_2[-1]
                )
            share_value_2.append(current_share_value)
            share_number_2.append(nb_share)
        return share_value_2, share_number_2

    def calculate_wallet_share_value_3(self, init_share_value: float=100) -> Tuple[List[float], List[float]]:    # OK !
        """
        Calculates the share value and number of shares for the wallet over a specified date range.
        This method considers your wallet as a fund, containing an initial number of shares,
        and calculates the value of those shares and their number, based on the valuations and cash flows of your wallet.
        It retrieves the necessary dates and cash flows, then calculates the share value and number of shares for each date, returning both as lists.
        Args:
            init_nb_share (float, optional): The initial number of shares for calculation purposes. Defaults to 1.

        Returns:
            Tuple[List[float], List[float]]: A tuple containing:
                - A list of calculated share values for each date.
                - A list of the number of shares corresponding to each date.
        """
        if not self.valuations:
            self.calculate_wallet_valuation()

        dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

        # Retrieve all cashflows
        cashflows_data = self.db_manager.get_all_cashflows_between_dates(dates[0], dates[-1])
        # Convert cashflows_data to a dictionary for quick access
        cashflows_dict = {row[0]: row[1] for row in cashflows_data}

        share_value_3: List[float] = [self.valuations[0] * init_share_value / cashflows_dict.get(dates[0])]
        share_number_3: List[float] = [cashflows_dict.get(dates[0]) / init_share_value]


        for date_id, date in enumerate(dates[1:], 1):
            current_share_value, nb_share = _calculate_current_share_value_3(
                current_wallet_value=self.valuations[date_id],
                cash_flow=cashflows_dict.get(date),
                nb_share=share_number_3[-1],
                current_share_value=share_value_3[-1]
                )
            share_value_3.append(current_share_value)
            share_number_3.append(nb_share)
        return share_value_3, share_number_3

    def calculate_wallet_TWRR(self, normalized_wallet_value: float=100) -> Tuple[List[float], List[float]]:
        """
        Calculates the Time-Weighted Rate of Return (TWRR) for the wallet.
        This method computes the TWRR based on the wallet's valuations and cash flows over the periods defined with set_evaluation_dates().
        It returns cumulative TWRR values and individual TWRR for each sub-period, allowing for an assessment of the wallet's performance over time.
        Args:
            normalized_wallet_value (float, optional): The initial normalized value of the wallet for TWRR calculation. Defaults to 100.

        Returns:
            Tuple[List[float], List[float]]: A tuple containing:
                - A list of cumulative TWRR values.
                - A list of TWRR for each sub-period.
        """
        if not self.valuations:
            self.calculate_wallet_valuation()

        dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

        # Retrieve all cashflows
        cashflows_data = self.db_manager.get_all_cashflows_between_dates(dates[0], dates[-1])
        # Convert cashflows_data and valuations to a dictionary for quick access
        cashflows_dict = {row[0]: row[1] for row in cashflows_data}
        valuation_dict = dict(zip(dates, self.valuations))

        twrr: List[float] = [_calculate_TWRR_for_sub_period(
            previous_wallet_value=0,
            current_wallet_value=self.valuations[0],
            cash_flow=cashflows_dict.get(dates[0])
            )
        ]
        twrr_cumulated: List[float] = [round(normalized_wallet_value * (1 + twrr[-1]), ROUNDING_VALUE)]

        for date_id, date in enumerate(dates[1:], 1):
            twrr.append(_calculate_TWRR_for_sub_period(
                previous_wallet_value=valuation_dict.get(dates[date_id-1]),
                current_wallet_value=valuation_dict.get(date),
                cash_flow=cashflows_dict.get(date)
                )
            )
            twrr_cumulated.append(round(twrr_cumulated[-1] * (1 + twrr[-1]), ROUNDING_VALUE))

        return twrr_cumulated, twrr

    # def get_wallet_MWRR(start_date: str, end_date: str) -> List:  # TODO : impementer cette fonction
    #     """Calculates the money weighted rates of return"""
    #     """
    #     https://www.investopedia.com/terms/m/money-weighted-return.asp
    #     PVO=PVI=CF0+CF1/(1+IRR)+CF2/(1+IRR)^2+CF3/(1+IRR)^3+...+CFn/(1+IRR)^n
    #     where:
    #     PVO=PV Outflows
    #     PVI=PV Inflows
    #     CF0=Initial cash outlay or investment
    #     CF1,CF2,CF3,...CFn=Cash flows
    #     N=Each period
    #     IRR=Initial rate of return
    #     """
    #     return 0

    # def get_wallet_IRR(start_date: str, end_date: str, cash_flows: List[float]) -> List:  # TODO : impementer cette fonction
    #     """Calculates the IRR"""
    #     return npf.irr(cash_flows) #from numpy_financial...

    # def calculate_irr(cash_flows: List[float]) -> float:
    #     return np.irr(cash_flows)


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


def load_assets_json_file(assets_jsonfile: Path) -> List[Asset]:
    """Charge les actifs depuis le fichier JSON
    et reconstruit l'arboressence en respectant les classes de chaque objet"""
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets_data = json.load(asset_file)

    return rebuild_assets_structure(assets_data)

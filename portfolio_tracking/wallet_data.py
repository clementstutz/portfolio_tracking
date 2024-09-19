from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple
from portfolio_tracking.yfinance_interface import ASSETS_JSON_FILENAME, HISTORIES_DIR_PATH, HISTORY_FILENAME_SUFIX, DatabaseManager, Asset, Order, load_assets_json_file
# import numpy_financial as npf
# import QuantLib as ql

DEBUG = True
ROUNDING_VALUE = 10     #Should not be less than 5 for accuracy reasons


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
        return 1
    return round((current_wallet_value - (previous_wallet_value + cash_flow)) / (previous_wallet_value + cash_flow), ROUNDING_VALUE)


def _calculate_current_share_value(current_wallet_value: float, cash_flow: float, previous_wallet_value: float, previous_share_value: float) -> float:  # OK !
    return round((current_wallet_value - cash_flow) / previous_wallet_value * previous_share_value, ROUNDING_VALUE)


def _current_share_value_2(current_wallet_value: float, cash_flow: float, nb_share: float, current_share_value: float) -> Tuple[float, float]:  # OK !
    if cash_flow != 0:
        nb_share = current_wallet_value / current_share_value
    return round(current_wallet_value / nb_share, ROUNDING_VALUE), round(nb_share, ROUNDING_VALUE)


def _check_dates_boundaries(start: datetime, end: datetime, lower_bound: datetime, upper_bound: datetime) -> Tuple[datetime, datetime]:
    if lower_bound > upper_bound:
        print("WARNING: lower_bound must be lower that upper_bound!")
        lower_bound, upper_bound = upper_bound, lower_bound
    if start > end:
        start, end = end, start
    start = max(start, lower_bound)
    end = min(end, upper_bound)
    return start, end


class Wallet:
    def __init__(self, currency: str="EUR") -> None:
        self.currency = currency
        self.db_manager = DatabaseManager()
        self.assets: List[Asset] = []
        self.evaluation_dates: Tuple[str, str] = ()
        self.dates: List[str] = []
        self.valuations = []

    def _set_dates(self) -> None:
        self.dates = [row[0] for row in self.db_manager.get_dates(self.evaluation_dates[0], self.evaluation_dates[1])]

    def set_evaluation_dates(self, start_date: str = None, end_date: str = None) -> None:
        """
        Sets the evaluation dates for the portfolio.
        This method allows to set ena strarting and ending date, that wwill be use later on to calculates valuation or other metrics of the portfolio based on these specified dates.
        If start_date is None, it use the earliest date of the portfolio as strarting date.
        If end_date is None, it use the current date.

        Args:
            start_date (str, optional): The start date in 'YYYY-MM-DD' format. If None, the earliest date of the portfolio is used.
            end_date (str, optional): The end date in 'YYYY-MM-DD' format. If None, the current date is used.

        Returns:
            None
        """
        _start_date, _end_date = _check_dates_boundaries(
            start=datetime.strptime(start_date, "%Y-%m-%d").date(),
            end=datetime.strptime(end_date, "%Y-%m-%d").date(),
            lower_bound=datetime.strptime(self.db_manager.get_first_date(), "%Y-%m-%d").date(),
            upper_bound=datetime.now().date())

        self.evaluation_dates = (_start_date.strftime('%Y-%m-%d'), _end_date.strftime('%Y-%m-%d'))
        self._set_dates()

    def add_assets(self, list_of_assets: List[Asset]) -> None:
        for asset in list_of_assets:
            self.db_manager.insert_one_asset(asset.short_name, asset.name, asset.ticker, asset.broker, asset.currency)
            if asset not in self.assets:
                self.assets.append(asset)
            if asset.orders != None :
                asset.add_orders(self.db_manager, asset.orders)


    def remove_asset(self, ticker: str) -> None:
        self.assets = [asset for asset in self.assets if asset.ticker != ticker]

    def __repr__(self, indent=0):
        indentation = " " * indent
        assets_str = "["
        for asset in self.assets:
            assets_str += f"\n{asset.__repr__(indent + 4)},"
        assets_str += "]"

        # Représentation finale du portefeuille
        return f"{{'currency': '{self.currency}',\n" \
               f"'assets': {assets_str},\n" \
               f"'evaluation_dates': {self.evaluation_dates}}}"

    def download_histories(self, end_date: str, save_dir: Path, filename_sufix: str=HISTORY_FILENAME_SUFIX, interval: str='1d') -> None:
        # Path.mkdir(save_dir, parents=True, exist_ok=True)
        for asset in self.assets:
            asset.download_history(end_date,
                                   save_dir,
                                   filename_sufix,
                                   interval,
                                   self.db_manager)

    def load_histories(self, save_dir: str, filename_sufix: str=HISTORY_FILENAME_SUFIX) -> None:
        for asset in self.assets:
            asset.load_history(save_dir, filename_sufix)


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

        share_value.extend(
            _calculate_current_share_value(
                current_wallet_value=self.valuations[date_id],
                cash_flow=cashflows_dict.get(date),
                previous_wallet_value=self.valuations[date_id - 1],
                previous_share_value=share_value[-1],
            )
            for date_id, date in enumerate(dates[1:], 1)
        )
        return share_value

    def get_wallet_share_value_2(self, init_nb_share: float=1) -> Tuple[List[float], List[float]]:    # OK !
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

        share_value_2: List[float] = [self.valuations[0] / init_nb_share]
        share_number_2: List[float] = [init_nb_share]

        for date_id, date in enumerate(dates[1:], 1):
            current_share_value, nb_part = _current_share_value_2(
                current_wallet_value=self.valuations[date_id],
                cash_flow=cashflows_dict.get(date),
                nb_share=share_number_2[-1],
                current_share_value=share_value_2[-1]
                )
            share_value_2.append(current_share_value)
            share_number_2.append(nb_part)
        return share_value_2, share_number_2

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

    # def get_wallet_MWRR(self) -> List:  # TODO : impementer cette fonction
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

    # def get_wallet_IRR(self) -> List:  # TODO : impementer cette fonction
    #     """Calculates the IRR"""
    #     # I could use npf.irr(cash_flows) from numpy_financial...
    #     return 0


def main():
    today = datetime.now()
    today_date = today.strftime("%Y-%m-%d")
    end_date = today_date
    save_dir = HISTORIES_DIR_PATH
    filename_sufix = HISTORY_FILENAME_SUFIX
    interval = "1d"

    wallet_1 = Wallet()

    # asset_1 = Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "EUR")
    # asset_2 = Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR")
    # list_of_assets = [asset_1, asset_2]
    list_of_assets = load_assets_json_file(HISTORIES_DIR_PATH / "assets_test.json")
    wallet_1.add_assets(list_of_assets)
    # asset_1.add_orders(wallet_1.db_manager,
    #                    [Order("2024-08-12", 1, 3.75),
    #                     Order("2024-08-13", 1, 3.5),
    #                     Order("2024-08-14", -2, 3.5)])
    # asset_2.add_orders(wallet_1.db_manager,
    #                    [Order("2024-08-20", 1, 28.18)])

    wallet_1.download_histories(end_date, save_dir, filename_sufix, interval)

    wallet_1.set_evaluation_dates("2020-07-09", "2024-09-10")
    valuations = wallet_1.calculate_wallet_valuation()
    if DEBUG : print(f"wallet_1 :\n{wallet_1}")
    if DEBUG : print("valuations =\n", valuations)

    share_value = wallet_1.calculate_wallet_share_value()
    if DEBUG : print(f"share_value =\n{share_value}")
    if DEBUG : print("len(share_value) =\n", len(share_value))

    share_value_2, share_number_2 = wallet_1.get_wallet_share_value_2()
    if DEBUG : print("share_value_2 =\n", share_value_2)
    if DEBUG : print("len(share_value_2) =\n", len(share_value_2))
    if DEBUG : print("share_number_2 =\n", share_number_2)
    if DEBUG : print("len(share_number_2) =\n", len(share_number_2))

    twrr_cumulated, twrr = wallet_1.calculate_wallet_TWRR()
    if DEBUG : print(f"twrr_cumulated =\n{twrr_cumulated}")
    if DEBUG : print("len(twrr_cumulated) =\n", len(twrr_cumulated))
    if DEBUG : print(f"twrr =\n{twrr}")
    if DEBUG : print("len(twrr) =\n", len(twrr))


    # # cash_flows = [-25, 10, 15, 20, 25, 30]
    # # irr = npf.irr(cash_flows)
    # # print("IRR:", irr)



    # # dates = [ql.Date(1, 1, 2022), ql.Date(1, 1, 2023), ql.Date(1, 1, 2024)]
    # # flows = [-100, 50, 40]
    # # npv = 0
    # # guess = 0.1
    # # irr = ql.Irr(flows, npv, guess)
    # # print("IRR:", irr)


if __name__ == '__main__':

    main()
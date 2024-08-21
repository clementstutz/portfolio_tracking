from datetime import date
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple
from portfolio_tracking.yfinance_interface import FILENAME_SUFIX, Asset, Assets, Order
# import numpy_financial as npf
# import QuantLib as ql


DEBUG = True


def _get_new_investement(date: str, asset: Asset, actions_count: Dict) -> float:  # OK !
    for order in asset.orders:
        # TODO : what if order.date is between two consecutive dates ?
        if date == order.date:
            actions_count[asset.short_name] += order.quantity
            return order.price * order.quantity
    return 0.0


def _get_wallet_TWRR(previous_value: float, current_value: float, cash_flow: float) -> float:  # OK ..?
    """Calculates the HP_i term from the equation TWR=[(1+HP_1)x(1+HP_2)×⋯×(1+HP_n)]-1 """
    twrr = (current_value - (previous_value + cash_flow)) / (previous_value + cash_flow)
    if abs(twrr) < 1.e-10:
        return 0.0
    return twrr


def _current_share_value(current_wallet_value: float, net_deposit: float, previous_wallet_value: float, previous_share_value: float) -> float:  # OK !
    return (current_wallet_value - net_deposit) / previous_wallet_value * previous_share_value


def _current_share_value_2(current_wallet_value: float, net_deposit: float, nb_part: float, current_share_value: float) -> float:  # OK !
        if net_deposit == 0:
            return (current_wallet_value) / nb_part, nb_part
        else:
            nb_part = current_wallet_value / current_share_value
            return (current_wallet_value) / nb_part, nb_part


def _check_dates_boundaries(start, end, lower_bound, upper_bound):
    if lower_bound > upper_bound:
        print("WARNING: lower_bound must be lower that upper_bound!")
        temp = lower_bound
        lower_bound = upper_bound
        upper_bound = temp

    if start > end :
        temp = start
        start = end
        end = temp

    if start < lower_bound:
        start = lower_bound

    if end > upper_bound:
        end = upper_bound

    return start, end


class Wallet:
    def __init__(self, assets: Assets, valuation: List=None, currency: str="EUR") -> None:
        self.assets = assets.assets
        self.currency = currency
        self.dates = assets.get_dates()
        self.valuations = [] if valuation is None else valuation
        self.investments = []  # TODO: rename it into cash_flow

    def add_dates(self, dates: List) -> None:
        self.dates.extend(dates)

    def add_valuation(self, valuation: List) -> None:
        self.valuations.extend(valuation)

    def to_dict(self) -> Dict:
        return {
            "assets": [asset.to_dict() for asset in self.assets],  # TODO : change for asset insted ?
            "currency": self.currency,
            "dates": self.dates,
            "valuations": self.valuations,
            "investments": self.investments,
        }


    def _get_close_price_of_a_day(self, date: str, asset: Asset) -> float:  # OK !
        if date in asset.dates:
            index = asset.dates.index(date)
            return asset.closes[index]

        previous_index = self.dates.index(date) -1
        while previous_index > 0 :
            previous_date = self.dates[previous_index]
            if previous_date in asset.dates:
                index = asset.dates.index(previous_date)
                return asset.closes[index]
            else :
                previous_index -= 1

        print('ERROR: try to get the price for a date before your first order for this asset.')
        return 1

    def get_wallet_valuation(self) -> Tuple[List, List]:  # OK !
        """Calculates the valorisation of your wallet over time"""
        if self.dates == []:
            print('ERROR: wallet.dates must have been defined. Please call yfinance_interface.get_dates().')
            return 1
        actions_count = {asset.short_name: 0 for asset in self.assets}  # Initialisez un dictionnaire pour suivre le nombre d'actions de chaque asset
        investment = 0
        for date in self.dates:  # TODO : voir si ce n'est pas mieux de boucler sur les assets plutôt que sur les dates.
            total_valuation = 0  # Initialisez la valeur totale du portefeuille à 0
            for asset in self.assets:
                if (asset.dates[0] <= date <= asset.dates[-1]) or (actions_count[asset.short_name] != 0):
                    close_price = self._get_close_price_of_a_day(date, asset)
                    investment += _get_new_investement(date, asset, actions_count)
                    total_valuation += close_price * actions_count[asset.short_name]
                else:
                    pass

            self.valuations.append(total_valuation)
            self.investments.append(investment)
        return self.valuations, self.investments

    def get_wallet_share_value(self, start_date, end_date, init_share_value: float=100) -> List:  # OK !
        """Considers your wallet as a fund containing only one share that was initially priced,
        and calculates the value of that share."""
        if self.valuations == []:
            self.get_wallet_valuation()

        share_value = [init_share_value]
        actions_count = {asset.short_name: asset.orders[0].quantity for asset in self.assets}

        start_date, end_date = _check_dates_boundaries(start_date, end_date, self.dates[1], self.dates[-1])
        dates = [date for date in self.dates[self.dates.index(start_date):self.dates.index(end_date)+1]]
        for date in dates:
            index = self.dates.index(date)
            new_value_invested = 0
            for asset in self.assets:
                if asset.dates[0] <= date <= asset.dates[-1]:
                    new_value_invested += _get_new_investement(date, asset, actions_count)
                else:
                    pass
            current_wallet_value = self.valuations[index]
            net_deposit = new_value_invested
            previous_wallet_value = self.valuations[index-1]
            previous_share_value = share_value[-1]
            current_share_value = _current_share_value(current_wallet_value,
                                                       net_deposit,
                                                       previous_wallet_value,
                                                       previous_share_value)
            share_value.append(current_share_value)
        return share_value

    def get_wallet_share_value_2(self, start_date, end_date, init_nb_part: float=1) -> Tuple[List, List]:  # OK !
        """Considers your wallet as a fund containing an initial number of shares,
        and calculates the value of those shares and their number."""
        if self.valuations == []:
            self.get_wallet_valuation()

        share_value_2 = []
        share_number_2 = []
        share_value_2.append(self.valuations[0] / init_nb_part)
        share_number_2.append(init_nb_part)

        start_date, end_date = _check_dates_boundaries(start_date, end_date, self.dates[1], self.dates[-1])
        dates = [date for date in self.dates[self.dates.index(start_date):self.dates.index(end_date)+1]]
        for date in dates:
            index = self.dates.index(date)
            net_deposit = self.investments[index] - self.investments[index-1]
            current_share_value, nb_part = _current_share_value_2(self.valuations[index],
                                                                  net_deposit,
                                                                  share_number_2[-1],
                                                                  share_value_2[-1])
            share_value_2.append(current_share_value)
            share_number_2.append(nb_part)
        return share_value_2, share_number_2

    def get_wallet_TWRR(self, start_date, end_date, normalized_wallet_value: float=100) -> Tuple[List, List, List]:
        """Calculates the time weighted rates of return"""
        """
        https://www.investopedia.com/terms/t/time-weightedror.asp
        TWR=[(1+HP_1)x(1+HP_2)×⋯×(1+HP_n)]-1
        where:
        TWR= Time-weighted return
        n= Number of sub-periods
        HP= (End Value-(Initial Value+Cash Flow)) / (Initial Value+Cash Flow)
        HP_n= Return for sub-period n
        """
        if self.valuations == []:
            self.get_wallet_valuation()

        twrr_cumulated = []
        twrr = []
        # Initialisation
        twrr.append(_get_wallet_TWRR(self.investments[0], self.valuations[0], 0))
        twrr_cumulated.append(normalized_wallet_value * (1 + twrr[-1]))

        start_date, end_date = _check_dates_boundaries(start_date, end_date, self.dates[0], self.dates[-1])
        dates = [date for date in self.dates[self.dates.index(start_date):self.dates.index(end_date)+1]]
        for date in dates[1:]:  # TODO : voir si ce n'est pas mieux de boucler sur les assets plutôt que sur les dates.
            index = self.dates.index(date)
            twrr.append(
                _get_wallet_TWRR(
                    self.valuations[index-1],
                    self.valuations[index],
                    (self.investments[index] - self.investments[index-1])
                )
            )
            twrr_cumulated.append(twrr_cumulated[-1] * (1 + twrr[-1]))
        return twrr_cumulated, dates, twrr

    def get_wallet_MWRR(self, start_date, end_date) -> List:  # TODO : impementer cette fonction
        """Calculates the money weighted rates of return"""
        """
        https://www.investopedia.com/terms/m/money-weighted-return.asp
        PVO=PVI=CF0+CF1/(1+IRR)+CF2/(1+IRR)^2+CF3/(1+IRR)^3+...+CFn/(1+IRR)^n
        where:
        PVO=PV Outflows
        PVI=PV Inflows
        CF0=Initial cash outlay or investment
        CF1,CF2,CF3,...CFn=Cash flows
        N=Each period
        IRR=Initial rate of return
        """
        return 0

    def get_wallet_IRR(self, start_date, end_date) -> List:  # TODO : impementer cette fonction
        """Calculates the IRR"""
        # I could use npf.irr(cash_flows) from numpy_financial...
        return 0


if __name__ == '__main__':

    order_1 = Order("2024-08-12", 1, 3.75)
    order_2 = Order("2024-08-15", 1, 3.5)
    order_3 = Order("2024-08-15", 1, 28.18)

    asset_1 = Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "USD", [order_1, order_2])
    asset_2 = Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR", [order_3])

    assets_1 = Assets([asset_1, asset_2])

    today = date.today()
    end_date = today.strftime("%Y-%m-%d")
    save_dir = Path(__file__).parent.absolute() / "histories"
    filename_sufix = FILENAME_SUFIX
    interval = "1d"
    assets_1.download_histories(end_date, save_dir, filename_sufix, interval)
    assets_1.load_histories(save_dir, filename_sufix)

    wallet_1 = Wallet(assets_1)
    # if DEBUG : print("wallet_1 =\n", wallet_1.to_dict())
    # if DEBUG : print("wallet_1.dates =\n", wallet_1.dates)
    # if DEBUG : print("len(dates) =\n", len(wallet_1.dates))


    wallet_1.get_wallet_valuation()
    if DEBUG : print("wallet_1.valuations =\n", wallet_1.valuations)
    if DEBUG : print("wallet_1.investments =\n", wallet_1.investments)

    twrr_cumulated, dates, twrr = wallet_1.get_wallet_TWRR(wallet_1.dates[0], wallet_1.dates[-1])
    if DEBUG : print("twrr_cumulated =\n", twrr_cumulated)
    if DEBUG : print("len(twrr_cumulated) =\n", len(twrr_cumulated))
    if DEBUG : print("twrr =\n", twrr)
    if DEBUG : print("len(twrr) =\n", len(twrr))

    # share_value = wallet_1.get_wallet_share_value(wallet_1.dates[0], wallet_1.dates[-1])
    # if DEBUG : print("share_value =\n", share_value)
    # if DEBUG : print("len(share_value) =\n", len(share_value))

    # share_value_2, share_number_2 = wallet_1.get_wallet_share_value_2(wallet_1.dates[0], wallet_1.dates[-1])
    # if DEBUG : print("share_value_2 =\n", share_value_2)
    # if DEBUG : print("len(share_value_2) =\n", len(share_value_2))
    # if DEBUG : print("share_number_2 =\n", share_number_2)
    # if DEBUG : print("len(share_number_2) =\n", len(share_number_2))


    # # cash_flows = [-25, 10, 15, 20, 25, 30]
    # # irr = npf.irr(cash_flows)
    # # print("IRR:", irr)



    # # dates = [ql.Date(1, 1, 2022), ql.Date(1, 1, 2023), ql.Date(1, 1, 2024)]
    # # flows = [-100, 50, 40]
    # # npv = 0
    # # guess = 0.1
    # # irr = ql.Irr(flows, npv, guess)
    # # print("IRR:", irr)
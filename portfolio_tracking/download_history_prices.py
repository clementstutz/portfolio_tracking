from datetime import date
from pathlib import Path
from typing import Dict, List
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
    return 0


def _get_wallet_time_weighted_rates_of_return(previous_value: float, current_value: float, cash_flow: float) -> float:  # OK ..?
    """
    https://www.investopedia.com/terms/t/time-weightedror.asp
    TWR=[(1+HP1)x(1+HP2)×⋯×(1+HPn)]-1
    where:
    TWR= Time-weighted return
    n= Number of sub-periods
    HP= (End Value-(Initial Value+Cash Flow)) / (Initial Value+Cash Flow)
    HPn= Return for sub-period n
    """
    # if DEBUG : print("_previous_value =", previous_value)
    # if DEBUG : print("_current_value =", current_value)
    # if DEBUG : print("_cash_flow =", cash_flow)
    twrr = (current_value - (previous_value + cash_flow)) / (previous_value + cash_flow)
    # if DEBUG : print("_twrr =", twrr)
    if abs(twrr) < 1.e-10:
        return 0
    return twrr


def _current_share_value(current_wallet_value: float, net_deposit: float, previous_wallet_value: float, previous_share_value: float) -> float:  # OK !
    return (current_wallet_value - net_deposit) / previous_wallet_value * previous_share_value


def _current_share_value_2(current_wallet_value: float, net_deposit: float, nb_part: float, current_share_value: float) -> float:  # OK !
        if net_deposit == 0:
            return (current_wallet_value) / nb_part, nb_part
        else:
            nb_part = current_wallet_value / current_share_value
            return (current_wallet_value) / nb_part, nb_part


class Wallet:
    def __init__(self, assets: Assets, valuation: List=None, devise: str="EUR") -> None:
        self.assets = assets.assets
        self.dates = assets.get_dates()
        self.valuation = [] if valuation is None else valuation
        self.devise = devise
        self.investment = []
        self.twrr = []
        self.twrr_cumulated = []
        self.share_value = []
        self.share_value_2 = []
        self.share_number_2 = []
    
    def add_dates(self, dates: List) -> None:
        self.dates.extend(dates)
    
    def add_valuation(self, valuation: List) -> None:
        self.valuation.extend(valuation)

    def to_dict(self) -> Dict:
        return {
            "assets": [asset.to_dict() for asset in self.assets],  # TODO : change for asset insted ?
            "dates": self.dates,
            "valuation": self.valuation,
            "devise": self.devise,
            "investment": self.investment,
            "twrr": self.twrr,
            "twrr_cumulated": self.twrr_cumulated,
            "share_value": self.share_value,
            "share_value_2": self.share_value_2,
            "share_number_2": self.share_number_2,
        }


    def _get_close_price_of_a_day(self, date: str, asset: Asset) -> float:  # OK !
        if date in asset.dates:
            date_index = asset.dates.index(date)
            return asset.closes[date_index]

        date_index = self.dates.index(date) -1
        while date_index > 0 :
            previous_date = self.dates[date_index]
            if previous_date in asset.dates:
                previous_date_index = asset.dates.index(previous_date)
                return asset.closes[previous_date_index]
            else :
                date_index -= 1
        print('ERROR: try to get the price for a date before your first order for this asset.')
        return 1

    def get_wallet_valuation(self, ref_wallet_value: float=100) -> None:  # OK !
        """Return the wallet with its valorisation over time"""
        if self.dates == []:
            print('ERROR: wallet.dates must have been defined. Please call yfinance_interface.get_dates().')
            return 1
        actions_count = {asset.short_name: 0 for asset in self.assets}  # Initialisez un dictionnaire pour suivre le nombre d'actions de chaque asset
        self.twrr_cumulated.append(ref_wallet_value)
        investment = 0
        for date in self.dates:  # TODO : voir si ce n'est pas mieux de boucler sur les assets plutôt que sur les dates.
            total_valuation = 0  # Initialisez la valeur totale du portefeuille à 0
            for asset in self.assets:
                if asset.dates[0] <= date <= asset.dates[-1]:
                    # TODO : Ici, à cause du (date <= asset.dates[-1]) si on n'a pas récupéré les données
                    # pour le dernier jour de cotation pour un asset donné,
                    # alors il ne sera pas comtabilisé dans le portefeuille.
                    # Or il le faut, il n'a pas été vendu, il manque juste la dernière cotation.
                    # Il faut donc ajouter un test pour savoir si la dernière cotation est manquante car
                    # on n'a pas réussi à la récupérer ou si c'est parce qu'on à vendu cet asset la veuille.
                    close_price = self._get_close_price_of_a_day(date, asset)
                    investment += _get_new_investement(date, asset, actions_count)
                    total_valuation += close_price * actions_count[asset.short_name]
                else:
                    pass
            
            self.valuation.append(total_valuation)
            self.investment.append(investment)

            if date != self.dates[0]:
                self.twrr.append(
                    _get_wallet_time_weighted_rates_of_return(
                        self.valuation[-2],
                        self.valuation[-1],
                        (self.investment[-1] - self.investment[-2])
                    )
                )
                self.twrr_cumulated.append(self.twrr_cumulated[-1] * (1 + self.twrr[-1]))

    def get_wallet_share_value(self, ref_wallet_value: float=100) -> None:  # OK !
        if self.valuation == []:
            print('ERROR: self.valuation must have been calculated. Please call get_wallet_valuation().')
            return 1
        
        actions_count = {asset.short_name: 0 for asset in self.assets}

        self.share_value.append(ref_wallet_value)
        current_share_value = self.share_value[0]
        for asset in self.assets:
            _get_new_investement(self.dates[0], asset, actions_count)

        for date in self.dates[1:]:
            new_value_invested = 0
            for asset in self.assets:
                if asset.dates[0] <= date <= asset.dates[-1]:
                    new_investement = _get_new_investement(date, asset, actions_count)
                    new_value_invested += new_investement
                else:
                    pass
            current_wallet_value = self.valuation[self.dates.index(date)]
            net_deposit = new_value_invested
            previous_wallet_value = self.valuation[self.dates.index(date)-1]
            previous_share_value = current_share_value
            current_share_value = _current_share_value(current_wallet_value, net_deposit, previous_wallet_value, previous_share_value)
            self.share_value.append(current_share_value)

    def get_wallet_share_value_2(self, nb_part: float=1) -> None:  # OK !
        if self.valuation == []:
            print('ERROR: self.valuation must have been calculated. Please call get_wallet_valuation().')
            return 1
        
        current_share_value = self.valuation[0] / nb_part
        self.share_value_2.append(current_share_value)
        self.share_number_2.append(nb_part)

        for date in self.dates[1:]:
            current_wallet_value = self.valuation[self.dates.index(date)]
            net_deposit = self.investment[self.dates.index(date)] - self.investment[self.dates.index(date)-1]
            current_share_value, nb_part = _current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value)
            self.share_value_2.append(current_share_value)
            self.share_number_2.append(nb_part)

    def get_wallet_money_weighted_rates_of_return(self) -> None:
        # TODO : impementer cette fonction
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
    
    def get_wallet_time_weighted_rates_of_return(self) -> None:
        # TODO : impementer cette fonction
        return 0

    def get_wallet_TRI(self) -> None:
        # TODO : impementer cette fonction
        self['TRI'] = []



if __name__ == '__main__':

    order_1 = Order("2024-01-22", 1, 3.75)
    order_2 = Order("2024-02-01", 1, 3.5)
    order_3 = Order("2024-01-22", 1, 28.18)

    asset_1 = Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "EUR", [order_1, order_2])
    asset_2 = Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR", [order_3])

    assets_1 = Assets([asset_1, asset_2])
    assets_2 = Assets([asset_1, asset_2])

    today = date.today()
    end_date = today.strftime("%Y-%m-%d")
    save_dir = Path(__file__).parent.absolute() / "histories"
    filename_sufix = FILENAME_SUFIX
    interval = "1d"
    assets_1.download_histories(end_date, save_dir, filename_sufix, interval)

    assets_1.load_histories(save_dir, filename_sufix)
    assets_2.load_histories(save_dir, filename_sufix)
    
    dates = assets_1.get_dates()
    if DEBUG : print("dates =\n", dates)

    wallet_1 = Wallet(assets_1)
    wallet_2 = Wallet(assets_2)
    if DEBUG : print("wallet_1 =\n", wallet_1)
    if DEBUG : print("wallet_1.dates =\n", wallet_1.dates)

    wallet_1.get_wallet_valuation()
    if DEBUG : print("wallet_1 =\n", wallet_1.to_dict())
    if DEBUG : print("wallet_1.valuation      = ", wallet_1.valuation)
    if DEBUG : print("wallet_1.twrr_cumulated = ", wallet_1.twrr_cumulated)

    wallet_2.get_wallet_valuation()
    wallet_2.get_wallet_share_value()

    if DEBUG : print("wallet_2.valuation      = ", wallet_2.valuation)
    if DEBUG : print("wallet_2.twrr_cumulated = ", wallet_2.twrr_cumulated)
    if DEBUG : print("wallet_2.share_value    = ", wallet_2.share_value)
    
    wallet_2.get_wallet_share_value_2()
    if DEBUG : print("wallet_2.share_value_2  = ", wallet_2.share_value_2)
    print("assets_2 =\n", assets_2.to_dict())
    print("wallet_2 =\n", wallet_2.to_dict())


    # cash_flows = [-100, 50, 40, 30, 20]
    # irr = npf.irr(cash_flows)
    # print("IRR:", irr)

    # dates = [ql.Date(1, 1, 2022), ql.Date(1, 1, 2023), ql.Date(1, 1, 2024)]
    # flows = [-100, 50, 40]
    # npv = 0
    # guess = 0.1
    # irr = ql.Irr(flows, npv, guess)
    # print("IRR:", irr)
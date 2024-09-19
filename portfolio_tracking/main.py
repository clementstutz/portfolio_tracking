from datetime import date
from pathlib import Path
from typing import List

from portfolio_tracking.class_order import Order
from portfolio_tracking.class_asset import Asset
from portfolio_tracking.data_import import DataImport, DataUpdater
from portfolio_tracking.data_storage import HISTORY_FILENAME_SUFIX, DataStorage
from portfolio_tracking.portfolio_management import Portfolio

def historical_main():
    order_1 = Order("2024-08-12", 1, 3.75)
    order_2 = Order("2024-08-13", 1, 3.5)
    order_3 = Order("2024-08-14", -2, 3.5)
    order_4 = Order("2024-08-20", 1, 28.18)

    asset_1 = Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "USD", [order_1, order_2, order_3])
    asset_2 = Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR", [order_4])

    assets_1 = Assets([asset_1, asset_2])
    # assets_1 = load_assets_json_file(HISTORIES_DIR_PATH / ASSETS_JSON_FILENAME)

    today = date.today()
    today_date = today.strftime("%Y-%m-%d")
    end_date = today_date #"2020-07-10"
    save_dir = Path(__file__).parent.absolute() / "histories"
    filename_sufix = HISTORY_FILENAME_SUFIX
    interval = "1d"
    assets_1.download_histories(today_date, save_dir, filename_sufix, interval)
    assets_1.load_histories(end_date, save_dir, filename_sufix)

    wallet_1 = Wallet(assets_1)
    # if DEBUG : print(f"wallet_1 : {wallet_1.to_dict()}")
    if DEBUG : print(f"wallet_1.dates : {wallet_1.dates[0]} -to- {wallet_1.dates[-1]}")
    if DEBUG : print(f"len(dates) : {len(wallet_1.dates)}")


    wallet_1.get_wallet_valuation()
    if DEBUG : print("wallet_1.valuations =\n", wallet_1.valuations)
    if DEBUG : print("wallet_1.investments =\n", wallet_1.investments)

    # twrr_cumulated, dates, twrr = wallet_1.get_wallet_TWRR(wallet_1.dates[0], wallet_1.dates[-1])
    # if DEBUG : print("twrr_cumulated =\n", twrr_cumulated)
    # if DEBUG : print("len(twrr_cumulated) =\n", len(twrr_cumulated))
    # if DEBUG : print("twrr =\n", twrr)
    # if DEBUG : print("len(twrr) =\n", len(twrr))

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


def my_new_main():
    # My exemple of usage
    today_date = date.today().strftime("%Y-%m-%d")

    list_of_assets: List[Asset] = []
    list_of_assets.append(Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "USD"))
    list_of_assets.append(Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR"))

    list_of_assets[0].add_orders([Order("2024-08-12", 1, 3.75),
                                  Order("2024-08-13", 1, 3.5),
                                  Order("2024-08-14", -2, 3.5)])
    list_of_assets[1].add_orders([Order("2024-08-20", 1, 28.18)])

    portfolio = Portfolio(currency="EUR")
    portfolio.add_assets(list_of_assets)

    data_importer = DataImport()
    data_storage = DataStorage()
    for asset in portfolio.assets :
        if data_storage.data_already_exist(asset) :
            # les lire et update
            pass
        else :
            df = data_importer.get_history(asset=asset,
                                           end_date=today_date)
            data_storage.save_data(asset=asset, df=df)


def GPT_new_main_1():
    # Example usage of the modules
    data_importer = DataImport(ticker="AAPL")
    data = data_importer.get_history(end_date="2022-12-31")

    data_storage = DataStorage(directory=Path("historical_data"))
    data_storage.save_data(data, filename="AAPL.csv")

    orders = [Order(date="2022-01-05", quantity=10, price=150), Order(date="2022-06-01", quantity=-5, price=170)]
    apple_asset = Asset(short_name="AAPL", name="Apple Inc.", ticker="AAPL", currency="USD", orders=orders)

    portfolio = Portfolio(currency="EUR", list_of_assets=[apple_asset])
    total_value = portfolio.get_total_value(date="2022-12-31", data_storage=data_storage)

    print(f"Total portfolio value on 2022-12-31: {total_value}")


def GPT_new_main_2():
    # Example usage
    data_storage = DataStorage(directory=Path("historical_data"))

    # Define assets and portfolio
    orders_apple = [Order(date="2022-01-05", quantity=10, price=150), Order(date="2022-06-01", quantity=-5, price=170)]
    apple_asset = Asset(short_name="AAPL", name="Apple Inc.", ticker="AAPL", currency="USD", orders=orders_apple)

    portfolio = Portfolio(assets=[apple_asset])

    # Create DataUpdater and CurrencyConverter instances
    data_updater = DataUpdater(portfolio=portfolio, data_storage=data_storage)
    currency_converter = CurrencyConverter(target_currency="EUR", data_storage=data_storage)

    # Update data and perform currency conversion
    data_updater.update_data(end_date="2022-12-31")
    currency_converter.convert_currency(asset_ticker="AAPL", asset_currency="USD", end_date="2022-12-31")


def GPT_irr():
    # Example usage:
    cash_flows = [-1000, 200, 300, 400, 500]  # Example cash flows
    portfolio = Portfolio(assets=[])
    irr = portfolio.calculate_irr(cash_flows)
    print(f"IRR: {irr}")


if __name__ == '__main__':
    my_new_main()
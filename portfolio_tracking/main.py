from datetime import date, datetime
from pathlib import Path
from typing import List

from portfolio_management import Wallet, load_assets_json_file
from data_downloader import DataDownloader
from data_storage import HISTORIES_DIR_PATH, HISTORY_FILENAME_SUFIX


DEBUG = True


def historical_main():
    today = datetime.now()
    today_date = today.strftime("%Y-%m-%d")
    end_date = today_date
    interval = "1d"

    wallet = Wallet()

    # asset_1 = Asset("Genfit", "Genfit SA", "GNFT.PA", "XTB", "EUR")
    # asset_2 = Asset("Spie", "Spie SA", "SPIE.PA", "XTB", "EUR")
    # list_of_assets = [asset_1, asset_2]
    list_of_assets = load_assets_json_file(HISTORIES_DIR_PATH / "assets_test.json")
    wallet.add_assets(list_of_assets)
    # asset_1.add_orders(wallet.db_manager,
    #                    [Order("2024-08-12", 1, 3.75),
    #                     Order("2024-08-13", 1, 3.5),
    #                     Order("2024-08-14", -2, 3.5)])
    # asset_2.add_orders(wallet.db_manager,
    #                    [Order("2024-08-20", 1, 28.18)])

    wallet.download_histories(end_date, HISTORIES_DIR_PATH, HISTORY_FILENAME_SUFIX, interval)

    wallet.set_evaluation_dates("2020-07-09", "2024-09-10")
    # if DEBUG : print(f"wallet :\n{wallet}")
    valuations = wallet.calculate_wallet_valuation()
    # if DEBUG : print(f"wallet :\n{wallet}")
    if DEBUG : print("valuations =\n", valuations)

    share_value = wallet.calculate_wallet_share_value(100)
    if DEBUG : print(f"share_value =\n{share_value}")
    if DEBUG : print("len(share_value) =\n", len(share_value))

    share_value_2, share_number_2 = wallet.calculate_wallet_share_value_2(1)
    if DEBUG : print("share_value_2 =\n", share_value_2)
    if DEBUG : print("len(share_value_2) =\n", len(share_value_2))
    if DEBUG : print("share_number_2 =\n", share_number_2)
    if DEBUG : print("len(share_number_2) =\n", len(share_number_2))

    share_value_3, share_number_3 = wallet.calculate_wallet_share_value_3(100)
    if DEBUG : print("share_value_3 =\n", share_value_3)
    if DEBUG : print("len(share_value_3) =\n", len(share_value_3))
    if DEBUG : print("share_number_3 =\n", share_number_3)
    if DEBUG : print("len(share_number_3) =\n", len(share_number_3))

    twrr_cumulated, twrr = wallet.calculate_wallet_TWRR(100)
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

    wallet = Wallet(currency="EUR")
    wallet.add_assets(list_of_assets)

    data_importer = DataDownloader()
    data_storage = DataStorage()
    for asset in wallet.assets :
        if data_storage.data_already_exist(asset) :
            # les lire et update
            pass
        else :
            df = data_importer.get_history(asset=asset,
                                           end_date=today_date)
            data_storage.save_data(asset=asset, df=df)


def GPT_new_main_1():
    # Example usage of the modules
    data_importer = DataDownloader(ticker="AAPL")
    data = data_importer.get_history(end_date="2022-12-31")

    data_storage = DataStorage(directory=Path("historical_data"))
    data_storage.save_data(data, filename="AAPL.csv")

    orders = [Order(date="2022-01-05", quantity=10, price=150), Order(date="2022-06-01", quantity=-5, price=170)]
    apple_asset = Asset(short_name="AAPL", name="Apple Inc.", ticker="AAPL", currency="USD", orders=orders)

    wallet = Wallet(currency="EUR", list_of_assets=[apple_asset])
    total_value = wallet.get_total_value(date="2022-12-31", data_storage=data_storage)

    print(f"Total wallet value on 2022-12-31: {total_value}")


def GPT_new_main_2():
    # Example usage
    data_storage = DataStorage(directory=Path("historical_data"))

    # Define assets and wallet
    orders_apple = [Order(date="2022-01-05", quantity=10, price=150), Order(date="2022-06-01", quantity=-5, price=170)]
    apple_asset = Asset(short_name="AAPL", name="Apple Inc.", ticker="AAPL", currency="USD", orders=orders_apple)

    wallet = Wallet(assets=[apple_asset])

    # Create DataUpdater and CurrencyConverter instances
    data_updater = DataUpdater(wallet=wallet, data_storage=data_storage)
    currency_converter = CurrencyConverter(target_currency="EUR", data_storage=data_storage)

    # Update data and perform currency conversion
    data_updater.update_data(end_date="2022-12-31")
    currency_converter.convert_currency(asset_ticker="AAPL", asset_currency="USD", end_date="2022-12-31")


def GPT_irr():
    # Example usage:
    cash_flows = [-1000, 200, 300, 400, 500]  # Example cash flows
    wallet = Wallet(assets=[])
    irr = wallet.calculate_irr(cash_flows)
    print(f"IRR: {irr}")


if __name__ == '__main__':
    historical_main()

    # my_new_main()
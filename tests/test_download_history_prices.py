from portfolio_tracking.wallet_data import Wallet
from portfolio_tracking.yfinance_interface import Asset, Assets, Order


def test_wallet():
    date_1 = "2024-01-01"
    date_2 = "2024-02-01"
    quantity_1 = 1
    quantity_2 = -1
    price_1 = 3.75
    price_2 = 6.75

    order_1 = Order(date_1, quantity_1, price_1)
    order_2 = Order(date_2, quantity_2, price_2)

    short_name_1 = "Genfit"
    short_name_2 = "Spie"
    name_1 = "Genfit SA"
    name_2 = "Spie SA"
    ticker_1 = "GNFT.PA"
    ticker_2 = "SPIE.PA"
    broker_1 = "XTB"
    broker_2 = "Saxo"
    devise_1 = "EUR"
    devise_2 = "USD"

    asset_1 = Asset(short_name_1, name_1, ticker_1, broker_1, devise_1, [order_1])
    asset_2 = Asset(short_name_2, name_2, ticker_2, broker_2, devise_2, [order_2])

    assets = Assets([asset_1, asset_2])

    wallet = Wallet(assets)

    wallet_dic = {
        "assets": [asset_1.to_dict(), asset_2.to_dict()],
        "dates": assets.get_dates(),
        "valuation": [],
        "devise": "EUR",
        "investment": [],
        "twrr": [],
        "twrr_cumulated": [],
        "share_value": [],
        "share_value_2": [],
        "share_number_2": [],
    }

    assert(wallet.assets == assets.assets)
    assert(wallet.dates == assets.get_dates())
    assert(wallet.to_dict() == wallet_dic)

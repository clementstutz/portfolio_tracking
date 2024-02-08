from .download_history_prices import Wallet, get_dates
from .yfinance_interface import Asset, Assets, Order


def test_orders():
    date = "2024-01-01"
    quantity = 1
    price = 3.75

    order_1 = Order(date, quantity, price)

    order_dic = {
        "date": date,
        "quantity": quantity,
        "price": price
    }

    assert(order_1.date == date)
    assert(order_1.quantity == quantity)
    assert(order_1.price == price)
    assert(order_1.to_dict() == order_dic)


def test_asset():
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
    asset_1.add_orders([order_2])
    asset_2 = Asset(short_name_2, name_2, ticker_2, broker_2, devise_2)
    asset_2.add_orders([order_1, order_2])

    asset_1_dic = {
        "short_name": short_name_1,
        "name": name_1,
        "ticker": ticker_1,
        "broker": broker_1,
        "devise": devise_1,
        "orders": [order_1.to_dict(), order_2.to_dict()],
        "dates": [],
        "closes": []
    }

    asset_2_dic = {
        "short_name": short_name_2,
        "name": name_2,
        "ticker": ticker_2,
        "broker": broker_2,
        "devise": devise_2,
        "orders": [order_1.to_dict(), order_2.to_dict()],
        "dates": [],
        "closes": []
    }

    assert(asset_1.short_name == short_name_1)
    assert(asset_1.name == name_1)
    assert(asset_1.ticker == ticker_1)
    assert(asset_1.broker == broker_1)
    assert(asset_1.devise == devise_1)
    assert(asset_1.orders == [order_1, order_2])
    assert(asset_1.dates == [])
    assert(asset_1.closes == [])
    assert(asset_1.to_dict() == asset_1_dic)

    assert(asset_2.short_name == short_name_2)
    assert(asset_2.name == name_2)
    assert(asset_2.ticker == ticker_2)
    assert(asset_2.broker == broker_2)
    assert(asset_2.devise == devise_2)
    assert(asset_2.orders == [order_1, order_2])
    assert(asset_2.dates == [])
    assert(asset_2.closes == [])
    assert(asset_2.to_dict() == asset_2_dic)


def test_assets():
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

    assets_1 = Assets()
    assets_1.add_asset(asset_1)
    assets_2 = Assets([asset_1, asset_2])

    assets_1_dic = {
        "assets": [asset_1.to_dict()]
    }

    assets_2_dic = {
        "assets": [asset_1.to_dict(), asset_2.to_dict()]
    }

    assert(assets_1.assets == [asset_1])
    assert(assets_1.to_dict() == assets_1_dic)

    assert(assets_2.assets == [asset_1, asset_2])
    assert(assets_2.to_dict() == assets_2_dic)


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

    dates = get_dates(assets)

    wallet = Wallet(assets)

    wallet_dic = {
        "assets": [asset_1.to_dict(), asset_2.to_dict()],
        "dates": dates,
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
    assert(wallet.dates == dates)
    assert(wallet.to_dict() == wallet_dic)

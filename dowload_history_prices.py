import json
import requests
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import csv


def ymd_to_timestamp(date: str) -> int:
    dt = datetime.strptime(date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
    utc_dt = dt.replace(tzinfo=timezone.utc)
    return int(utc_dt.timestamp())


def _download_stock_data(url, save_path):
    # Thank's GPT
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Leve une exception en cas de probleme avec la requête

        with open(save_path, 'wb') as file:
            file.write(response.content)

        print(f'Le fichier CSV a ete telecharge avec succes et enregistre sous {save_path}')
    except requests.exceptions.RequestException as e:
        print(f'Une erreur est survenue lors du telechargement : {e}')


def normalized_name(name):
    return name.replace(' ', '_').replace('-', '_')


def _get_history(asset, end_date, save_path_sufix, interval):
    start_timestamp = ymd_to_timestamp(asset['orders'][0]['date'])
    end_timestamp = ymd_to_timestamp(end_date)
    # URL generee a partir de la page : https://fr.finance.yahoo.com/quote/AAPL/history?period1=1690848000&period2=1691452800&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{asset['ticker']}?period1={start_timestamp}&period2={end_timestamp}&interval={interval}&events=history&includeAdjustedClose=true"
    save_path = f"{normalized_name(asset['short_name'])}{save_path_sufix}"
    _download_stock_data(url, save_path)


def dowload_devise(asset, end_date, save_path_sufix='_history.csv', interval='1d'):
    devise_asset = {
            "short_name": 'EUR/'+asset['devise'],
            "ticker": 'EUR'+asset['devise'],
            "orders": [
				{"date": asset['orders'][0]['date']}
			]
        }
    _get_history(devise_asset, end_date, save_path_sufix, interval)


def dowload_history(assets_jsonfile, end_date, save_path_sufix='_history.csv', interval='1d'):
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets = json.load(asset_file)

    for asset in assets['assets']:
        _get_history(asset,
                     end_date,
                     save_path_sufix,
                     interval)


def load_history(assets_jsonfile, save_path_sufix='_history.csv'):
    with open(assets_jsonfile, 'r', encoding='utf-8') as asset_file:
        assets = json.load(asset_file)

    for asset in assets['assets']:
        dates = []
        close = []
        csv_filename = f"{normalized_name(asset['short_name'])}{save_path_sufix}"
        with open(csv_filename, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)

            # Parcourir les lignes du fichier CSV
            for row in csvreader:
                if not 'null' in row['Close']:
                    # row est un dictionnaire où les clés sont les noms de colonnes
                    dates.append(row['Date'])
                    close.append(float(row['Close']))
                else:
                    print(f'ERROR in file: {csv_filename}, row = {row}')
                    # TODO: Trouver mieux que ça...
                    dates.append(row['Date'])
                    close.append(close[-1])
        asset['dates'] = dates
        asset['close'] = close
    return assets


#####################################
def plot_single_chart(dates_and_prices, value, figure_id, title, xlabel, ylabel, style='', xscale='linear', yscale='linear'):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.xscale(xscale)
    plt.ylabel(ylabel)
    plt.yscale(yscale)
    ymin = min(0, min(dates_and_prices[value][:])*1.05)
    ymax = max(0, max(dates_and_prices[value][:])*1.05)
    plt.ylim(ymin, ymax)
    plt.grid(True)
    plt.plot(dates_and_prices['dates'][:], dates_and_prices[value][:], style)
    plt.show(block=False)


def plot_multi_charts(dates_and_prices, values, figure_id, title, xlabel, ylabel, xscale='linear', yscale='linear'):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.xscale(xscale)
    plt.ylabel(ylabel)
    plt.yscale(yscale)
    ymin = 0
    ymax = 0
    for value in values:
        ymin = min(ymin, min(dates_and_prices[value][:])*1.05)
        ymax = max(ymax, max(dates_and_prices[value][:])*1.05)
    plt.ylim(ymin, ymax)
    plt.grid(True)
    for value in values:
        plt.plot(dates_and_prices['dates'][:], dates_and_prices[value][:])
    plt.show(block=False)
#####################################


def get_dates(assets):
    dates_temp = set()
    for asset in assets:
        asset_dates = asset['dates']
        for date in asset_dates:
            dates_temp.add(date)
    return list(sorted(dates_temp))


def get_close_price_of_a_day(date, asset, wallet_history):
    if date in asset['dates']:
        date_index = asset['dates'].index(date)
        return asset['close'][date_index]
    date_index = wallet_history['dates'].index(date)
    previous_date = wallet_history['dates'][date_index-1]
    previous_date_index = asset['dates'].index(previous_date)
    return asset['close'][previous_date_index]


def get_new_investement(date, asset, actions_count):
    for i in range(len(asset['orders'])):
        if date == asset['orders'][i]['date']:
            actions_count[asset['short_name']] += asset['orders'][i]['quantity']
            return asset['orders'][i]['price'] * asset['orders'][i]['quantity']
    return 0


def current_share_value(current_wallet_value, net_deposit, previous_wallet_value, previous_share_value):
    return (current_wallet_value - net_deposit) / previous_wallet_value * previous_share_value


def current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value):
        if net_deposit == 0:
            return (current_wallet_value) / nb_part, nb_part
        else:
            nb_part = current_wallet_value / current_share_value
            return (current_wallet_value) / nb_part, nb_part


def get_wallet_valuation(wallet_history, assets):
    wallet_history['valuation'] = []
    wallet_history['investment'] = []

    actions_count = {asset['short_name']: 0 for asset in assets}  # Initialisez un dictionnaire pour suivre le nombre d'actions de chaque asset
    investement = 0
    for date in wallet_history['dates']:
        total_valuation = 0  # Initialisez la valeur totale du portefeuille à 0
        for asset in assets:
            if asset['dates'][0] <= date <= asset['dates'][-1]:
                close_price = get_close_price_of_a_day(date, asset, wallet_history)
                investement += get_new_investement(date, asset, actions_count)
                total_valuation += close_price * actions_count[asset['short_name']]
            else:
                pass
        # normalized_value = (total_valuation) / investement * base
        # wallet_history['useless_close'].append(normalized_value)  # Nul ! n'a aucune utilité
        wallet_history['valuation'].append(total_valuation)
        wallet_history['investment'].append(investement)
    return wallet_history

def get_wallet_share_value(wallet_history, assets, base):
    wallet_history['share_value'] = [base]
    actions_count = {asset['short_name']: 0 for asset in assets}

    _current_share_value = base
    for asset in assets:
        get_new_investement(wallet_history['dates'][0], asset, actions_count)

    for date in wallet_history['dates'][1:]:
        new_value_invested = 0
        for asset in assets:
            if asset['dates'][0] <= date <= asset['dates'][-1]:
                new_investement = get_new_investement(date, asset, actions_count)
                new_value_invested += new_investement
            else:
                pass
        _current_wallet_value = wallet_history['valuation'][wallet_history['dates'].index(date)]
        _net_deposit = new_value_invested
        _previous_wallet_value = wallet_history['valuation'][wallet_history['dates'].index(date)-1]
        _previous_share_value = _current_share_value
        _current_share_value = current_share_value(_current_wallet_value, _net_deposit, _previous_wallet_value, _previous_share_value)
        wallet_history['share_value'].append(_current_share_value)
    return wallet_history

def get_wallet_share_value_2(wallet_history, nb_part):
    _current_share_value = wallet_history['valuation'][0] / nb_part
    wallet_history['share_value_2'] = [_current_share_value]
    wallet_history['share_number_2'] = [nb_part]

    for date in wallet_history['dates'][1:]:
        _current_wallet_value = wallet_history['valuation'][wallet_history['dates'].index(date)]
        _net_deposit = wallet_history['investment'][wallet_history['dates'].index(date)] - wallet_history['investment'][wallet_history['dates'].index(date)-1]
        _current_share_value, nb_part = current_share_value_2(_current_wallet_value, _net_deposit, nb_part, _current_share_value)
        wallet_history['share_value_2'].append(_current_share_value)
        wallet_history['share_number_2'].append(nb_part)
    return wallet_history

def get_wallet_TRI(wallet_history):
    wallet_history['TRI'] = []

    return wallet_history


def main():
    end_date_ = '2021-02-05'
    end_date = '2023-09-28'
    interval = '1d'  # must be '1d', '1wk' or '1mo'
    assets_jsonfile_ = 'assets_short.json'
    assets_jsonfile = 'assets.json'
    save_path_sufix = '_history.csv'

    assets = {
        'assets': [
            {'short_name': 'Wayne',
             "devise": 'EUR',
             'orders': [{'date': '2023-08-01', 'quantity': 15, "price": 104},
                        {'date': '2023-08-03', 'quantity': 5, "price": 107}],
             'dates': ['2023-08-01',
                       '2023-08-02',
                       '2023-08-03',
                       '2023-08-04'],
             'close': [105, 108, 106, 107]
            },
            {'short_name': 'Stark',
             "devise": 'USD',
             'orders': [{'date': '2023-08-03', 'quantity': 25, "price": 75}],
             'dates': ['2023-08-03',
                       '2023-08-04'],
             'close': [75, 76]
            }
        ]
    }

    #dowload_history(assets_jsonfile, end_date, save_path_sufix, interval)
    assets = load_history(assets_jsonfile, save_path_sufix)

    # for asset in assets['assets']:
    #     if asset['devise'] != 'EUR':
    #         dowload_devise(asset, end_date)

    # return 0
    wallet_history = {}
    wallet_history['dates'] = get_dates(assets=assets['assets'])

    wallet_history = get_wallet_valuation(wallet_history=wallet_history,
                                          assets=assets['assets'])

    wallet_history = get_wallet_share_value(wallet_history=wallet_history,
                                            assets=assets['assets'],
                                            base=100)
    
    wallet_history = get_wallet_share_value_2(wallet_history=wallet_history,
                                              nb_part=4.3847951)

    print("wallet_history['share_value'] =", wallet_history['share_value'][-1])
    print("wallet_history['share_value_2'] =", wallet_history['share_value_2'][-1])
    print("wallet_history['share_number_2'] =", wallet_history['share_number_2'][-1])

    plot_multi_charts(dates_and_prices=wallet_history,
                      values = ['valuation', 'investment'],
                      figure_id=2,
                      title='wallet_history',
                      xlabel='time',
                      ylabel='€')

    # plot_single_chart(dates_and_prices=wallet_history,
    #                   value = 'share_value',
    #                   figure_id=3,
    #                   title='share_value',
    #                   xlabel='time',
    #                   ylabel='€')
    
    # plot_single_chart(dates_and_prices=wallet_history,
    #                   value = 'share_value_2',
    #                   figure_id=4,
    #                   title='share_value_2',
    #                   xlabel='time',
    #                   ylabel='€')
    
    plot_multi_charts(dates_and_prices=wallet_history,
                      values = ['share_value', 'share_value_2'],
                      figure_id=2,
                      title='wallet_history',
                      xlabel='time',
                      ylabel='€')
    
    plt.show()
    

    # print("")
    # nb_part = 100
    # current_wallet_value = 10000
    # net_deposit = 0
    # current_share_value = 100
    # current_share_value, nb_part = current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value)
    # print("nb_part =", nb_part, " _current_share_value =", current_share_value, " capitalisation =", nb_part*current_share_value)

    # current_wallet_value = 12123
    # net_deposit = 0
    # current_share_value, nb_part = current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value)
    # print("nb_part =", nb_part, " _current_share_value =", current_share_value, " capitalisation =", nb_part*current_share_value)

    # current_wallet_value = 14123
    # net_deposit = 2000
    # current_share_value, nb_part = current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value)
    # print("nb_part =", nb_part, " _current_share_value =", current_share_value, " capitalisation =", nb_part*current_share_value)

    # current_wallet_value = 14200
    # net_deposit = 0
    # current_share_value, nb_part = current_share_value_2(current_wallet_value, net_deposit, nb_part, current_share_value)
    # print("nb_part =", nb_part, " _current_share_value =", current_share_value, " capitalisation =", nb_part*current_share_value)

main()

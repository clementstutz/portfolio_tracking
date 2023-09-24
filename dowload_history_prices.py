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
def plot_single_chart(dates_and_prices, figure_id, title, xlabel, ylabel):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)

    plt.plot(dates_and_prices['dates'][:], dates_and_prices['close'][:], '+-b')
    plt.show()


def plot_multi_charts(dates_and_prices, figure_id, title, xlabel, ylabel):
    plt.figure(figure_id)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)

    for elm in dates_and_prices:
        plt.plot(elm[0][:], elm[1][:], '+-b')
    plt.show()



def _get_dates(assets):
    # print('assets =', assets)
    dates_temp = set()
    for asset in assets:
        asset_dates = asset['dates']
        for date in asset_dates:
            dates_temp.add(date)
    return list(sorted(dates_temp))


def get_value_invested(date, asset, action_counts, value_invested, prix):
    for i in range(len(asset['orders'])):
        if date == asset['orders'][i]['date']:
            quantity = asset['orders'][i]['quantity']
            action_counts[asset['short_name']] += quantity
            value_invested += prix * quantity

    return value_invested


def get_wallet_history(assets, base):
    wallet_history = {}
    wallet_history['dates'] = _get_dates(assets)

    values = []  # Liste pour stocker les valeurs du portefeuille
    value_invested = 0
    action_counts = {asset['short_name']: 0 for asset in assets}  # Initialisez un dictionnaire pour suivre le nombre d'actions de chaque asset
    for date in wallet_history['dates']:
        total_value = 0  # Initialisez la valeur totale du portefeuille à 0
        total_quantity = 0  # Initialisez la quantité totale d'actions à 0
        for asset in assets:
            if asset['dates'][0] <= date <= asset['dates'][-1]:
                if date in asset['dates']:
                    date_index = asset['dates'].index(date)
                    # Obtenez le prix et la quantité correspondants à cette date
                    prix = asset['close'][date_index]
                else:
                    date_index = wallet_history['dates'].index(date)
                    previous_date = wallet_history['dates'][date_index-1]
                    previous_date_index = asset['dates'].index(previous_date)
                    # Obtenez le prix et la quantité correspondants à cette date
                    prix = asset['close'][previous_date_index]
                    # Mettez à jour le nombre d'actions de cet asset
                    for i in range(len(asset['orders'])):
                        if date == asset['orders'][i]['date']:
                            quantity = asset['orders'][i]['quantity']
                            action_counts[asset['short_name']] += quantity
                value_invested = get_value_invested(date, asset, action_counts, value_invested, prix)
                # Mettez à jour la valeur totale et la quantité totale pondérée
                total_value += prix * action_counts[asset['short_name']]
                total_quantity += action_counts[asset['short_name']]
            else:
                pass
        # Calculez la valeur pondérée du portefeuille à cette date
        if total_quantity > 0:
            normalized_value = total_value / value_invested * base
            values.append(normalized_value)
    wallet_history['close'] = values
    return wallet_history


def main():
    end_date = '2023-09-20'
    interval = '1d'  # must be '1d', '1wk' or '1mo'
    assets_jsonfile = 'assets_example.json'
    save_path_sufix = '_history.csv'

    dowload_history(assets_jsonfile, end_date, save_path_sufix, interval)
    assets = load_history(assets_jsonfile, save_path_sufix)

    wallet_history = get_wallet_history(assets=assets['assets'],
                                        base=100)

    plot_single_chart(dates_and_prices=wallet_history,
                      figure_id=2,
                      title='wallet_history',
                      xlabel='time',
                      ylabel='€')


main()

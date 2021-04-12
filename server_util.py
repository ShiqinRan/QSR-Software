import pandas as pd
import numpy as np
import os
import requests
import datetime
import json

# pull historical data from source 1
def get_historical_data(tickers, interval):
    tickers_ts = {}
    for ticker in tickers:
        ticker = ticker
        slices = ['year1month2','year1month1'] #could add more until year2month12, used 2 months for now
        apiKey = 'MGMR9XRHE6PE2RUN'

        result_df = pd.DataFrame()
        for slice in slices:
            df = pd.read_csv(
                'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' +
                ticker + '&interval=' + interval + '&slice=' + slice + '&apikey=' + apiKey)
            # reformat df
            df = df.drop(columns=['open', 'high', 'low', 'volume'])
            df = df.rename(columns={"time": "datetime", "close": "price"})
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.sort_values(by='datetime', inplace=True)
            df.set_index('datetime', inplace=True)

            #append
            result_df = result_df.append(df)

        tickers_ts[ticker] = result_df
    return  tickers_ts


# load data from reload file, assume formatting is the same as source 1
def load_file(file, tickers):
    if len(tickers > 1):
        raise Exception('too many tickers')

    df = pd.read_csv(file)

    # reformat df
    df = df.drop(columns=['open', 'high', 'low', 'volume'])
    df = df.rename(columns={"time": "datetime", "close": "price"})
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    tickers_ts = {tickers[0]: df}

    return tickers_ts


# compute pnl column
def compute_pnl_column(df):
    pos_1 = df['signal'].values[:-1]
    s = df['price'].values[1:]
    s_1 = df['price'].values[:-1]
    pnl = pos_1 * (s - s_1)
    pnl = np.insert(pnl, 0 , 0)
    df['pnl'] = pnl

    return df

# compute trading signal column
def compute_trading_signal_column(df):
    df['s_avg'] = df['price'].rolling('24H').mean()
    df['sigma'] = df['price'].rolling('24H').std()

    signal = [0]
    prev_pos = 0
    for index, row in df.iterrows():
        s = row['price']
        s_avg = row['s_avg']
        sigma = row['sigma']
        try:
            if s > s_avg + sigma:
                signal.append(1)
                prev_pos = 1
            elif s < s_avg + sigma:
                signal.append(-1)
                prev_pos = -1
            else:
                signal.append(prev_pos)
        except:
            signal.append(0)
            prev_pos = 0
    signal = signal[:-1]
    df['signal'] = signal

    return df

# process historical data, return computed df
def process_historical_data(historical_data):
    processed_data = {}
    for ticker in historical_data:
        add_signal = compute_trading_signal_column(historical_data[ticker])
        add_pnl = compute_pnl_column(add_signal)
        processed_data[ticker] = add_pnl
    return processed_data


# save to csv files
def save_to_files(ticker_computations):
    path = os.path.abspath(os.getcwd())

    for ticker in ticker_computations:
        ticker_df = ticker_computations[ticker]
        ticker_df = ticker_df.drop(columns=['s_avg', 'sigma'])

        # save result file
        ticker_df.to_csv(path_or_buf= path + '/' + ticker + '_result.csv')

        # save price file
        ticker_df = ticker_df.drop(columns=['signal', 'pnl'])
        ticker_df.to_csv(path_or_buf=path + '/' + ticker + '_price.csv')


# get current data from source 2
def get_realtime_data(ticker):
    r = requests.get('https://finnhub.io/api/v1/quote?symbol='+ ticker +'&token=c1p8k4qad3ic1jon9spg')
    return r.json()

def get_price(result):
    return result['c']

#update the ticker computation df
def update_computations(ticker, df, interval):
    # get current price from source 2
    current_price_json = get_realtime_data(ticker)
    price = get_price(current_price_json)
    utc_time = datetime.datetime.utcfromtimestamp(current_price_json['t'])
    utc_time = utc_time.strftime("%Y-%m-%d %H:%M:%S")
    if utc_time == df.index[-1]:
        return df

    #calculate signal
    total_intervals = int(24 * 60 / interval)
    rolling_window = df['price'][-total_intervals:].to_numpy()
    rolling_window = np.append(rolling_window, price)
    s_avg = rolling_window.mean()
    sigma = rolling_window.std()

    last_price = df['price'].iloc[-1]
    last_s_avg = df['s_avg'].iloc[-1]
    last_sigma = df['sigma'].iloc[-1]
    last_signal = df['signal'].iloc[-1]

    if last_price > last_s_avg + last_sigma:
        signal = 1
    elif last_price < last_s_avg + last_sigma:
        signal = -1
    else:
        signal = last_signal

    #calculate pnl
    pnl = last_signal * (price - last_price)

    newrow = pd.Series(data= {'price' : price, 's_avg' : s_avg, 'sigma' : sigma, 'signal' : signal, 'pnl' : pnl}, name=utc_time)
    df = df.append(newrow, ignore_index=False)

    return df

def process_action(action, tickers_computation):
    if action == 'reset':
        pass
    elif action == 'price':
        pass
    elif action == 'signal':
        pass
    elif action == 'del_ticker':
        pass
    elif action == 'add_ticker':
        pass

def search_column(time, df):
    time_reformat = datetime.datetime.strptime(time, '%Y-%m-%d-%H:%M')
    time_reformat = time_reformat.strftime('%Y-%m-%d %H:%M:%S')
    try:
        result = df.loc[time_reformat]
    except:
        if datetime.datetime.strptime(time_reformat, '%Y-%m-%d %H:%M:%S') > df.index[-1] or \
                datetime.datetime.strptime(time_reformat, '%Y-%m-%d %H:%M:%S') < df.index[0]:
            result = 'Server has no data'
        else:
            result = 'No Data'
    return result

def check_symbol(ticker):
    return True


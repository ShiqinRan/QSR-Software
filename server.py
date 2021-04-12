import socket
import argparse
from server_util import *
import threading
import time
import sys

tickers_computations = None
interval = None

def server_queries(interval):
    interval_sec = interval * 60
    starttime = time.time()
    while True:
        time.sleep(interval_sec - ((time.time() - starttime) % interval_sec))
        for ticker, df in tickers_computations.items():
            tickers_computations[ticker] = update_computations(ticker, df, interval)


def server(server_socket):
    while True:
        server_socket.listen(1)
        print('listening to client connection')

        connection, client_address = server_socket.accept()
        client_request = connection.recv(2048)
        request_decoded = json.loads(client_request.decode('utf-8'))
        action = list(request_decoded)[0]

        result = None

        #procee action
        if action == 'reset':
            result = 0
            try:
                global tickers_computations
                tickers = list(tickers_computations)
                historical_data = get_historical_data(tickers, str(interval) + "min")
                tickers_computations = process_historical_data(historical_data)
                save_to_files(tickers_computations)
            except:
                result = 1

        elif action == 'price':
            time = request_decoded['price']
            result = {}

            if time == 'now':
                for ticker in tickers_computations:
                    price = get_price(get_realtime_data(ticker))
                    result[ticker] = price
            else:
                for ticker, df in tickers_computations.items():
                    price_df = df['price']
                    price = search_column(time, price_df)
                    if price == 'Server has no data':
                        result = price
                        break
                    else:
                        result[ticker] = price

        elif action == 'signal':
            time = request_decoded['signal']
            result = {}

            if time == 'now':
                for ticker, df in tickers_computations.items():
                    signal = df['signal'].iloc[-1]
                    result[ticker] = signal
            else:
                for ticker, df in tickers_computations.items():
                    signal_df = tickers_computations[ticker]['signal']
                    signal = search_column(time, signal_df)
                    if signal == 'Server has no data':
                        result = signal
                        break
                    else:
                        result[ticker] = signal

        elif action == 'del_ticker':
            ticker = request_decoded['del_ticker']
            if ticker in list(tickers_computations):
                result = 0
                try:
                    tickers_computations.pop(ticker)
                except:
                    result = 1
            else:
                result = 2

        elif action == 'add_ticker':
            ticker = request_decoded['add_ticker']
            try:
                valid_symbol = check_symbol(ticker)
                if valid_symbol:
                    if ticker not in list(tickers_computations):
                        historical_data = get_historical_data([ticker], str(interval) + "min")
                        processed_data = process_historical_data(historical_data)
                        tickers_computations[ticker] = processed_data[ticker]
                    result = 0
                else:
                    result = 1
            except:
                result = 2

        #send result to client
        result_encoded = json.dumps(result).encode('utf-8')
        connection.send(result_encoded)
        connection.close()


def main():
    try:
        # parse input options
        parser = argparse.ArgumentParser()
        parser.add_argument("--tickers",
                            help = 'max 3 US tickers. default =  AAPL',
                            default = ['AAPL'],
                            action= 'store',
                            nargs='*')
        parser.add_argument("--port",
                            help='port number of the server. default =  8000',
                            default=8000,
                            action='store')
        parser.add_argument("--reload",
                            help='file to load historical data. default =  AAPL',
                            action='store',
                            dest='reload_file')
        parser.add_argument("--minutes",
                            help='time interval to download data. default =  5',
                            default=5,
                            action='store')
        args = parser.parse_args()

        # input validation
        global interval
        tickers = args.tickers
        port = args.port
        reload_file = args.reload_file
        interval = args.minutes

        if len(tickers) > 3:
            print('Number of tickers provided exceeds maximum (3)')
            return
        if interval not in [5,15,30,60]:
            print('incorrect interval. Please use 5, 15, 30, 60')
            return


        # start server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('localhost', port)
        print('starting up server on %s port %s' % server_address)
        sock.bind(server_address)

        # start up computations
        try:
            if reload_file is not None:
                historical_data = load_file(reload_file, tickers)
            else:
                historical_data = get_historical_data(tickers, str(interval) + "min")

            global tickers_computations
            tickers_computations = process_historical_data(historical_data)
        except KeyboardInterrupt:
            print('Shutting down server')
            sys.exit(0)
        except:
            raise Exception('historical computation failed')


        # save to files
        save_to_files(tickers_computations)

        print('start up completed')

        # start to listen to client requests
        t1 = threading.Thread(target=server, args=(sock,))
        t2 = threading.Thread(target=server_queries, args=(interval,))
        t1.start()
        t2.start()
    except KeyboardInterrupt:
        print('Shutting down server')
        sys.exit(0)





if __name__ == '__main__':
    main()
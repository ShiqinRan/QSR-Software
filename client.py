import argparse
import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json


def main():
    # parse input options
    parser = argparse.ArgumentParser()
    parser.add_argument("--price",
                        help='o	If specified, queries server for latest price available as of the time specified. The time queried is expected to be in UTC Time.',
                        action='store')
    parser.add_argument("--signal",
                        help='o	If specified, queries server for latest trading signal available as of the time specified. The time queried is expected to be in UTC Time.',
                        action='store')
    parser.add_argument("--server_address",
                        help='o	If specified, connect to server running on the IP address, and use specified port number.',
                        action='store',
                        default='127.0.0.1:8000')
    parser.add_argument("--del_ticker",
                        help='Instruct the server to del a ticker from the server database.',
                        action='store')
    parser.add_argument("--add_ticker",
                        help='o	Instruct the server to add a new ticker to the server database. Server must download historical data for said ticker, and start appending on the next pull.',
                        action='store')
    parser.add_argument("--reset",
                        help='instructs the server to reset all the data',
                        action='store_true')
    args = parser.parse_args()

    # input validation
    price = args.price
    signal = args.signal
    server_address = args.server_address.split(':')
    del_ticker = args.del_ticker
    add_ticker = args.add_ticker
    reset = args.reset
    arg_list = [price,signal,del_ticker,add_ticker,reset]
    arg_count = sum(x is not None for x in arg_list)
    if arg_count == 1 and reset == False:
        print('please specify action')
        return
    elif arg_count > 1 and reset == True:
        print('too many actions')
        return

    #connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (server_address[0], int(server_address[1]))
    print('connecting to %s port %s' % server_address)

    try:
        sock.connect(server_address)
        # send message to server
        if reset == True:
            data = {'reset' : reset}
        elif price:
            data = {'price': price}
        elif signal:
            data = {'signal' : signal}
        elif del_ticker:
            data = {'del_ticker' : del_ticker}
        elif add_ticker:
            data = {'add_ticker' : add_ticker}
        else:
            data = None
        data_encoded = json.dumps(data).encode('utf-8')
        sock.sendall(data_encoded)

        # get response from server
        result = sock.recv(2048)
        result_decoded = json.loads(result.decode('utf-8'))
        if isinstance(result_decoded, dict):
            for key, val in result_decoded.items():
                print(key, val)
        else:
            print(result_decoded)
        return

    except Exception as e:
        print('Server Error. Sending notification email')

        mail_content = 'Server Error'

        #read from configuration
        sender_address = None
        sender_pass = None
        receiver_address = []
        file = open('configuration.txt', 'r')
        count = 0
        while True:
            count += 1
            line = file.readline()
            if not line:
                break
            if count == 1:
                sender_address = line.strip()
            elif count == 2:
                sender_pass = line.strip()
            else:
                receiver_address.append(line.strip())

        #Setup the MIME
        message = MIMEMultipart()
        message['From'] = sender_address
        message['To'] = receiver_address
        message['Subject'] = 'Server Error Notification'
        message.attach(MIMEText(mail_content, 'plain'))
        session = smtplib.SMTP('smtp-mail.outlook.com', 465)
        session.starttls()
        session.login(sender_address, sender_pass)
        text = message.as_string()
        session.sendmail(sender_address, receiver_address, text)
        session.quit()
        print('Mail Sent')


if __name__ == '__main__':
    main()
import requests
import json
import time
import sys
import pandas as pd
import calendar
from datetime import datetime, timedelta

def get_unix_ms_from_date(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond/1000)

def get_trades(symbol, start):
    r = requests.get(f'https://api-pub.bitfinex.com/v2/trades/{symbol}/hist?limit=10000&start={start}&sort=1')
    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        get_historical_trades(symbol, start)

    return r.json()

def trim(df, to_date):
    return df[df[1] <= get_unix_ms_from_date(to_date)]

def fetch_bitfinex_trades(symbol, from_date, to_date):
    current_time = get_unix_ms_from_date(from_date)
    df = pd.DataFrame()

    while current_time < get_unix_ms_from_date(to_date):
        try:
            trades = get_trades(symbol, current_time)
            current_time = trades[-1][1]
            
            print(f'fetched {len(trades)} @ {datetime.utcfromtimestamp(current_time/1000.0)}')
            
            df = pd.concat([df, pd.DataFrame(trades)])
        
            #dont exceed request limits
            time.sleep(0.1)
        except Exception:
            print('somethings wrong....... sleeping for 15s')
            time.sleep(15)

    df = trim(df, to_date)
    
    filename = f'bitfinex__{symbol.replace(":", "")}__trades__from__{sys.argv[2].replace("/", "_")}__to__{sys.argv[3].replace("/", "_")}.csv'
    df.to_csv(filename)

    print(f'{filename} file created!')

if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception('arguments format: <symbol> <start_date> <end_date>')
        exit()

    symbol = sys.argv[1]

    from_date = datetime.strptime(sys.argv[2], '%m/%d/%Y')
    to_date = datetime.strptime(sys.argv[3], '%m/%d/%Y') + timedelta(days=1) - timedelta(microseconds=1)

    fetch_bitfinex_trades(symbol, from_date, to_date)
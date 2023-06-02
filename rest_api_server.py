from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from threading import Lock

from helpers import *

app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': 'http://localhost:3000'}})

api = Api(app)

parser = reqparse.RequestParser()
        
parser.add_argument('tickers', type=list, location='json')
parser.add_argument('start', type=str)
parser.add_argument('end', type=str)
parser.add_argument('interval', type=str)

lock = Lock()

class History(Resource):
    def post(self):
        
        args = parser.parse_args()
        print(args)
        tickers = args['tickers']
        start = args['start']
        end = args['end']
        interval = args['interval']

        date_format_dict = {
            '1d': '%Y-%m-%d',
            '15m': '%Y-%m-%dT%H:%M'
        }
        date_format = date_format_dict[interval]

        with lock:
            df = yf.download(
                tickers,
                start=start,
                end=end,
                interval=interval,
                progress=False,
                show_errors=True,
                ignore_tz=True,
                prepost=True
            )

        columns = {
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adjClose',
            'Volume': 'volume'
        }
        df.rename(columns=columns, inplace=True)

        def create_bar(data, timestamp, ticker):
            data.update({
                'ticker': ticker,
                'dateTime': pd.to_datetime(timestamp, utc=True).strftime(date_format),
            })
            return data

        bars = []
        if len(set(tickers)) == 1:
            for timestamp, data in df.to_dict(orient='index').items():
                bars.append(create_bar(data, timestamp, tickers[0]))
        else:
            for (timestamp, ticker), data in df.stack().to_dict(orient='index').items():
                bars.append(create_bar(data, timestamp, ticker))

        return bars, 200

api.add_resource(History, '/history')

class Gainers(Resource):
    def get(self):
        df_gainers = get_df("https://finance.yahoo.com/screener/predefined/day_gainers")[0]
        df_gainers.dropna(how="all", axis=1, inplace=True)
        df_gainers = df_gainers.replace(float("NaN"), "")

        return df_gainers.to_json(), 200

api.add_resource(Gainers, '/gainers')

if __name__ == '__main__':
    app.run(port=5002)
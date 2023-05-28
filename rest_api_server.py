from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from datetime import datetime

from helpers import *

app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': 'http://localhost:3000'}})

api = Api(app)

parser = reqparse.RequestParser()
        
parser.add_argument('ticker', type=str)
parser.add_argument('start', type=str)
parser.add_argument('end', type=str)
parser.add_argument('interval', type=str)

class History(Resource):
    def post(self):
        
        args = parser.parse_args()
        print(args)
        ticker = args['ticker']
        start = args['start']
        end = args['end']
        interval = args['interval']

        start = datetime.strptime(str(start), "%Y-%m-%d")
        end = datetime.strptime(str(end), "%Y-%m-%d")

        data = yf.download(
            ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            show_errors=True,
            ignore_tz=True,
        )

        columns = {
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adjClose',
            'Volume': 'volume'
        }
        data.rename(columns=columns, inplace=True)

        data.index = pd.to_datetime(data.index, utc=True)
        if interval == '1d':
            data.index = data.index.strftime('%Y-%m-%d')
        elif interval == '15m':
            data.index = data.index.strftime('%Y-%m-%dT%H:%M')

        return data.to_dict(orient='index'), 200

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
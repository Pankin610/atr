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

class UsIndices(Resource):
    def get(self):
        url = (
            "https://www.wsj.com/market-data/stocks?id=%7B%22application%22%3A%22WSJ%22%2C%22instruments%22%3A%5B%7B"
            "%22symbol%22%3A%22INDEX%2FUS%2F%2FDJIA%22%2C%22name%22%3A%22DJIA%22%7D%2C%7B%22symbol%22%3A%22INDEX%2FUS%2F"
            "%2FCOMP%22%2C%22name%22%3A%22Nasdaq%20Composite%22%7D%2C%7B%22symbol%22%3A%22INDEX%2FUS%2F%2FSPX%22%2C%22name"
            "%22%3A%22S%26P%20500%22%7D%2C%7B%22symbol%22%3A%22INDEX%2FUS%2F%2FDWCF%22%2C%22name%22%3A%22DJ%20Total%20Stock"
            "%20Market%22%7D%2C%7B%22symbol%22%3A%22INDEX%2FUS%2F%2FRUT%22%2C%22name%22%3A%22Russell%202000%22%7D%2C%7B"
            "%22symbol%22%3A%22INDEX%2FUS%2F%2FNYA%22%2C%22name%22%3A%22NYSE%20Composite%22%7D%2C%7B%22symbol%22%3A%22INDEX"
            "%2FUS%2F%2FB400%22%2C%22name%22%3A%22Barron%27s%20400%22%7D%2C%7B%22symbol%22%3A%22INDEX%2FUS%2F%2FVIX%22%2C%22"
            "name%22%3A%22CBOE%20Volatility%22%7D%2C%7B%22symbol%22%3A%22FUTURE%2FUS%2F%2FDJIA%20FUTURES%22%2C%22name%22%3A%"
            "22DJIA%20Futures%22%7D%2C%7B%22symbol%22%3A%22FUTURE%2FUS%2F%2FS%26P%20500%20FUTURES%22%2C%22name%22%3A%22S%26P"
            "%20500%20Futures%22%7D%5D%7D&type=mdc_quotes"
        )
        try:
            response = request(
                url,
               # headers={"User-Agent": get_user_agent()},
            )
        except requests.exceptions.RequestException:
            print("Could not retrieve data from wsj.")
            return
        data = response.json()

        name, last_price, net_change, percent_change = [], [], [], []

        for entry in data["data"]["instruments"]:
            name.append(entry["formattedName"])
            last_price.append(entry["lastPrice"])
            net_change.append(entry["priceChange"])
            percent_change.append(entry["percentChange"])

        indices = pd.DataFrame(
            {" ": name, "Price": last_price, "Chg": net_change, "%Chg": percent_change}
        )

        return indices.to_json(), 200

api.add_resource(Gainers, '/gainers')
api.add_resource(UsIndices, '/usindices')


if __name__ == '__main__':
    app.run(port=5002)
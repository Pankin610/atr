from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from threading import Lock
import financedatabase as fd
from helpers import *


class History(Resource):
    lock = Lock()
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('tickers', type=list, location='json')
        self.parser.add_argument('start', type=str)
        self.parser.add_argument('end', type=str)
        self.parser.add_argument('interval', type=str)

    def post(self):
        args = self.parser.parse_args()
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

        with self.lock:
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

class Search(Resource):
    parser = reqparse.RequestParser()

    data_frames = {
        'Equities': fd.Equities().select(),
        'ETFs': fd.ETFs().select(),
        'Funds': fd.Funds().select(),
        'Currencies': fd.Currencies().select(),
        'Cryptos': fd.Cryptos().select(),
        'Indices': fd.Indices().select(),
        'Money Markets': fd.Moneymarkets().select()
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def __get_row_score(self, row, value):
        def match(value, column_name):
            try:
                text = row[column_name].lower()
                pattern = value.lower()
                penalty = (len(text) - len(pattern)) / len(text)
                if text.startswith(pattern):
                    return 2 - penalty
                if text in pattern:
                    return 1 - penalty
                return 0
            except:
                return -1

        score = 0
        score += 0.5 * match(value, 'symbol')
        score += 0.5 * match(value, 'name')
        score += 0.2 * match(value, 'summary')
        score += 0.1 * match('usd', 'currency')
        return score

    def __get_best_matches(self, df, query, limit=3):
        df = df.reset_index()
        df = df[df['symbol'].notna() & df['name'].notna()]
        query = query.lower()
        if len(query) <= 2:
            df = df[df['symbol'].str.fullmatch(query, case=False)]
        else:
            df = df[
                df['symbol'].str.contains(query, regex=False, case=False) |
                df['name'].str.contains(query, regex=False, case=False)
            ]
            df['score'] = df.apply(self.__get_row_score, args=(query,), axis=1)
            df = df.sort_values('score', ascending=False)
        return df.head(limit)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']

        result = {}
        for name, df in self.data_frames.items():
            matches = self.__get_best_matches(df, query).fillna(
                value='').to_dict(orient='records')
            if matches:
                result[name] = matches
        return result


if __name__ == '__main__':
    app = Flask(__name__)
    CORS(app, resources={r'/*': {'origins': 'http://localhost:3000'}})
    api = Api(app)
    api.add_resource(Search, '/search')
    api.add_resource(History, '/history')
    api.add_resource(Gainers, '/gainers')
    app.run(port=5002)

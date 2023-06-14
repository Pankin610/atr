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

        column_mapping = {
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adjClose',
            'Volume': 'volume'
        }
        df.rename(columns=column_mapping, inplace=True)

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


class Movers(Resource):
    pattern = re.compile(r'(-)?([0-9]+)(\.[0-9]+)?([MBT])?')

    def __parse_numeric(self, text):
        if not isinstance(text, str):
            return text

        match = self.pattern.search(text)

        if not match:
            return None

        minus = match.group(1)
        number = match.group(2)
        decimal = match.group(3)
        unit = match.group(4)

        number = float(number) + float(decimal or 0)

        match unit:
            case 'M':
                number *= 1e6
            case 'B':
                number *= 1e9
            case 'T':
                number *= 1e12

        if minus:
            number *= -1

        if not decimal or unit:
            return int(number)
        return number

    def __process_df(self, df):
        column_mapping = {
            'Symbol': 'symbol',
            'Name': 'name',
            'Price (Intraday)': 'priceIntraday',
            'Change': 'change',
            '% Change': 'percentChange',
            'Volume': 'volume',
            'Avg Vol (3 month)': 'avgVol3Month',
            'Market Cap': 'marketCap',
            'PE Ratio (TTM)': 'peRatioTTM'
        }
        df.rename(columns=column_mapping, inplace=True)

        for column in ['percentChange', 'volume', 'avgVol3Month', 'marketCap']:
            df[column] = df[column].apply(self.__parse_numeric)

        return df.dropna(axis="columns", how="all").replace(float("NaN"), None).to_dict(orient="records")

    def get(self):
        df_gainers = get_df(
            "https://finance.yahoo.com/screener/predefined/day_gainers"
        )[0]
        df_losers = get_df(
            "https://finance.yahoo.com/screener/predefined/day_losers"
        )[0]
        return {
            "gainers": self.__process_df(df_gainers),
            "losers": self.__process_df(df_losers)
        }, 200


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
    api.add_resource(Movers, '/movers')
    app.run(port=5002)

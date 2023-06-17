from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from threading import Lock
import financedatabase as fd
from helpers import *
from yahooquery import Ticker


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


class EquityDetails(Resource):
    parser = reqparse.RequestParser()
    equities = fd.Equities().select().reset_index()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']
        result = self.equities[self.equities['symbol'] == query]
        result = result.drop(columns=['cusip', 'figi', 'composite_figi', 'shareclass_figi', 'isin'])
        return result.to_dict(orient='records')


class EquityNews(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']
        ticker_yf = yf.Ticker(query)
        return ticker_yf.news


class BasicPriceInfo(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']
        ticker = Ticker(query)
        price = ticker.price[query]
        basic_price_info = ticker.summary_detail[query]
        basic_price_info['price'] = price['regularMarketPrice']
        if 'exDividendDate' in basic_price_info:
            basic_price_info['exDividendDate'] = basic_price_info['exDividendDate'].split()[0]
        else:
            basic_price_info['exDividendDate'] = None
        return basic_price_info


class EquityKeyStats(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']
        ticker = Ticker(query)
        equity_key_stats = ticker.key_stats[query]
        price = ticker.price[query]['regularMarketPrice']
        equity_key_stats['priceToEarnings'] = round(equity_key_stats['trailingEps'] / price, 2)
        if 'priceToBook' not in equity_key_stats and 'bookValue' in equity_key_stats:
            equity_key_stats['priceToBook'] = round(price / equity_key_stats['bookValue'], 2)
        return equity_key_stats


class EquityEarningsInfo(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('query', type=str)

    def post(self):
        args = self.parser.parse_args()
        query = args['query']
        ticker = Ticker(query)
        result = {}
        result['earningsDate'] = ticker.calendar_events[query]['earnings']['earningsDate']
        result['earningsDate'] = [date.split()[0] for date in result['earningsDate']]
        result['epsForward'] = ticker.earnings_trend[query]['trend'][0]['earningsEstimate']['avg']
        result['peForward'] = round(ticker.price[query]['regularMarketPrice'] / result['epsForward'], 2)
        result['yearAgoEps'] = ticker.earnings_trend[query]['trend'][0]['earningsEstimate']['yearAgoEps']
        return result


if __name__ == '__main__':
    app = Flask(__name__)
    CORS(app, resources={r'/*': {'origins': 'http://localhost:3000'}})
    api = Api(app)
    api.add_resource(Search, '/search')
    api.add_resource(History, '/history')
    api.add_resource(Gainers, '/gainers')
    api.add_resource(EquityDetails, '/equity-details')
    api.add_resource(EquityNews, '/equity-news')
    api.add_resource(BasicPriceInfo, '/basic-price-info')
    api.add_resource(EquityKeyStats, '/equity-key-stats')
    api.add_resource(EquityEarningsInfo, '/equity-earnings-info')
    app.run(port=5002)

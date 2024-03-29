from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
import yfinance as yf
from threading import Lock
import financedatabase_loader as fdb_loader
from helpers import *
from yahooquery import Ticker
from urllib.parse import quote
import feedparser
import time
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from datetime import datetime
import numpy as np


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
    pattern = re.compile(r'(-)?([0-9]+)(\.[0-9]+)?([MBT%])?')

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
            case '%':
                number *= 1e-2

        if minus:
            number *= -1

        if not decimal or unit in ['M', 'B', 'T']:
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



class Search(Resource):
    parser = reqparse.RequestParser()

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
        for name, df in fdb_loader.get_data_frames().items():
            matches = self.__get_best_matches(df, query).fillna(
                value='').to_dict(orient='records')
            if matches:
                result[name] = matches
        return result


class TickerDetails(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('tickers', type=list, location='json')

    def post(self):
        args = self.parser.parse_args()
        tickers = args['tickers']

        prices = {}
        try:
            prices = Ticker(tickers).price
        except:
            pass

        def get_previous_close(symbol: str):
            try:
                return prices[symbol]['regularMarketPreviousClose']
            except:
                return None

        def get_percent_change(symbol: str):
            try:
                return prices[symbol]['regularMarketChangePercent']
            except:
                return None

        def keep_columns(df, columns):
            return df.drop(columns=set(df.columns) - set(columns))

        result = []
        for df in fdb_loader.get_data_frames().values():
            df = df[df['symbol'].notna() & df['name'].notna()]
            df = df[df['symbol'].isin(tickers)]
            df['previousClose'] = df['symbol'].apply(get_previous_close)
            df['percentChange'] = df['symbol'].apply(get_percent_change)
            df = keep_columns(df, ['symbol', 'name', 'previousClose', 'percentChange'])
            records = df.to_dict(orient='records')
            result.extend(records)
        return result


class Dividends(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('symbol', type=str)

    def get(self):
        args = self.parser.parse_args()
        symbol = args['symbol']
        stock = yf.Ticker(symbol)
        dividends = stock.dividends

        last_dividend_date = dividends.index[-1]
        dividend_per_share = dividends[-1]

        ttm_yield = stock.dividends[-4:].sum() / stock.history(period="1y")["Close"].mean() * 100

        start_date = dividends.index[-5]
        end_date = dividends.index[-1]

        dividend_growth_rate = ((dividends[end_date] / dividends[start_date]) ** (1/5) - 1) * 100

        res = pd.DataFrame(
            {"last_dividend_date": last_dividend_date, "dividend_per_share": dividend_per_share, "ttm_yield": ttm_yield,
            "dividend_growth_rate": dividend_growth_rate}
        )

        return res.to_json(), 200



class EquityDetails(Resource):
    parser = reqparse.RequestParser()
    equities = fdb_loader.get_data_frames()['Equities']

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


class SearchNews(Resource):
    parser = reqparse.RequestParser()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('term', type=str)
        self.parser.add_argument('when', type=int)
        self.parser.add_argument('limit', type=int)

    def post(self):
        args = self.parser.parse_args()
        term = args['term']
        when = args['when']
        limit = args['limit']
        if not term:
            return {}, 400
        retries = 10
        got_data = False
        term = quote(term)
        while not got_data:
            data = feedparser.parse(
                f'https://news.google.com/rss/search?q={term}&hl=en-US&gl=US&ceid=US:en&when:{when}h+allinurl')
            if hasattr(data, 'status') and data.status == 200 and data.entries:
                got_data = True
            elif (not hasattr(data, 'status')) or data.status != 200:
                return {}, 502
            elif retries == 0:
                return {}, 504
            retries -= 1
        df = pd.DataFrame(data.entries, columns=['title', 'link', 'published', 'source'])
        df['source'] = df['source'].apply(lambda x: x['title'])
        df['title'] = df['title'].apply(lambda x: x.rsplit('-', 1)[0].strip())
        df['title'] = df['title'].apply(lambda x: x.rsplit('|', 1)[0].strip())
        df['published'] = pd.to_datetime(df['published'])
        try:
            timezone_name = time.tzname[0]
            df['published'] = df['published'].dt.tz_convert(timezone_name)
        except:
            pass
        df = df.sort_values(by=['published'], ascending=False)
        df['published'] = df['published'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df = df[:limit]
        df = df[['title', 'link', 'source', 'published']]
        df.columns = ['title', 'link', 'publisher', 'date']
        return df.to_dict(orient='records'), 200

#Allowed statement values: balance-sheet, financials, cash-flow
class GetFinancialStatement(Resource):
    parser = reqparse.RequestParser()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('ticker', type=str)
        self.parser.add_argument('statement', type=str)
    def post(self):
        def format_value(value):
            if pd.isnull(value):
                return '-'
            elif value >= 1e9:
                return f'{value/1e9:.2f} BLN'
            elif value >= 1e6:
                return f'{value/1e6:.2f} M'
            else:
                return str(value)
        args = self.parser.parse_args()
        ticker = args['ticker']
        statement = args['statement']
        url = (
        "https://uk.finance.yahoo.com/quote/"
        + ticker
        + "/"
        + statement
        + "?p="
        + ticker
    )
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        webpage = urlopen(req).read()
        soup = BeautifulSoup(webpage, "html.parser")
        features = soup.find_all("div", class_="D(tbr)")
        headers = []
        temp_list = []
        final = []
        if len(features) == 0:
            return {}, 502
        index = 0
        for item in features[0].find_all("div", class_="D(ib)"):
            headers.append(item.text)
        while index <= len(features) - 1:
            temp = features[index].find_all("div", class_="D(tbc)")
            for line in temp:
                temp_list.append(line.text)
            final.append(temp_list)
            temp_list = []
            index += 1

        df = pd.DataFrame(final[1:])
        if df.empty:
            return pd.DataFrame()
        new_headers = []

        if statement == "balance-sheet":
            for dates in headers[1:]:
                read = datetime.strptime(dates, "%d/%m/%Y")
                write = read.strftime("%Y-%m-%d")
                new_headers.append(write)
            new_headers[:0] = ["Breakdown"]
            df.columns = new_headers
            df.set_index("Breakdown", inplace=True)
        elif statement == "financials":
            for dates in headers[2:]:
                read = datetime.strptime(dates, "%d/%m/%Y")
                write = read.strftime("%Y-%m-%d")
                new_headers.append(write)
            new_headers[:0] = ["Breakdown", "ttm"]
            df.columns = new_headers
            df.set_index("Breakdown", inplace=True)
        elif statement == "cash-flow":
            for dates in headers[2:]:
                read = datetime.strptime(dates, "%d/%m/%Y")
                write = read.strftime("%Y-%m-%d")
                new_headers.append(write)
            new_headers[:0] = ["Breakdown", "ttm"]
            df.columns = new_headers
            df.set_index("Breakdown", inplace=True)

        df.replace("", np.nan, inplace=True)
        df.replace("-", np.nan, inplace=True)
        df = df.dropna(how="all")
        df = df.replace(",", "", regex=True)
        df = df.replace("k", "", regex=True)
        df = df.astype("float")
        df.index = df.index.str.replace(",", "")

        not_skipped = ~df.reset_index()["Breakdown"].str.contains("EPS", case=False)
        skipped = df.reset_index()["Breakdown"].str.contains("EPS", case=False)
        skipped_df = df.reset_index()[skipped].set_index("Breakdown")
        transformed = df.reset_index()[not_skipped].set_index("Breakdown") * 1000
        df = pd.concat([transformed, skipped_df])

        df = df.applymap(format_value)
        return df.to_dict(orient='index')

if __name__ == '__main__':
    app = Flask(__name__)
    CORS(app, resources={r'/*': {'origins': 'http://localhost:3000'}})
    api = Api(app)
    api.add_resource(Search, '/search')
    api.add_resource(History, '/history')
    api.add_resource(Movers, '/movers')
    api.add_resource(TickerDetails, '/details')
    api.add_resource(EquityDetails, '/equity-details')
    api.add_resource(EquityNews, '/equity-news')
    api.add_resource(BasicPriceInfo, '/basic-price-info')
    api.add_resource(EquityKeyStats, '/equity-key-stats')
    api.add_resource(EquityEarningsInfo, '/equity-earnings-info')
    api.add_resource(UsIndices, '/usindices')
    api.add_resource(Dividends, '/dividends')
    api.add_resource(SearchNews, '/search-news')
    api.add_resource(GetFinancialStatement, '/get-financial-statement')
    app.run(port=5002)

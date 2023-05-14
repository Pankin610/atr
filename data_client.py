from bar import *
import yfinance as yf
import pandas as pd
import sys
import requests
from datetime import datetime, timedelta

class ApiUnavailableException(Exception):
    pass

# interface
class DataClient:
    def get_recent_bars(self, ticker, days):
        pass

    def get_bars(self, ticker, start_date, end_date, timeframe='D'):
        pass


class YfinanceClient(DataClient):
    def YfinanceDecorator(func):
        def wrapper(*args, **kwargs):
            old_stdout = sys.stdout
            try:
                with open("log.txt", "w") as temp_output:
                    sys.stdout = temp_output
                    return func(*args, **kwargs)
            finally:
                sys.stdout = old_stdout
        return wrapper
    yf.download = YfinanceDecorator(yf.download)

    def yf_data_to_bars(self, data, timeframe='D'):
        if len(data) == 0:
            return []
        data['DateVal'] = data.index.values
        dates = data['DateVal'].dt.strftime('%Y/%m/%d')
        lows, highs = data['Low'].resample(timeframe).min(), data['High'].resample(timeframe).max()
        start_dates = dates.resample(timeframe).min()
        end_dates = dates.resample(timeframe).max()

        good_indices = [i for i in range(len(lows)) if not math.isnan(lows[i])]
        lows = [lows[i] for i in good_indices]
        highs = [highs[i] for i in good_indices]
        start_dates = [start_dates[i] for i in good_indices]
        end_dates = [end_dates[i] for i in good_indices]

        bars = [Bar(low=lows[i], high=highs[i], start_date=start_dates[i], end_date=end_dates[i]) for i in range(len(lows))]
        return bars

    def get_recent_bars(self, ticker, days):
        ticker = yf.Ticker(ticker)
        data = ticker.history(period='%dd' % days)
        if len(data) == 0 and days > 0:
            raise ApiUnavailableException('Yfinance unavailable')
        return self.yf_data_to_bars(data)

    def get_bars(self, ticker, start_date, end_date, timeframe='D'):
        data = pd.concat([
            yf.download(ticker, start_date, end_date, progress=False),
            yf.download(ticker, start=end_date - timedelta(days=7), end=end_date, interval='1m', progress=False)])
        if len(data) == 0 and start_date < end_date:
            raise ApiUnavailableException('Yfinance unavailable')
        return self.yf_data_to_bars(data, timeframe)


class PolygonClient(DataClient):
    def __init__(self):
        self.api_token = 'NU09R3tTbn8YT1GuYBSZR4Mij6bG2wAi'

    def get_api_url(self, ticker, start_date, end_date):
        return 'https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&apiKey=%s' % \
            (ticker, start_date.date().isoformat(), end_date.date().isoformat(), self.api_token)

    def response_to_bars(self, response, start_date):
        if not ('results' in response):
            raise ApiUnavailableException('Polygon API unavailable')

        bars = []
        cur_date = start_date
        for result in response['results']:
            bars.append(Bar(result['l'], result['h'], cur_date, cur_date))
            cur_date = cur_date + timedelta(days=1)
        return bars

    def get_bars(self, ticker, start_date, end_date, timeframe='D'):
        if timeframe != 'D':
            raise NotImplementedError()

        api_url = self.get_api_url(ticker, start_date, end_date)
        response = requests.get(api_url).json()
        return self.response_to_bars(response, end_date)


    def get_recent_bars(self, ticker, days):
        cur_date = datetime.today()
        start_date = cur_date - timedelta(days=7 + days)
        api_url = self.get_api_url(ticker, start_date, cur_date)
        response = requests.get(api_url).json()
        return self.response_to_bars(response, cur_date)[-days:]


class MixedDataClient(DataClient):
    def __init__(self):
        self.yfinance = YfinanceClient()
        self.polygon = PolygonClient()

    def api_call(self, func_list, *args, **kwargs):
        for func in func_list:
            try:
                return func(*args, **kwargs)
            except:
                pass
        raise ApiUnavailableException('No available API')

    def get_recent_bars(self, *args, **kwargs):
        return self.api_call([self.yfinance.get_recent_bars, self.polygon.get_recent_bars], *args, **kwargs)
        
    def get_bars(self, *args, **kwargs):
        return self.api_call([self.yfinance.get_bars, self.polygon.get_bars], *args, **kwargs)
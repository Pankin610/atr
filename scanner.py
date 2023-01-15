import pandas as pd
from yahoo_fin import stock_info
import yfinance as yf
from datetime import datetime, timedelta
import random
import threading
import time
import warnings
from pandas.errors import PerformanceWarning

warnings.simplefilter(action='ignore', category=PerformanceWarning)


def get_all_stocks():
    return stock_info.tickers_nasdaq()


class Scanner:
    def __init__(self):
        self.all_stocks = get_all_stocks()
        self.stock_batches = []
        self.batch_size = 50
        self.stock_data = pd.DataFrame()
        self.day_len = 3
        self.data_mutex = threading.Lock()
        self.update_data_loop = threading.Thread(target=self.update_stocks_loop, daemon=True)
        self.update_data_loop.start()

    def get_rising_stocks(self):
        self.data_mutex.acquire()
        try:
            return [stock for stock in self.all_stocks if self.is_rising_stock(stock)]
        finally:
            self.data_mutex.release()

    def is_rising_stock(self, stock):
        if not (stock in self.stock_data):
            return False
        highs = list(self.stock_data[stock].values)
        return highs == sorted(highs)

    def update_stocks_loop(self):
        while True:
            if not self.stock_batches:
                self.stock_batches = self.all_stocks
                random.shuffle(self.stock_batches)
            batch = self.stock_batches[:self.batch_size]
            self.stock_batches = self.stock_batches[self.batch_size:]

            self.update_stock_batch(batch)

    def update_stock_batch(self, batch):
        data = self.get_stock_data_for_trend(batch)['High']

        self.data_mutex.acquire()
        try:
            if len(self.stock_data) == 0:
                self.stock_data = data
            else:
                self.stock_data[batch] = data
        finally:
            self.data_mutex.release()

    def get_stock_data_for_trend(self, stocks):
        cur_date = datetime.today()
        data = yf.download(stocks, start=cur_date - timedelta(days=4 + self.day_len), progress=False)
        return data[-self.day_len:]


scanner = Scanner()
time.sleep(1)
print(scanner.get_rising_stocks())

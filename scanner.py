import pandas as pd
from yahoo_fin import stock_info
import yfinance as yf
from datetime import datetime, timedelta
import random
import threading
import time
import warnings
from pandas.errors import PerformanceWarning
from bar import *

warnings.simplefilter(action='ignore', category=PerformanceWarning)


def get_all_stocks():
    return stock_info.tickers_nasdaq()


class Scanner:
    def __init__(self):
        self.all_stocks = get_all_stocks()
        self.stock_batches = []
        self.batch_size = 50
        self.stock_data = {}
        self.day_len = 3
        self.data_mutex = threading.Lock()
        self.terminate_event = threading.Event()
        self.update_data_loop = threading.Thread(target=self.update_stocks_loop, daemon=True)
        self.update_data_loop.start()

    def stop_update_loop(self):
        self.terminate_event.set()
        self.update_data_loop.join()

    def __del__(self):
        self.stop_update_loop()
    
    def hold_updates(self):
        self.data_mutex.acquire()

    def continue_updates(self):
        if self.data_mutex.locked():
            self.data_mutex.release()

    def get_filtered_stocks(self, filter):
        with self.data_mutex:
            return [stock for stock in self.all_stocks if filter(stock)]

    def is_rising_stock(self, stock):
        if not (stock in self.stock_data):
            return False
        highs = [bar.high for bar in self.stock_data[stock]]
        return highs == sorted(highs)

    def is_falling_stock(self, stock):
        if not (stock in self.stock_data):
            return False
        highs = [bar.high for bar in self.stock_data[stock]]
        return highs == list(reversed(sorted(highs)))

    def get_rising_stocks(self):
        return self.get_filtered_stocks(self.is_rising_stock)

    def get_falling_stocks(self):
        return self.get_filtered_stocks(self.is_falling_stock)

    def update_stocks_loop(self):
        while not self.terminate_event.is_set():
            if not self.stock_batches:
                self.stock_batches = self.all_stocks
                random.shuffle(self.stock_batches)
            batch = self.stock_batches[:self.batch_size]
            self.stock_batches = self.stock_batches[self.batch_size:]

            self.update_stock_batch(batch)

    def update_stock_batch(self, batch):
        with self.data_mutex:
            for stock in batch:
                self.stock_data[stock] = self.get_stock_data_for_trend(stock)

    def get_stock_data_for_trend(self, stock):
        return get_recent_bars(stock, self.day_len)


if __name__ == '__main__':
    scanner = Scanner()
    time.sleep(10)
    print(scanner.get_falling_stocks())

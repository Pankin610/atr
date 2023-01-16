import math

from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd
import warnings
from bar import *
from data_client import *

from pandas.errors import SettingWithCopyWarning

warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)

ATR_WINDOW = 5
        
def get_bar_chunk(instrument, end_date, data_client, timeframe='D'):
    start_date = end_date - timedelta(days=(2 * ATR_WINDOW) * 365 + 5)
    return data_client.get_bars(instrument, start_date, end_date, timeframe)


def is_anomaly_bar(bar, atr):
    return bar_atr(bar) >= 2.0 * atr or bar_atr(bar) <= atr / 3.0


def average_bar_atr(bars):
    return sum(bar_atr(bar) for bar in bars) / len(bars)
    

class ATR:
    def __init__(self, current_bar, good_bars, anomaly_bars=None):
        if not anomaly_bars:
            anomaly_bars = None
        self.current_bar = current_bar
        self.value = average_bar_atr(good_bars)
        self.good_bars = good_bars
        self.anomaly_bars = anomaly_bars

    def __str__(self):
        return 'Final ATR value: %.2f\n' \
               'Current bar: \n%s' \
               'Current bar %% of ATR: %.2f%%\n\n' \
               'Bars accounted: \n%s\n ' \
               'Bars discarded: %s\n' % \
               (self.value,
                str(self.current_bar),
                bar_atr(self.current_bar) / self.value * 100.,
                bars_to_str(self.good_bars),
                bars_to_str(self.anomaly_bars))


def get_current_atr(instrument, bar_timeframe, data_client):
    cur_date = datetime.today()
    bars_queue = get_bar_chunk(instrument, cur_date, data_client, bar_timeframe)
    current_bar = bars_queue[-1]
    bars_queue = bars_queue[:-1]

    good_bars = []
    skipped_bars = []

    while True:
        while len(good_bars) < ATR_WINDOW:
            good_bars.append(bars_queue[-1])
            bars_queue = bars_queue[:-1]

        atr = average_bar_atr(good_bars)
        skipped_bars += [bar for bar in good_bars if is_anomaly_bar(bar, atr)]
        good_bars = [bar for bar in good_bars if not is_anomaly_bar(bar, atr)]
        if len(good_bars) == ATR_WINDOW:
            break
    return ATR(current_bar, good_bars, skipped_bars)


if __name__ == "__main__":
    print("Please enter the instrument name (ex: AAPL)")
    input_instrument = input()
    print("Please enter timeframe (ex: D, W, Y)")
    atr_timeframe = input()
    print(get_current_atr(input_instrument, atr_timeframe, YfinanceClient()))

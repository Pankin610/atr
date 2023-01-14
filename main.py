import math

import yfinance as yf
from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd
import warnings

from pandas.errors import SettingWithCopyWarning

warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)

ATR_WINDOW = 5


@dataclass
class Bar:
    low: float
    high: float
    start_date: datetime
    end_date: datetime

    def __str__(self):
        return " Bar low: %.2f\n" \
               " Bar high: %.2f\n" \
               " Bar start date: %s\n" \
               " Bar end date: %s\n" \
               " Bar ATR: %.2f\n\n" % \
               (self.low, self.high, self.start_date, self.end_date, bar_atr(self))


def get_data_chunk(instrument, end_date):
    start_date = end_date - timedelta(days=(2 * ATR_WINDOW) * 365 + 5)
    return pd.concat([
        yf.download(instrument, start_date, end_date),
        yf.download(instrument, start=end_date - timedelta(days=7), end=end_date, interval='1m')])


def get_bar_chunk(instrument, end_date, timeframe='D'):
    data = get_data_chunk(instrument, end_date)[['High', 'Low']]
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


def bar_atr(bar):
    return bar.high - bar.low


def is_anomaly_bar(bar, atr):
    return bar_atr(bar) >= 2.0 * atr or bar_atr(bar) <= atr / 3.0


def average_bar_atr(bars):
    return sum(bar_atr(bar) for bar in bars) / len(bars)


def bars_to_str(bars):
    if bars is None or not bars:
        return str(None)

    return '\n'.join(str(bar) for bar in bars)


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


def get_current_atr(instrument, bar_timeframe):
    cur_date = datetime.today()
    bars_queue = get_bar_chunk(instrument, cur_date, bar_timeframe)
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
    print(get_current_atr(input_instrument, atr_timeframe))

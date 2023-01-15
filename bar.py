from datetime import datetime, timedelta
from dataclasses import dataclass
import yfinance as yf
import pandas as pd
import math

def bar_atr(bar):
    return bar.high - bar.low

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


def bars_to_str(bars):
    if bars is None or not bars:
        return str(None)

    return '\n'.join(str(bar) for bar in bars)


def yf_data_to_bars(data, timeframe):
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


def get_bars(ticker, start_date, end_date, timeframe='D'):
    data = pd.concat([
        yf.download(ticker, start_date, end_date),
        yf.download(ticker, start=end_date - timedelta(days=7), end=end_date, interval='1m')])
    return yf_data_to_bars(data, timeframe)


def get_recent_bars(ticker, days):
    data = yf.download(ticker, datetime.today() - timedelta(days=4 + days))
    return yf_data_to_bars(data[-days:], 'D')
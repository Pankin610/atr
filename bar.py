from datetime import datetime, timedelta
from dataclasses import dataclass
import yfinance as yf
import pandas as pd
import math
import sys
import pandas_datareader as pdr



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
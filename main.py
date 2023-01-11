import yfinance as yf
from datetime import datetime, timedelta
from dataclasses import dataclass

ATR_WINDOW = 5

@dataclass
class Bar:
    low: float
    high: float
    start_date: datetime
    end_date: datetime


def get_data_chunk(instrument, end_date):
    start_date = end_date - timedelta(days=ATR_WINDOW * 2)
    return yf.download(instrument, start_date, end_date)


def get_bar_chunk(instrument, end_date):
    data = get_data_chunk(instrument, end_date)
    data.reset_index(inplace=True)
    data['Date'] = data['Date'].dt.strftime('%Y/%m/%d')

    lows, highs, dates = data['Low'], data['High'], data['Date']
    bars = [Bar(low=lows[i], high=highs[i], start_date=dates[i], end_date=dates[i]) for i in range(len(lows))]
    return bars


def bar_atr(bar):
    return bar.high - bar.low


def is_anomaly_bar(bar, atr):
    return bar_atr(bar) >= 2.0 * atr or bar_atr(bar) <= atr / 3.0


def average_bar_atr(bars):
    return sum(bar_atr(bar) for bar in bars) / len(bars)


class ATR:
    def __init__(self, good_bars, anomaly_bars=None):
        if not anomaly_bars:
            anomaly_bars = None
        self.value = average_bar_atr(good_bars)
        self.good_bars = good_bars
        self.anomaly_bars = anomaly_bars

    def __str__(self):
        return 'Final ATR value: %f\n Bars accounted: %s\n Bars discarded: %s\n' % \
               (self.value, str(self.good_bars), str(self.anomaly_bars))


def get_current_atr(instrument):
    cur_date = datetime.today()
    bars_queue = []
    good_bars = []
    skipped_bars = []

    while True:
        while len(good_bars) < ATR_WINDOW:
            if len(bars_queue) == 0:
                bars_queue = get_bar_chunk(instrument, cur_date)
                cur_date = cur_date - timedelta(days=2 * ATR_WINDOW)
            good_bars.append(bars_queue[-1])
            bars_queue = bars_queue[:-1]

        atr = average_bar_atr(good_bars)
        skipped_bars += [bar for bar in good_bars if is_anomaly_bar(bar, atr)]
        good_bars = [bar for bar in good_bars if not is_anomaly_bar(bar, atr)]
        if len(good_bars) == ATR_WINDOW:
            break
    return ATR(good_bars, skipped_bars)


if __name__ == "__main__":
    print("Please enter the instrument name (ex: AAPL)")
    input_instrument = input()
    print(get_current_atr(input_instrument))

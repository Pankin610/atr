import pandas as pd
from yahoo_fin import stock_info
import yfinance as yf
from datetime import datetime, timedelta

stock_symbols = stock_info.tickers_nasdaq()
stock_batches = [stock_symbols[i:i + 50] for i in range(0, len(stock_symbols), 50)]
cur_date = datetime.today()

data = None
for batch in stock_batches:
    batch_data = yf.download(batch, start=cur_date - timedelta(days=4), end=cur_date)['High']
    if data is None:
        data = batch_data
    else:
        data = pd.concat([data, batch_data])

rising_stocks = [stock for stock in stock_symbols if list(data[stock].values) == list(sorted(data[stock].values))]
print("Rising stocks:")
print(rising_stocks)

for stock in rising_stocks:
    print(data[stock].values)

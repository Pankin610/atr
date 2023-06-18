from functools import cache
from typing import Dict, Literal

import financedatabase as fd
import pandas as pd
from loguru import logger

DataFramesDict = Dict[
    Literal[
        'Equities',
        'ETFs',
        'Funds',
        'Currencies',
        'Cryptos',
        'Indices',
        'Money Markets'
    ],
    pd.DataFrame
]


@cache
def __load_data_frames() -> DataFramesDict:
    logger.info("Loading FinanceDatabase data frames")
    dfs = {
        'Equities': fd.Equities().select(),
        'ETFs': fd.ETFs().select(),
        'Funds': fd.Funds().select(),
        'Currencies': fd.Currencies().select(),
        'Cryptos': fd.Cryptos().select(),
        'Indices': fd.Indices().select(),
        'Money Markets': fd.Moneymarkets().select()
    }
    logger.success("Successfully loaded all FinanceDatabase data frames")

    if not 'BTC-USD' in dfs['Cryptos'].index:
        dfs['Cryptos'] = pd.concat([dfs['Cryptos'], pd.DataFrame([{
            'symbol': 'BTC-USD',
            'name': 'Bitcoin USD',
            'cryptocurrency': 'BTC',
            'currency': 'USD',
            'summary': 'Bitcoin (BTC) is a cryptocurrency...',
            'exchange': 'CCC',
            'market': 'ccc_market'
        }])])

    return dfs


def get_data_frames() -> DataFramesDict:
    return {category: df.reset_index() for category, df in __load_data_frames().items()}

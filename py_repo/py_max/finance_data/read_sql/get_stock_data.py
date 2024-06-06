import pandas as pd
import numpy as np
import datetime as dt

from sqlalchemy import text, TextClause

from typing import Optional

from py_max.finance_data.config import (
    StockBase,
    Amazon,
    Apple,
    Meta,
    Paypal,
    Nvidia,
    Google,
    Tesla,
)
from py_max.py_utils import ExecuteQuery, SQLYahooData


class Stock:
    def __init__(
        self, stock_choice: StockBase, day_filter: Optional[dt.datetime] = None
    ) -> None:
        self.stock_choice: StockBase = stock_choice
        self.day_filter: Optional[dt.datetime] = day_filter

        # self.data: pd.DataFrame = self.__get_stock()

    @property
    def data(self) -> pd.DataFrame:
        data: pd.DataFrame = self._data()

        # Removing any nan values on all of the entries here.
        data.dropna(
            how="all",
            subset=[
                SQLYahooData.market_close,
                SQLYahooData.market_high,
                SQLYahooData.market_low,
                SQLYahooData.market_high,
            ],
            inplace=True,
        )
        return data

    @ExecuteQuery()
    def _data(self) -> None:
        """Gets the data for the stock. Uses double underscore so less accessible."""
        query_base: str = f"""SELECT [AsAtDateTime]
                    ,[Security]
                    ,[Currency]
                    ,[MarketLow]
                    ,[MarketHigh]
                    ,[MarketOpen]
                    ,[MarketClose]
                    ,[MarketVolume]
                    ,[InstrumentType]
                    ,[ExchangeName]
                    ,[TimeZone]
                    ,[GmtOffSet]
                FROM [max_dev].[stk].[yahooData]
                WHERE Security = '{self.stock_choice.ticker}'"""

        if self.day_filter is not None:
            raise NotImplementedError

        final_query: TextClause = text(query_base)
        return final_query


if __name__ == "__main__":
    y = Stock(Apple)

    b = 1

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

from sqlalchemy import text, TextClause

from typing import Optional, List

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
    def name(self) -> str:
        return self._name()

    def _name(self) -> str:
        return self.stock_choice.ticker

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

        # Adding sub-division for the dates
        try:
            data[SQLYahooData.date] = data[SQLYahooData.as_at_date].dt.date
        except AttributeError:
            data[SQLYahooData.date] = 0

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
            day_filter_string: str = dt.datetime.strftime(self.day_filter, r"%Y-%m-%d")
            day_filter_plus_one_string: str = dt.datetime.strftime(
                self.day_filter + dt.timedelta(days=1), r"%Y-%m-%d"
            )
            date_filter: str = (
                f"AND AsAtDateTime >= '{day_filter_string}' AND AsAtDateTime < '{day_filter_plus_one_string}'"
            )
            query_base += date_filter

        final_query: TextClause = text(query_base)
        return final_query

    def get_day(self, day: Optional[dt.datetime]) -> pd.DataFrame:
        data: pd.DataFrame = self.data
        if day is None:
            # If none, take the latest date
            day: dt.datetime = data[SQLYahooData.date].max()

        data_filtered: pd.DataFrame = data.loc[
            data[SQLYahooData.date] == day.date()
        ].copy()

        # Ensuring that we are ascending
        data_filtered.sort_values(
            by=SQLYahooData.as_at_date, ascending=True, inplace=True
        )
        return data_filtered

    def plot_day(
        self,
        day_to_plot: Optional[dt.datetime] = None,
        column_choice: Optional[str] = None,
        SHOW_PLOT: bool = False,
    ) -> None:

        data_filtered: pd.DataFrame = self.get_day(day_to_plot)

        # Plotting
        datetimes: List[dt.datetime] = pd.to_datetime(
            data_filtered[SQLYahooData.as_at_date]
        ).to_list()

        if column_choice is None:
            # We take the mid
            plotting_data: List[float] = (
                data_filtered[SQLYahooData.market_high]
                .add(data_filtered[SQLYahooData.market_low])
                .div(2)
                .to_list()
            )
            data_name: str = SQLYahooData.market_mid
        else:
            plotting_data: List[float] = data_filtered[column_choice].to_list()
            data_name: str = column_choice

        plt.plot(datetimes, plotting_data)
        plt.xlabel(SQLYahooData.as_at_date)
        plt.ylabel(data_name)
        plt.legend()

        if SHOW_PLOT:
            plt.show()


if __name__ == "__main__":
    Stock(Apple).plot_day()
    # Stock(Nvidia).plot_day()
    Stock(Google).plot_day(SHOW_PLOT=True)
    b = 1

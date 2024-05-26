from py_max.stock_project.stock_stripper import StockGrabber
import pandas as pd
import datetime as dt
import holidays

from typing import List, Optional, Any, Dict, Iterator
from sqlalchemy.types import Integer, String, DateTime, Float


from py_max.stock_project.config import logger
from py_max.py_utils.sql import SQLYahooData, DatabaseConnector


class DataCapture:
    def __init__(self, valid_stocks: Optional[List[str]] = None):
        if valid_stocks is None:
            # Have default options for the stocks to strip
            self.valid_stocks: List[str] = [
                "AAPL",
                "GOOGL",
                "TSLA",
                "NVDA",
                "AMZN",
                "META",
                "PYPL",
            ]
        else:
            self.valid_stocks: List[str] = valid_stocks
        self.end_date_dt: dt.datetime = dt.datetime.today().replace(
            hour=0, minute=0, microsecond=0, second=0
        )
        self.start_date_dt: dt.datetime = self.end_date_dt - dt.timedelta(days=30)

    def date_generator(self) -> Iterator[dt.datetime]:
        us_holidays: Any = holidays.US()
        time_delta: dt.timedelta = self.end_date_dt - self.start_date_dt
        day_iterable_dt: dt.datetime = self.start_date_dt
        for _ in range(time_delta.days):
            day_iterable_dt += dt.timedelta(days=1)
            if (day_iterable_dt in us_holidays) or (
                day_iterable_dt.weekday() in [5, 6]
            ):
                pass
            else:
                yield day_iterable_dt

    def stock_call(self, ticker: str) -> pd.DataFrame:
        stock_timeseries_df: pd.DataFrame = pd.DataFrame()
        for day_dt in self.date_generator():
            start_time: dt.datetime = day_dt + dt.timedelta(hours=9)
            end_time: dt.datetime = day_dt + dt.timedelta(hours=21)
            stock_df: pd.DataFrame = StockGrabber(
                ticker, start_time, end_time
            ).GetData()
            stock_timeseries_df: pd.DataFrame = pd.concat(
                [stock_timeseries_df, stock_df]
            )
        return stock_timeseries_df

    def insert_to_sql(self, dataframe: pd.DataFrame) -> None:
        dataframe_schema: Dict[str, Any] = {
            SQLYahooData.as_at_date: DateTime,
            SQLYahooData.security: String(255),
            SQLYahooData.currency: String(255),
            SQLYahooData.market_low: Float,
            SQLYahooData.market_high: Float,
            SQLYahooData.market_open: Float,
            SQLYahooData.market_close: Float,
            SQLYahooData.market_volume: Float,
            SQLYahooData.instrument_type: String(255),
            SQLYahooData.exchange_name: String(255),
            SQLYahooData.time_zone: String(255),
            SQLYahooData.gmt_off_set: Integer,
        }

        with DatabaseConnector(DatabaseConnector.LOCAL) as connection:
            dataframe.to_sql(
                SQLYahooData.table_name,
                connection,
                schema=SQLYahooData.schema,
                index=False,
                if_exists="replace",
                dtype=dataframe_schema,
            )

        logger.LogInfo("Successfully written to database.")

    def DataCreation(self, write: bool) -> None:
        all_stock_dataset_df: pd.DataFrame = pd.DataFrame()
        for ticker in self.valid_stocks:
            stock_dataset_df: pd.DataFrame = self.stock_call(ticker)
            logger.LogInfo(f"{ticker} information successfully gathered.")
            all_stock_dataset_df: pd.DataFrame = pd.concat(
                [all_stock_dataset_df, stock_dataset_df]
            )

        if write:
            self.insert_to_sql(all_stock_dataset_df)
        else:
            pass


if __name__ == "__main__":
    start_date = "2022-01-01"
    to_date = "2024-05-22"
    DataCapture().DataCreation(write=True)

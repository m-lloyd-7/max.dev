from py_max.finance_data.upload_to_sql.stock_stripper import StockGrabber
import pandas as pd
import datetime as dt
import holidays
import sqlalchemy as db

from typing import List, Optional, Any, Dict, Iterator
from sqlalchemy.types import Integer, String, DateTime, Float


from py_max.finance_data.config import (
    logger,
    Paypal,
    Amazon,
    Tesla,
    Meta,
    Apple,
    Google,
    Nvidia,
    StockBase,
)
from py_max.py_utils import SQLYahooData, DatabaseConnector, DBChoice, ExecuteQuery


class DataCapture:
    def __init__(self, valid_stocks: Optional[List[StockBase]] = None):
        if valid_stocks is None:
            # Have default options for the stocks to strip
            self.valid_stocks: List[StockBase] = [
                Apple,
                Google,
                Tesla,
                Nvidia,
                Amazon,
                Meta,
                Paypal,
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

        with DatabaseConnector(DBChoice.LOCAL) as connection:
            dataframe.to_sql(
                SQLYahooData.table_name,
                connection,
                schema=SQLYahooData.schema,
                index=False,
                if_exists="append",
                dtype=dataframe_schema,
            )

        logger.LogInfo("Successfully written to database.")

    def DataCreation(self, write: bool) -> None:
        all_stock_dataset_df: pd.DataFrame = pd.DataFrame()
        for stock in self.valid_stocks:
            stock_dataset_df: pd.DataFrame = self.stock_call(stock.ticker)
            logger.LogInfo(f"{stock.ticker} information successfully gathered.")
            all_stock_dataset_df = pd.concat([all_stock_dataset_df, stock_dataset_df])

        if write:
            unique_stocks: List[str] = all_stock_dataset_df[
                SQLYahooData.security
            ].unique()
            minimum_date: dt.datetime = all_stock_dataset_df[
                SQLYahooData.as_at_date
            ].min()
            existing_data: pd.DataFrame = self.get_sql_data(unique_stocks, minimum_date)

            # Taking the existing data and dropping anything in the new data that isn't inscope
            # our distinct keys are the datetime, security and currency
            master_keys: List[str] = [
                SQLYahooData.as_at_date,
                SQLYahooData.security,
                SQLYahooData.currency,
            ]
            existing_data.set_index(master_keys, inplace=True)
            all_stock_dataset_df.set_index(master_keys, inplace=True)

            # Taking the difference on the indices
            index_difference: pd.MultiIndex = all_stock_dataset_df.index.difference(
                existing_data.index
            )
            all_stock_dataset_df = all_stock_dataset_df[
                all_stock_dataset_df.index.isin(index_difference)
            ].copy()
            if len(index_difference) != len(all_stock_dataset_df):
                raise IndexError("Missing some indices somewhere. Debug.")

            # Final removal of null values and resetting of index
            all_stock_dataset_df.reset_index(inplace=True)
            all_stock_dataset_df.dropna(subset=master_keys, how="any", inplace=True)

            self.insert_to_sql(all_stock_dataset_df)
        else:
            pass

    @ExecuteQuery()
    def get_sql_data(
        self, inscope_stocks: List[str], minimum_date: dt.datetime
    ) -> db.TextClause:
        min_date_string: str = dt.datetime.strftime(minimum_date, r"%Y-%m-%d")
        security_string: str = ",".join([f"'{isin}'" for isin in inscope_stocks])
        query: db.TextClause = db.text(
            f"""
            SELECT [AsAtDateTime]
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
            WHERE Security in ({security_string}) AND AsAtDateTime >= '{min_date_string}'"""
        )
        return query


if __name__ == "__main__":
    DataCapture().DataCreation(write=True)

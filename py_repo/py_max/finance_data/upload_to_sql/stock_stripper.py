import requests
import pandas as pd
import numpy as np
import json
import datetime as dt
from typing import List, Tuple, Dict, Any, Union, Set
from py_max.finance_data.config import logger
from py_max.finance_data.static import WebPageStatics
from py_max.py_utils.sql import SQLYahooData


class StockGrabber:
    """
    Class object for reading stock market data from Yahoo finance
    """

    def __init__(
        self, ticker: str, from_date_dt: dt.datetime, to_date_dt: dt.datetime
    ) -> None:
        # Static data initialisation
        self.header: Dict[str, str] = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0"
        }
        self.ticker: str = ticker
        self.from_date_dt: str = from_date_dt
        self.to_date_dt: dt.datetime = to_date_dt
        self.from_date: int = round(dt.datetime.timestamp(self.from_date_dt))
        self.to_date: int = round(dt.datetime.timestamp(self.to_date_dt))
        self.url: str = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{self.ticker}?symbol={self.ticker}&period1={self.from_date}&period2={self.to_date}&useYfid=true&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=azr2X8.O.Sf&corsDomain=finance.yahoo.com"
        )

    def __enter__(self) -> Dict[str, Any]:
        # Request execution
        self.request: requests.Response = requests.get(self.url, headers=self.header)
        self.decode_format: str = "ISO-8859-1"
        self.content: bytes = self.request.content

        self.main_dictionary: Dict[str, Any] = json.loads(
            self.content.decode(self.decode_format)
        )

        return self.main_dictionary

    def __exit__(self, arg1, arg2, arg3):
        self.request.close()

    def __repr__(self):
        return f"Stock price information for {self.ticker} from {self.from_date_dt} to {self.to_date_dt}"

    def data_processing(self) -> pd.DataFrame:
        """
        Takes the main dictionary read from the yahoo website and prepares for conversion to a dataframe.
        """
        with self as main_info:
            try:
                chart_info: Dict[str, Any] = main_info[WebPageStatics.chart]
                result_info: Dict[str, Any] = chart_info[WebPageStatics.result][0]
            except TypeError as _:
                chart_info: Dict[str, Any] = main_info[WebPageStatics.chart]
                result_info: Dict[str, Any] = chart_info[WebPageStatics.result]
            except KeyError as _:
                try:
                    chart_info: Dict[str, Any] = main_info[WebPageStatics.finance]
                    result_info: Dict[str, Any] = chart_info[WebPageStatics.result]
                    logger.LogInfo(
                        f"From {self.from_date_dt} to {self.to_date_dt} 'chart' not valid."
                    )
                except KeyError as _:
                    logger.LogError(
                        f"From {self.from_date_dt} to {self.to_date_dt} 'finance' not valid. Debug"
                    )

        if result_info is None:
            logger.LogCritical(
                f"No data for {self.from_date_dt}. Returning Nan dataframe."
            )
            stock_df: pd.DataFrame = self.nan_dataframe(self.from_date_dt)

        else:
            try:
                meta_data: Dict[str, Any] = result_info[WebPageStatics.meta]

                # Deprecated
                timestamp_info: Dict[str, Any] = result_info[WebPageStatics.timestamp]
                indicator_info: Dict[str, Any] = result_info[WebPageStatics.indicators]

                # Pulling price information
                low, high, open, close, volume = self.indicator_reader(indicator_info)

                # Getting the meta data
                (
                    currency,
                    instrument_type,
                    security,
                    gmt_offset,
                    time_zone,
                    exchange_name,
                ) = self.meta_processing(meta_data)

                # Converting timestamp to datetime
                datetime_info: List[dt.datetime] = [
                    dt.datetime.fromtimestamp(timestamp) for timestamp in timestamp_info
                ]

                # Creation of headers for dataframe
                stock_info_pre_dataframe: Dict[str, List[Union[dt.datetime, float]]] = {
                    SQLYahooData.as_at_date: datetime_info,
                    SQLYahooData.market_low: low,
                    SQLYahooData.market_high: high,
                    SQLYahooData.market_open: open,
                    SQLYahooData.market_close: close,
                    SQLYahooData.market_volume: volume,
                }

                stock_df: pd.DataFrame = pd.DataFrame(stock_info_pre_dataframe)
                stock_df[SQLYahooData.security] = security
                stock_df[SQLYahooData.currency] = currency
                stock_df[SQLYahooData.instrument_type] = instrument_type
                stock_df[SQLYahooData.time_zone] = time_zone
                stock_df[SQLYahooData.exchange_name] = exchange_name
                stock_df[SQLYahooData.gmt_off_set] = gmt_offset
            except KeyError as _:
                logger.LogCritical(
                    f"No data for {self.from_date_dt}. Returning Nan dataframe."
                )
                stock_df = self.nan_dataframe(self.from_date_dt)

        # Columns ordering
        stock_final_df: pd.DataFrame = self.stock_header_organisation(stock_df).copy()

        return stock_final_df

    @staticmethod
    def nan_dataframe(date) -> pd.DataFrame:
        stock_info_pre_dataframe = {
            SQLYahooData.as_at_date: [date],
            SQLYahooData.market_low: [np.nan],
            SQLYahooData.market_high: [np.nan],
            SQLYahooData.market_open: [np.nan],
            SQLYahooData.market_close: [np.nan],
            SQLYahooData.market_volume: [np.nan],
        }
        stock_df: pd.DataFrame = pd.DataFrame(stock_info_pre_dataframe)
        stock_df[SQLYahooData.security] = np.nan
        stock_df[SQLYahooData.currency] = np.nan
        stock_df[SQLYahooData.instrument_type] = np.nan
        stock_df[SQLYahooData.time_zone] = np.nan
        stock_df[SQLYahooData.exchange_name] = np.nan
        stock_df[SQLYahooData.gmt_off_set] = np.nan
        return stock_df

    @staticmethod
    def stock_header_organisation(stock_df: pd.DataFrame) -> pd.DataFrame:
        stock_columns: pd.Index = stock_df.columns
        stock_df: pd.DataFrame = stock_df[
            [
                SQLYahooData.as_at_date,
                stock_columns[6],
                SQLYahooData.currency,
                SQLYahooData.market_low,
                SQLYahooData.market_high,
                SQLYahooData.market_open,
                SQLYahooData.market_close,
                SQLYahooData.market_volume,
                SQLYahooData.instrument_type,
                SQLYahooData.exchange_name,
                SQLYahooData.time_zone,
                SQLYahooData.gmt_off_set,
            ]
        ]
        return stock_df

    @staticmethod
    def length_check(main_data: dict) -> bool:
        dataset_lengths: List[int] = [len(main_data[key]) for key in main_data.keys()]
        lengths: Set[int] = set(dataset_lengths)
        lengths: List[int] = list(lengths)
        if len(lengths) == 1:
            singular_flag: bool = True
        else:
            singular_flag: bool = False

        return singular_flag

    def indicator_reader(
        self, indicator_info: dict
    ) -> Tuple[np.array, np.array, np.array, np.array, np.array]:
        """
        Pulls the pricing information, needs a length check
        """
        main_data = indicator_info[WebPageStatics.quote][0]

        if self.length_check(main_data):
            pass
        else:
            logger.LogError("Dataset has columns of different lengths.")
            raise AttributeError("This dataset has columns of different lengths.")

        market_low: np.array = np.float_(main_data[WebPageStatics.low])
        market_high: np.array = np.float_(main_data[WebPageStatics.high])
        market_open: np.array = np.float_(main_data[WebPageStatics.open])
        market_volume: np.array = np.float_(main_data[WebPageStatics.volume])
        market_close: np.array = np.float_(main_data[WebPageStatics.close])

        return (market_low, market_high, market_open, market_close, market_volume)

    @staticmethod
    def meta_processing(meta_data: dict) -> Tuple[str, str, str, int, str, str]:
        """
        Procceses the meta data.
        """
        try:
            currency = meta_data[WebPageStatics.currency]
            inst_type = meta_data[WebPageStatics.instrument_type]
            first_trade_date = meta_data[WebPageStatics.first_trade_date]
            security_ticker = meta_data[WebPageStatics.ticker]
            gmt_offset = meta_data[WebPageStatics.gmt_off_set]
            time_zone = meta_data[WebPageStatics.time_zone]
            exchange_name = meta_data[WebPageStatics.exchange_name]

        except KeyError:
            logger.LogError("Meta data extraction failed due to KeyError.")

        return (
            currency,
            inst_type,
            security_ticker,
            gmt_offset,
            time_zone,
            exchange_name,
        )

    def GetData(self) -> pd.DataFrame:
        daily_information_df: pd.DataFrame = self.data_processing()
        return daily_information_df


if __name__ == "__main__":
    from_date = dt.datetime(year=2024, day=24, month=5, hour=9)
    to_date = dt.datetime(year=2024, day=24, month=5, hour=17)
    # from_date = round(dt.datetime.timestamp(from_date))
    # to_date = round(dt.datetime.timestamp(to_date))
    ticker = "TSLA"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?symbol={ticker}&period1={from_date}&period2={to_date}&useYfid=true&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=azr2X8.O.Sf&corsDomain=finance.yahoo.com"
    StockGrabber(ticker, from_date, to_date).GetData()

    string = f"The ticker I am using is {ticker}"
    print(string)

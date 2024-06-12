import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

from copy import deepcopy
from typing import Optional, List, Dict, Union
from sklearn.linear_model import LinearRegression
from enum import Enum

from py_max.finance_data import Stock, Amazon, StockBase
from py_max.py_utils import SQLYahooData
from py_max.model_data.config import log

PLOT: bool = False


# Guesses - narrowing window from a regression implies that the volatility is reducing => people will buy lower vol so price will rise
class Regressions(Enum):
    FULL: str = "Full"
    UPPER: str = "Upper"
    LOWER: str = "Lower"


class StrategyColumns:
    Data: str = "Data"


def create_time_indices(time_data: pd.DataFrame) -> pd.DataFrame:
    """Takes a dataframe with a datetime column and converts each minute forward to an index."""
    times: np.ndarray = time_data[SQLYahooData.as_at_date].to_numpy()
    min_time: np.datetime64 = times.min()
    max_time: np.datetime64 = times.max()

    time_indices: List[int] = []
    counter: int = 0
    while min_time <= max_time:
        # Append only if the time is inscope
        if min_time in times:
            time_indices.append(counter)

        # Iterate forward by a minute every time
        min_time += np.timedelta64(1, "m")
        counter += 1

    # Converting these new time indices into the index for the dataframe
    time_data.index = time_indices
    return time_data


def gradient(data: pd.DataFrame) -> float:
    """Gets the gradient of a series-like data frame."""
    y_final_value: float = data.iloc[-1, 0]
    y_first_value: float = data.iloc[0, 0]

    x_final_value: float = data.index[-1]
    x_first_value: float = data.index[0]

    grad: float = (y_final_value - y_first_value) / (x_final_value - x_first_value)
    return grad


def width_variance(data_upper: pd.DataFrame, data_lower: pd.DataFrame) -> float:
    """Gives a measure of whether the trend lines are closing in or not."""
    end_minus_end: float = data_upper.iloc[-1, 0] - data_lower.iloc[-1, 0]
    start_minus_start: float = data_upper.iloc[0, 0] - data_lower.iloc[0, 0]
    return end_minus_end, start_minus_start


class Trade:
    POSITION: float = None
    NET_MARKET_VALUE: float = None
    LAST_TRADED_PRICE: float = None

    def __init__(self, stock: Stock, trade_date: dt.datetime) -> None:
        self.stock: Stock = stock
        self.trade_date: dt.datetime = trade_date

        # Storing the data for that particular day
        self.create_performance_data(self.trade_date)

        # Flag to mark whether portfolio holds security
        self.IN_PORTFOLIO: bool = False

    def create_performance_data(self, trade_date: dt.datetime) -> None:
        # Storing the data for that particular day
        if trade_date != self.trade_date:
            self.stock.day_filter = trade_date  # resetting

        self.performance_data: pd.DataFrame = self.stock.get_day(trade_date)
        self.performance_data[SQLYahooData.market_mid] = (
            self.performance_data[SQLYahooData.market_high]
            .add(self.performance_data[SQLYahooData.market_low])
            .div(2)
        )

        # Adding the time indices to the data
        self.performance_data = create_time_indices(self.performance_data)

    def run_day(
        self, time: dt.datetime, BASED_ON: str = SQLYahooData.market_mid
    ) -> Union[pd.DataFrame, bool]:
        # Filter onto the time in scope
        time_data: pd.DataFrame = self.performance_data.loc[
            self.performance_data[SQLYahooData.as_at_date] <= time
        ].copy()
        time_data = time_data.loc[time_data[SQLYahooData.date] == time.date()].copy()

        if time_data.empty:
            # Refresh the data
            self.create_performance_data(time)
            time_data: pd.DataFrame = self.performance_data.loc[
                self.performance_data[SQLYahooData.as_at_date] <= time
            ].copy()

        # Data snapshot, taking the last value of the filtered data
        time_data_snapshot: pd.DataFrame = time_data.iloc[-1].copy()

        # Only really concerned about the time and the column that you execute on
        time_data = time_data[[SQLYahooData.as_at_date, BASED_ON]].copy()

        if PLOT:
            times: np.ndarray = time_data.index.to_numpy()
            values: np.ndarray = time_data[BASED_ON].to_numpy()
            plt.plot(times, values)

        regged_data: Dict[Regressions, pd.DataFrame] = self.regression_analysis(
            time_data
        )

        # If we think the stock is going down, we wish to sell, if we think
        # it's going up, we buy.
        reg_trend: pd.DataFrame = regged_data[Regressions.FULL]
        trend_grad: float = gradient(reg_trend)

        reg_upper: pd.DataFrame = regged_data[Regressions.UPPER]
        reg_lower: pd.DataFrame = regged_data[Regressions.LOWER]

        # Some stats for the regs
        upper_grad: float = gradient(reg_upper)
        lower_grad: float = gradient(reg_lower)
        end_minus_end, start_minus_start = width_variance(reg_upper, reg_lower)

        BUY: bool = False
        # Never buy if we are trending downwards on a small time scale
        if trend_grad < 0:
            pass
        # If the upper and lower regressions are greater than zero, definitely buy
        elif (upper_grad > 0) & (lower_grad > 0):
            BUY = True
        # Else, we are trending upwards and now testing whether the 'variance' is narrowing
        elif (start_minus_start > 0) & (start_minus_start - end_minus_end > 0):
            BUY = True
        else:
            pass
        return time_data_snapshot, BUY

    def regression_analysis(
        self, time_data: pd.DataFrame
    ) -> Dict[Regressions, pd.DataFrame]:

        # Extending all of the times that we have by 10 minutes. We then filter after that so we preserve the
        # mapping of any indices.
        predicting_times: pd.DataFrame = extend_time_data(
            time_data[SQLYahooData.as_at_date].to_numpy()
        )
        predicting_times = create_time_indices(predicting_times)
        predicting_times = predicting_times.iloc[-30:].copy()
        time_data = time_data.iloc[-20:].copy()

        # Creating a dictionary to store the regged data
        output_data: Dict[Regressions, pd.DataFrame] = {}

        # Performing the regressions
        for regression in Regressions:
            fitted_reg: LinearRegression = process_regression_type(
                regression, time_data
            )
            reg_values: np.ndarray = fitted_reg.predict(
                predicting_times.index.to_numpy().reshape(-1, 1)
            )

            regged_dataframe: pd.DataFrame = pd.DataFrame(
                {StrategyColumns.Data: reg_values},
                index=predicting_times.index.to_numpy(),
            )
            output_data[regression] = regged_dataframe
            if PLOT:
                plt.plot(predicting_times.index.to_numpy(), reg_values)

        if PLOT:
            plt.ion()
            plt.show(block=False)
            plt.pause(1e-5)

        return output_data


def extend_time_data(times: np.ndarray, mins_to_the_future: int = 10) -> pd.DataFrame:
    """Extends time data n minutes into the future for the reg models."""
    max_time: np.datetime64 = times.max()

    times_to_extend: List[dt.datetime] = []
    for _ in range(mins_to_the_future):
        max_time += np.timedelta64(1, "m")
        times_to_extend.append(max_time)

    times_to_extend_numpy: np.ndarray = np.array(times_to_extend)

    # Concatenating the two data sources together.
    times = np.concatenate((times, times_to_extend_numpy))

    times.sort()

    # Converting this to a dataframe
    time_dataframe: pd.DataFrame = pd.DataFrame(times)
    time_dataframe.columns = [SQLYahooData.as_at_date]

    return time_dataframe


def process_regression_type(
    regression: Regressions, time_data: pd.DataFrame
) -> LinearRegression:
    """
    Contains the logic for regressing our data. For the given type, takes our data
    and returns a linear reg model fitted based on the condition provided.
    """
    # Creating the base model
    linear_model: LinearRegression = LinearRegression()

    # Sample column is always the last element
    sampled_column: str = list(time_data.columns)[-1]

    # Converting data to numpy arrays
    x_data: np.ndarray = time_data.index.to_numpy()
    # x_data: np.ndarray = time_data[SQLYahooData.as_at_date].to_numpy()
    y_data: np.ndarray = time_data[sampled_column].to_numpy()
    match regression:
        case Regressions.FULL:
            # If full, we regress the full data
            pass
        case Regressions.UPPER:
            # Take the largest two values and regress based on these
            y_max: float = y_data.max()
            y_second_max: float = y_data[np.where(y_data != y_max)].max()

            # Returning the corresponding x values
            x_max: int = x_data[np.where(y_data == y_max)][0]
            x_second_max: int = x_data[np.where(y_data == y_second_max)][0]

            # Writing the numpy arrays
            y_data = np.array([y_max, y_second_max])
            x_data = np.array([x_max, x_second_max])
        case Regressions.LOWER:
            y_max: float = y_data.min()
            y_second_max: float = y_data[np.where(y_data != y_max)].min()

            # Returning the corresponding x values
            x_max: int = x_data[np.where(y_data == y_max)][0]
            x_second_max: int = x_data[np.where(y_data == y_second_max)][0]

            # Writing the numpy arrays
            y_data = np.array([y_max, y_second_max])
            x_data = np.array([x_max, x_second_max])
    linear_model.fit(x_data.reshape(-1, 1), y_data)
    return linear_model


class Portfolio:
    open_time: int = 9
    end_time: int = 21

    def __init__(
        self,
        stocks: List[StockBase],
        trade_day: Optional[dt.datetime] = None,
        CAPITAL: float = 1_000_000,
    ) -> None:
        self.stocks: List[StockBase] = stocks
        self.trade_day: Optional[dt.datetime] = trade_day
        self.CAPITAL: float = CAPITAL

        self.trades: List[Trade] = []
        self.InitialiseTrades()

    def InitialiseTrades(self) -> None:
        """Imports the data for the stocks, ready for testing."""
        for stock in self.stocks:
            self.trades.append(Trade(Stock(stock, self.trade_day), self.trade_day))
            log.LogInfo(f"Initialised data for stock: {stock.ticker}")

    def test_data(self) -> None:
        """Testing the model for the data of the trade day."""
        # Starting trading at 9:30 to have some daily data
        for day in range(3):
            current_time: dt.datetime = deepcopy(
                self.trade_day + dt.timedelta(days=day)
            ).replace(minute=30, hour=self.open_time)

            end_time: dt.datetime = deepcopy(
                self.trade_day + dt.timedelta(days=day)
            ).replace(minute=0, hour=self.end_time)

            if day == 1:
                b = 1

            # Running the daily data
            BUY_STATUS: bool = False
            while current_time < end_time:
                for trade in self.trades:
                    trade_snap_shot, NEW_BUY_STATUS = trade.run_day(current_time)

                    if trade.POSITION is not None and trade.POSITION < 0:
                        log.LogInfo("Have gone bust.")
                        break

                    # If the buy statuses are not matching, update them
                    if NEW_BUY_STATUS != BUY_STATUS:
                        # Updating the status
                        BUY_STATUS = NEW_BUY_STATUS

                        # Getting price at that time
                        price: float = trade_snap_shot[SQLYahooData.market_mid] / 100

                        # Buy security
                        if BUY_STATUS == True:
                            # Security not traded yet
                            if trade.POSITION is None:
                                trade.POSITION = self.CAPITAL / price
                                trade.NET_MARKET_VALUE = self.CAPITAL
                                trade.LAST_TRADED_PRICE = price
                            else:
                                # Net market value is the net capital we can deploy if we previously sold
                                trade.POSITION = trade.NET_MARKET_VALUE / price
                                trade.LAST_TRADED_PRICE = price

                                # Do not need to alter the net market value in this case.

                        # Only can have a different buy status from False and reach this
                        # point if bought in the past. We now sell
                        if BUY_STATUS == False:
                            trade.NET_MARKET_VALUE = trade.POSITION * price
                            trade.POSITION = 0  # selling everything
                            trade.LAST_TRADED_PRICE = price

                # Increment by a minute
                current_time += dt.timedelta(minutes=1)
            log.LogInfo(f"Day {day} capital: {self.trades[0].NET_MARKET_VALUE}")
            b = 1

        return None


if __name__ == "__main__":
    Portfolio([Amazon], dt.datetime(year=2024, month=5, day=15)).test_data()

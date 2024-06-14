import pandas as pd
import numpy as np
import datetime as dt

from copy import deepcopy
from typing import Optional, List, Dict, Union
from sklearn.linear_model import LinearRegression
from enum import Enum

from py_max.finance_data import (
    Stock,
    Amazon,
    StockBase,
    Nvidia,
    Google,
    Apple,
    Tesla,
    Paypal,
    Meta,
)
from py_max.py_utils import SQLYahooData
from py_max.model_data.config import log


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
    TRADE_COUNT: int = 0

    def __init__(
        self,
        stock: Stock,
        trade_date: dt.datetime,
        starting_capital: Optional[float] = None,
    ) -> None:
        self.stock: Stock = stock
        self.trade_date: dt.datetime = trade_date

        if starting_capital is not None:
            self.NET_MARKET_VALUE = starting_capital

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
        self,
        time: dt.datetime,
        BASED_ON: str = SQLYahooData.market_mid,
        reverse_points: int = 10,
    ) -> Union[pd.DataFrame, bool]:
        # Filter onto the time in scope
        time_data: pd.DataFrame = self.performance_data.loc[
            self.performance_data[SQLYahooData.as_at_date] <= time
        ].copy()
        time_data = time_data.loc[time_data[SQLYahooData.date] == time.date()].copy()

        # Taking the total volatility of the data at that time
        total_daily_vol: float = time_data[BASED_ON].std()

        # Data snapshot, prices for that security at that time
        time_data_snapshot: pd.DataFrame = time_data.iloc[-1].copy()

        # Only really concerned about the time and the column that you execute on
        time_data = time_data[[SQLYahooData.as_at_date, BASED_ON]].copy()

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

        ### - BASIC STRATEGY CONDITIONS - ###
        inscope_vol: float = time_data[BASED_ON].iloc[-reverse_points:].std()

        BUY: bool = False
        # Never buy if we are trending downwards on a small time scale
        if trend_grad <= 0.1:
            pass
        # Condition on keeping the volatility low
        elif inscope_vol > total_daily_vol:
            pass
        # If the upper and lower regressions are greater than zero, definitely buy
        elif (upper_grad > 0) & (lower_grad > 0):
            BUY = True
        # Else, we are trending upwards and now testing whether the 'variance' is narrowing
        elif (
            (abs(start_minus_start) < 1)
            # & (start_minus_start < 10)
            & (start_minus_start - end_minus_end > 0)
        ):
            BUY = True
        else:
            pass
        return time_data_snapshot, BUY

    def first_trade(
        self, price: float, initial_capital: Optional[float] = None
    ) -> None:
        """
        Executes the first trade of this security. We require an initial capital amount and a price to convert this to a position.
        """
        # Position * price = capital
        if self.POSITION is not None:
            log.LogWarning(
                "Stock alredy has position. Check whether actually a first trade or not."
            )

        # Checking if we initalised any initial capital
        if self.NET_MARKET_VALUE is not None:
            initial_capital = self.NET_MARKET_VALUE
        elif initial_capital is not None:
            pass
        else:
            raise ValueError("No initial capital value has been given for this trade.")

        # Setting all of the values for the trade
        self.POSITION = initial_capital / price
        self.NET_MARKET_VALUE = initial_capital
        self.LAST_TRADED_PRICE = price

        self.TRADE_COUNT += 1

    def execute_trade(
        self, price: float, BUY: bool = True, FULL_SALE: bool = True
    ) -> None:
        """
        Executing secondary trades of the security. NOTE: only binary strat has been implmented.
        """
        if not FULL_SALE:
            raise NotImplementedError("Have not implemented non-binary strat.")

        if BUY:
            # If we are buying, the net market value is conserved (which in this case we are using as 'the amount of capital we can deploy')
            # hence, position value should be zero and net market value is the conserved amount to allocate at that price
            self.POSITION = self.NET_MARKET_VALUE / price
        else:
            # New value of the position is position * price
            self.NET_MARKET_VALUE = self.POSITION * price
            self.POSITION = 0

        # Regardless of buy or sell, always overwrite the last traded price
        self.LAST_TRADED_PRICE = price
        self.TRADE_COUNT += 1

    def regression_analysis(
        self, time_data: pd.DataFrame, reverse_points: int = 10
    ) -> Dict[Regressions, pd.DataFrame]:

        # Extending all of the times that we have by 10 minutes. We then filter after that so we preserve the
        # mapping of any indices.
        predicting_times: pd.DataFrame = extend_time_data(
            time_data[SQLYahooData.as_at_date].to_numpy()
        )
        predicting_times = create_time_indices(predicting_times)
        predicting_times = predicting_times.iloc[-10 - reverse_points :].copy()
        time_data = time_data.iloc[-reverse_points:].copy()

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
            try:
                y_second_max: float = y_data[np.where(y_data != y_max)].max()
            except ValueError:
                y_second_max = y_max

            # Returning the corresponding x values
            x_max: int = x_data[np.where(y_data == y_max)][0]

            if y_max == y_second_max:
                x_second_max: int = x_data[np.where(y_data == y_second_max)][1]
            else:
                x_second_max: int = x_data[np.where(y_data == y_second_max)][0]

            # Writing the numpy arrays
            y_data = np.array([y_max, y_second_max])
            x_data = np.array([x_max, x_second_max])
        case Regressions.LOWER:
            y_min: float = y_data.min()
            try:
                y_second_min: float = y_data[np.where(y_data != y_min)].min()
            except ValueError:
                y_second_min = y_min

            # Returning the corresponding x values
            x_min: int = x_data[np.where(y_data == y_min)][0]

            if y_min == y_second_min:
                x_second_min: int = x_data[np.where(y_data == y_second_min)][1]
            else:
                x_second_min: int = x_data[np.where(y_data == y_second_min)][0]

            # Writing the numpy arrays
            y_data = np.array([y_min, y_second_min])
            x_data = np.array([x_min, x_second_min])
    linear_model.fit(x_data.reshape(-1, 1), y_data)
    return linear_model


class Portfolio:

    # Need to run the data from the start time until the end time.
    open_time: int = 9
    end_time: int = 21

    def __init__(
        self,
        stocks: List[StockBase],
        trade_dates: List[dt.datetime],
        CAPITAL: float = 1_000_000,
    ) -> None:
        self.stocks: List[StockBase] = stocks
        self.trade_dates: List[dt.datetime] = trade_dates
        self.CAPITAL: float = CAPITAL

        self.trades: Dict[dt.datetime, List[Trade]] = {}

    def initalise_trades(self, trade_date: dt.datetime) -> None:
        """Imports the data for the stocks, ready for testing that day."""
        # Clearing any existing data.
        if self.trades != []:
            self.trades = []

        for stock in self.stocks:
            # Initialising each trade with the same amount of capital (we are only testing strategy)
            self.trades.append(
                Trade(Stock(stock, trade_date), trade_date, self.CAPITAL)
            )
            log.LogInfo(f"Initialised data for stock: {stock.ticker}")

    def test_data(self) -> None:
        """Testing the model for the data of the trade day."""
        output_data: pd.DataFrame = pd.DataFrame()

        # Running through each day
        for date in self.trade_dates:
            # Initalising the data for that trade date
            self.initalise_trades(date)

            # Running the sim for each trde
            for trade in self.trades:
                if trade.performance_data.empty:
                    log.LogWarning(f"No data for {trade.stock.name} on {date}")

                # Setting the current time (initally the start time of the sim, iterated through the loop)
                current_time: dt.datetime = deepcopy(date).replace(
                    minute=30, hour=self.open_time
                )

                # Cut off time of the simulation
                end_time: dt.datetime = deepcopy(date).replace(
                    minute=0, hour=self.end_time
                )

                # Running the daily data
                BUY_STATUS: bool = False
                while current_time < end_time:

                    # Running the data for the day, at that time
                    trade_snap_shot, NEW_BUY_STATUS = trade.run_day(current_time)

                    # If our position is less than zero, we are bust
                    if trade.POSITION is not None and trade.POSITION < 0:
                        log.LogInfo(f"{trade.stock.name} has gone bust.")
                        break

                    # If the buy statuses are not matching, this means we either buy or we sell
                    if NEW_BUY_STATUS != BUY_STATUS:
                        # Updating the status
                        BUY_STATUS = NEW_BUY_STATUS

                        # Getting price at that time
                        price: float = trade_snap_shot[SQLYahooData.market_mid] / 100

                        # Buy security
                        if BUY_STATUS == True:
                            # If the security is not traded yet and we need to buy, we need to initialise.
                            if trade.POSITION is None:
                                trade.first_trade(self.CAPITAL, price)

                        # Else, we can just execute the trade.
                        trade.execute_trade(price=price, BUY=BUY_STATUS)

                    # Increment by a minute
                    current_time += dt.timedelta(minutes=1)

                # return
                return_value: float = trade.NET_MARKET_VALUE / self.CAPITAL - 1

                # Logging the daily info
                log.LogInfo(
                    f"Day {date} capital for {trade.stock.name}: {trade.NET_MARKET_VALUE} - daily return is {return_value*100:.2f}%"
                )

                # Storing the data for output to excel
                daily_report: pd.DataFrame = pd.DataFrame(
                    {
                        "Date": [date],
                        "Ticker": [trade.stock.name],
                        "Return": [return_value],
                        "TradeCount": [trade.TRADE_COUNT],
                    }
                )
                output_data = pd.concat([output_data, daily_report])

        output_data.to_csv("C:/Temp/StockTesterData.csv")
        return output_data


if __name__ == "__main__":
    Portfolio(
        [Nvidia, Google, Amazon, Apple, Tesla, Paypal, Meta],
        [
            # dt.datetime(year=2024, month=5, day=27),
            dt.datetime(year=2024, month=5, day=28),
            dt.datetime(year=2024, month=5, day=29),
            dt.datetime(year=2024, month=5, day=30),
            dt.datetime(year=2024, month=5, day=31),
            dt.datetime(year=2024, month=6, day=3),
            dt.datetime(year=2024, month=6, day=4),
            dt.datetime(year=2024, month=6, day=5),
            dt.datetime(year=2024, month=6, day=6),
            dt.datetime(year=2024, month=6, day=7),
            # dt.datetime(year=2024, month=5, day=29),
            # dt.datetime(year=2024, month=5, day=30),
            # dt.datetime(year=2024, month=5, day=31),
        ],
    ).test_data()

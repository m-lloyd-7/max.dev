from py_max.py_utils import ErrorLogger
from py_max.finance_data.config.stocks import (
    Apple,
    Amazon,
    Paypal,
    Nvidia,
    Google,
    Tesla,
    Meta,
    StockBase,
)


logger: ErrorLogger = ErrorLogger(
    __name__,
    # "C:/Users/User/dev/max-dev/max_development/stock_project/stock_project/error_logs/stock_stripper_log.log",
)

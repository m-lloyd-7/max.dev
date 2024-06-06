from abc import ABC, abstractmethod


class StockBase(ABC):

    @property
    @abstractmethod
    def ticker(self) -> str:
        return NotImplementedError


class Apple(StockBase):
    ticker: str = "AAPL"


class Meta(StockBase):
    ticker: str = "META"


class Google(StockBase):
    ticker: str = "GOOGL"


class Tesla(StockBase):
    ticker: str = "TSLA"


class Nvidia(StockBase):
    ticker: str = "NVDA"


class Amazon(StockBase):
    ticker: str = "AMZN"


class Paypal(StockBase):
    ticker: str = "PYPL"

class SQLYahooData:
    table_name: str = "yahooData"
    schema: str = "stk"
    as_at_date: str = "AsAtDateTime"
    market_high: str = "MarketHigh"
    market_low: str = "MarketLow"
    market_open: str = "MarketOpen"
    market_close: str = "MarketClose"
    market_volume: str = "MarketVolume"
    security: str = "Security"
    currency: str = "Currency"
    instrument_type: str = "InstrumentType"
    time_zone: str = "TimeZone"
    exchange_name: str = "ExchangeName"
    gmt_off_set: str = "GmtOffSet"

    # User defined
    date: str = "Date"
    market_mid: str = "MarketMid"

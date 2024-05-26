class SQLYahooData:
    table_name: str = "yahooData"
    schema: str = "stk"

    as_at_date: str = "asAtDateTime"
    market_high: str = "marketHigh"
    market_low: str = "marketLow"
    market_open: str = "marketOpen"
    market_close: str = "marketClose"
    market_volume: str = "marketVolume"
    security: str = "security"
    currency: str = "currency"
    instrument_type: str = "instrumentType"
    time_zone: str = "timeZone"
    exchange_name: str = "exchangeName"
    gmt_off_set: str = "gmtOffSet"

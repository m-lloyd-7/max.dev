use chrono::{DateTime, Utc};

pub struct Stock {
    ticker: String,
    currency: String,
    instrument_type: String,
    exchange_name: String,
    time_zone: String,
    gmt_offset: i32,
    timestamps: Vec<DateTime<Utc>>,
    low: Vec<f64>,
    high: Vec<f64>,
    open: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<f64>,
}

impl Stock {
    pub fn new(
        ticker: String,
        currency: String,
        instrument_type: String,
        exchange_name: String,
        time_zone: String,
        gmt_offset: i32,
    ) -> Self {
        Self {
            ticker,
            currency,
            instrument_type,
            exchange_name,
            time_zone,
            gmt_offset,
            timestamps: Vec::new(),
            low: Vec::new(),
            high: Vec::new(),
            open: Vec::new(),
            close: Vec::new(),
            volume: Vec::new(),
        }
    }

    pub fn write_line(
        &mut self,
        timestamp: DateTime<Utc>,
        low_price: f64,
        high_price: f64,
        open_price: f64,
        close_price: f64,
        trade_volume: f64,
    ) {
        self.timestamps.push(timestamp);
        self.low.push(low_price);
        self.high.push(high_price);
        self.open.push(open_price);
        self.close.push(close_price);
        self.volume.push(trade_volume);
    }
}

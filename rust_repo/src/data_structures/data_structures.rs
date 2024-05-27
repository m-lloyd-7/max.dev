use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct Stock {
    pub ticker: String,
    pub currency: String,
    pub instrument_type: String,
    pub exchange_name: String,
    pub time_zone: String,
    pub gmt_offset: i32,
    pub timestamps: Vec<DateTime<Utc>>,
    pub low: Vec<f32>,
    pub high: Vec<f32>,
    pub open: Vec<f32>,
    pub close: Vec<f32>,
    pub volume: Vec<f32>,
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
        low_price: f32,
        high_price: f32,
        open_price: f32,
        close_price: f32,
        trade_volume: f32,
    ) {
        self.timestamps.push(timestamp);
        self.low.push(low_price);
        self.high.push(high_price);
        self.open.push(open_price);
        self.close.push(close_price);
        self.volume.push(trade_volume);
    }
}

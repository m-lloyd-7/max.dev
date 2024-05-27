use crate::data_structures::data_structures::Stock;

use chrono::{DateTime, Utc};
use odbc_api::{ConnectionOptions, Cursor, CursorRow, Environment};
use std::error::Error;

pub enum StockQueryMapping {
    AsAtDate,
    Security,
    Currency,
    Low,
    High,
    Open,
    Close,
    Volume,
    InstrumentType,
    ExchangeName,
    TimeZone,
    GmtOffSet,
}

pub fn stock_query_col_map(column_name: StockQueryMapping) -> u16 {
    match column_name {
        StockQueryMapping::AsAtDate => 1,
        StockQueryMapping::Security => 2,
        StockQueryMapping::Currency => 3,
        StockQueryMapping::Low => 4,
        StockQueryMapping::High => 5,
        StockQueryMapping::Open => 6,
        StockQueryMapping::Close => 7,
        StockQueryMapping::Volume => 8,
        StockQueryMapping::InstrumentType => 9,
        StockQueryMapping::ExchangeName => 10,
        StockQueryMapping::TimeZone => 11,
        StockQueryMapping::GmtOffSet => 12,
    }
}

pub struct StockData {
    stock_values: Vec<Stock>,
}

impl StockData {
    pub const CONNECTION_STRING:&'static str = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-SPUR71A;Database=max_dev;Trusted_Connection=yes;";
    pub const STOCK_QUERY: &'static str = "SELECT [asAtDateTime]
            ,[security]
            ,[currency]
            ,[marketLow]
            ,[marketHigh]
            ,[marketOpen]
            ,[marketClose]
            ,[marketVolume]
            ,[instrumentType]
            ,[exchangeName]
            ,[timeZone]
            ,[gmtOffSet]
        FROM [max_dev].[stk].[yahooData]";

    pub fn new() -> Self {
        Self {
            stock_values: Vec::new(),
        }
    }

    pub fn get_stocks(&mut self) -> Result<(), Box<dyn Error>> {
        // Creating a new environment
        let environment = Environment::new()?;

        // Getting the database
        let connection = environment.connect_with_connection_string(
            Self::CONNECTION_STRING,
            ConnectionOptions::default(),
        )?;

        // Executing the query
        if let Some(mut cursor) = connection.execute(Self::STOCK_QUERY, ())? {
            // Getting the rows from the cursor object
            while let Some(mut rows) = cursor.next_row()? {
                // Getting the security name from the rows
                let security_name = Self::get_stock_attr(&mut rows, StockQueryMapping::Security)?;

                // Checking if that name is already written
                match self.check_if_written_stock(&security_name)? {
                    false => {
                        // If it's not written, we want to add it to the list
                        let stock = self.initialise_stock(&mut rows, &security_name)?;
                        self.stock_values.push(stock);
                    }
                    true => (),
                }
                self.retrieve_update_stock(&mut rows, &security_name)?;
            }
        }
        Ok(())
    }

    fn initialise_stock(
        &self,
        cursor_row: &mut CursorRow,
        security_name: &String,
    ) -> Result<Stock, Box<dyn Error>> {
        let currency = Self::get_stock_attr(cursor_row, StockQueryMapping::Currency)?;
        let instrument_type = Self::get_stock_attr(cursor_row, StockQueryMapping::InstrumentType)?;
        let exchange_name = Self::get_stock_attr(cursor_row, StockQueryMapping::ExchangeName)?;
        let time_zone = Self::get_stock_attr(cursor_row, StockQueryMapping::TimeZone)?;
        let gmt_offset = Self::get_stock_int(cursor_row, StockQueryMapping::GmtOffSet)?;

        // Cloning the security name. Note that we parse this as an argument since you cannot use twice
        let security_name_copy = security_name.clone();

        let stock = Stock::new(
            security_name_copy,
            currency,
            instrument_type,
            exchange_name,
            time_zone,
            gmt_offset,
        );
        Ok(stock)
    }

    fn retrieve_update_stock(
        &mut self,
        cursor_row: &mut CursorRow,
        security_name: &String,
    ) -> Result<(), Box<dyn Error>> {
        let mut matched_index = 0;

        // Retrieving the stock from the list
        for (index, stock) in self.stock_values.iter().enumerate() {
            if stock.ticker == *security_name {
                // Updating the value of the matched index
                matched_index = index;
            }
        }

        if let Some(stock) = self.stock_values.get_mut(matched_index) {
            Self::update_stock(stock, cursor_row)?;
            Ok(())
        } else {
            Err(Box::from("Failed retrieving stock value to update."))
        }
    }

    fn update_stock(stock: &mut Stock, cursor_row: &mut CursorRow) -> Result<(), Box<dyn Error>> {
        // Getting the stock data
        let stock_low = Self::get_stock_float(cursor_row, StockQueryMapping::Low)?;
        let stock_high = Self::get_stock_float(cursor_row, StockQueryMapping::High)?;
        let stock_open = Self::get_stock_float(cursor_row, StockQueryMapping::Open)?;
        let stock_close = Self::get_stock_float(cursor_row, StockQueryMapping::Close)?;
        let stock_volume = Self::get_stock_float(cursor_row, StockQueryMapping::Volume)?;

        // Getting the date for the stock -- different proceedure
        let date_time = Self::get_stock_date(cursor_row, StockQueryMapping::AsAtDate)?;

        // Adding data to the stocks attributes
        stock.write_line(
            date_time,
            stock_low,
            stock_high,
            stock_open,
            stock_close,
            stock_volume,
        );

        Ok(())
    }

    fn check_if_written_stock(&self, security_name: &String) -> Result<bool, Box<dyn Error>> {
        match self.stock_values.is_empty() {
            true => return Ok(false),
            false => {
                // Converting the array to an int so we can test whether contains or not
                let copied_stocks = self.stock_values.clone();

                // Converting the name of the stock into an integer based on true or false
                let inted_stocks: Vec<i32> = copied_stocks
                    .iter()
                    .map(|x| if x.ticker == *security_name { 1 } else { 0 })
                    .collect();

                // Summing the values - if it's 1, then we have the value, else we don't. If greater than 1, error
                let inscope_count = inted_stocks.iter().sum();
                match inscope_count {
                    0 => return Ok(false),
                    1 => return Ok(true),
                    _ => Err(Box::from(
                        "The database has generated more than one stock object for the security!",
                    )),
                }
            }
        }
    }

    fn get_stock_attr(
        cursor_row: &mut CursorRow,
        stock_attribute: StockQueryMapping,
    ) -> Result<String, Box<dyn Error>> {
        let mut security_name_buffer: Vec<u8> = Vec::new();
        match stock_attribute {
            StockQueryMapping::Security
            | StockQueryMapping::Currency
            | StockQueryMapping::ExchangeName
            | StockQueryMapping::InstrumentType
            | StockQueryMapping::TimeZone => cursor_row.get_text(
                stock_query_col_map(stock_attribute),
                &mut security_name_buffer,
            ),
            _ => Ok(false),
        }?;

        let security_name = String::from_utf8(security_name_buffer).unwrap();
        Ok(security_name)
    }

    fn get_stock_int(
        cursor_row: &mut CursorRow,
        stock_attribute: StockQueryMapping,
    ) -> Result<i32, Box<dyn Error>> {
        let mut stock_int_buffer: i32 = 0;
        match stock_attribute {
            StockQueryMapping::GmtOffSet => {
                cursor_row.get_data(stock_query_col_map(stock_attribute), &mut stock_int_buffer)
            }

            _ => Ok(()),
        }?;

        Ok(stock_int_buffer)
    }

    fn get_stock_float(
        cursor_row: &mut CursorRow,
        stock_attribute: StockQueryMapping,
    ) -> Result<f32, Box<dyn Error>> {
        let mut stock_float_buffer: f32 = 0.0;
        match stock_attribute {
            StockQueryMapping::High
            | StockQueryMapping::Low
            | StockQueryMapping::Open
            | StockQueryMapping::Close
            | StockQueryMapping::Volume => cursor_row.get_data(
                stock_query_col_map(stock_attribute),
                &mut stock_float_buffer,
            ),
            _ => Ok(()),
        }?;

        Ok(stock_float_buffer)
    }

    fn get_stock_date(
        cursor_row: &mut CursorRow,
        stock_attribute: StockQueryMapping,
    ) -> Result<DateTime<Utc>, Box<dyn Error>> {
        let mut stock_date_buffer: i64 = 0;
        match stock_attribute {
            StockQueryMapping::AsAtDate => {
                cursor_row.get_data(stock_query_col_map(stock_attribute), &mut stock_date_buffer)
            }
            _ => Ok(()),
        }?;
        let date_time: DateTime<Utc> = DateTime::from_timestamp(stock_date_buffer, 0).unwrap();
        Ok(date_time)
    }
}

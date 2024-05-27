use crate::data_structures::data_structures::Stock;

use chrono::{DateTime, Utc};
use odbc_api::{ConnectionOptions, Cursor, CursorRow, Environment};
use std::error::Error;

enum StockQueryMapping {
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
            // Getting the column names out of the database.
            // let column_names: Vec<String> = cursor
            //     .column_names()?
            //     .into_iter()
            //     .map(|s| match s {
            //         Ok(s) => s.to_string(),
            //         Err(s) => "".to_string(),
            //     })
            //     .collect();

            // Getting the rows from the cursor object
            if let Some(mut rows) = cursor.next_row()? {
                let security_name = Self::get_stock_attr(&mut rows, StockQueryMapping::Security)?;

                match self.check_if_written_stock(&security_name)? {
                    false => {
                        let stock = Self::initialise_stock(&mut rows)?;
                        self.stock_values.push(stock);
                    }
                    true => (),
                }
                self.retrieve_update_stock(&security_name, &mut rows);
            }
        }
        Ok(())
    }

    fn get_stock_attr(
        cursor_row: &mut CursorRow,
        stock_attribute: StockQueryMapping,
    ) -> Result<String, Box<dyn Error>> {
        let mut security_name_buffer = Vec::new();
        match stock_attribute {
            StockQueryMapping::Security
            | StockQueryMapping::Currency
            | StockQueryMapping::ExchangeName
            | StockQueryMapping::TimeZone
            | StockQueryMapping::InstrumentType => cursor_row.get_text(
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
            StockQueryMapping::GmtOffSet | StockQueryMapping::Volume => {
                cursor_row.get_data(stock_query_col_map(stock_attribute), &mut stock_int_buffer)
            }
            _ => Ok(()),
        }?;

        Ok(stock_int_buffer)
    }

    fn initialise_stock(cursor_row: &mut CursorRow) -> Result<Stock, Box<dyn Error>> {
        let name = Self::get_stock_attr(cursor_row, StockQueryMapping::Security)?;
        let currency = Self::get_stock_attr(cursor_row, StockQueryMapping::Currency)?;
        let exchange_name = Self::get_stock_attr(cursor_row, StockQueryMapping::ExchangeName)?;
        let inst_type = Self::get_stock_attr(cursor_row, StockQueryMapping::InstrumentType)?;
        let time_zone = Self::get_stock_attr(cursor_row, StockQueryMapping::TimeZone)?;
        let gmt_offset = Self::get_stock_int(cursor_row, StockQueryMapping::GmtOffSet)?;

        let stock = Stock::new(
            name,
            currency,
            inst_type,
            exchange_name,
            time_zone,
            gmt_offset,
        );
        Ok(stock)
    }

    fn retrieve_update_stock(
        &mut self,
        security_name: &String,
        mut row_data: &mut CursorRow,
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
            Self::update_stock(stock, &mut row_data);
            Ok(())
        } else {
            Err(Box::from("Failed retrieving stock value to update."))
        }
    }

    fn update_stock(stock: &mut Stock, row_data: &mut CursorRow) {
        let mut date_buffer: i64 = 0;
        let mut close_buffer: f64 = 0.0;
        let mut low_buffer: f64 = 0.0;
        let mut high_buffer: f64 = 0.0;
        let mut open_buffer: f64 = 0.0;
        let mut volume_buffer: f64 = 0.0;

        // Getting the data from the query
        row_data.get_data(
            stock_query_col_map(StockQueryMapping::AsAtDate),
            &mut date_buffer,
        );
        row_data.get_data(stock_query_col_map(StockQueryMapping::Low), &mut low_buffer);
        row_data.get_data(
            stock_query_col_map(StockQueryMapping::High),
            &mut high_buffer,
        );
        row_data.get_data(
            stock_query_col_map(StockQueryMapping::Open),
            &mut open_buffer,
        );
        row_data.get_data(
            stock_query_col_map(StockQueryMapping::Close),
            &mut close_buffer,
        );
        row_data.get_data(
            stock_query_col_map(StockQueryMapping::Volume),
            &mut volume_buffer,
        );

        // Converting the date buffer to a datetime
        let date_time: DateTime<Utc> = DateTime::from_timestamp(date_buffer, 0).unwrap();

        // Adding data to the stocks attributes
        stock.write_line(
            date_time,
            low_buffer,
            high_buffer,
            open_buffer,
            close_buffer,
            volume_buffer,
        );
    }
}

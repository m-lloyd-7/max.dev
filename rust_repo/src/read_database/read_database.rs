use crate::data_structures::data_structures::Stock;

use odbc_api::{ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::error::Error;

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

    pub fn get_stocks(&self) -> Result<Option, Box<dyn Error>> {
        // Creating a new environment
        let environment = Environment::new()?;

        // Getting the database
        let mut connection = environment.connect_with_connection_string(
            Self::CONNECTION_STRING,
            ConnectionOptions::default(),
        )?;

        // Executing the query
        if let Some(cursor) = connection.execute(Self::STOCK_QUERY, ())? {
            let column_names: Vec<String> = cursor
                .column_names()?
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            // Getting the rows from the cursor object
            if let Some(rows) = cursor.next_row()? {
                for (index, column) in rows.iter().enumerate() {
                    let b = 1;
                }
            }
        }
    }
}

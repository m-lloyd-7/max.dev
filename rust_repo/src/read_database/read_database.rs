use crate::data_structures::data_structures::Stock;

use odbc_api::{ConnectionOptions, Environment, ResultSetMetadata};

pub struct StockData {
    stock_values: Vec<Stock>,
}

impl StockData {
    const STOCK_QUERY: &str = "SELECT [asAtDateTime]
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

    pub fn new() {
        Self { Vec: new() }
    }

    pub fn get_stocks(&self) {
        // Creating a new environment
        let environment = Environment::new()?;

        // Getting the database
        let mut connection = environment
            .connect_with_connection_string(DB_CONNECTION, ConnectionOptions::default())?;

        // Executing the query
        if let Some(cursor) = connecton.execute(self.STOCK_QUERY, ())? {
            let column_names: Vec<String> = cursor.column_names()?.collect();

            // Getting the rows from the cursor object
            if Some(rows) = cursor.fetch()? {
                for (index, column) in rows.iter().enumerate() {
                    let b = 1;
                }
            }
        }
    }
}

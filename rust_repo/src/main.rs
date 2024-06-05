mod read_database {
    pub mod read_database;
}
mod data_structures {
    pub mod data_structures;
}

fn main() {
    let mut stock_data = read_database::read_database::StockData::new();
    match stock_data.initialise_stock_data() {
        Ok(d) => d,
        Err(_e) => println!("{}", _e),
    }
    match stock_data.get_stock_data() {
        Ok(d) => d,
        Err(_e) => println!("{}", _e),
    }
}

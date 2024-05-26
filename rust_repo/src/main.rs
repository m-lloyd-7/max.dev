mod read_database {
    pub mod read_database;
}
mod data_structures {
    pub mod data_structures;
}
mod statics {
    pub mod statics;
}
fn main() {
    let stock_data = read_database::StockData::new();
    stock_data.get_stocks();
}

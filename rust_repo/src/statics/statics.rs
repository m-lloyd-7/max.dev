use lazy_static;

lazy_static! {
    [pub] static ref DB_CONNECTION: str = "mssql+pyodbc://DESKTOP-SPUR71A/max_dev?driver=ODBC+Driver+17+for+SQL+Server";
    [pub] static ref CONNECTION_STRING:str = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-SPUR71A;Database=max_dev;Trusted_Connection=yes;";
}

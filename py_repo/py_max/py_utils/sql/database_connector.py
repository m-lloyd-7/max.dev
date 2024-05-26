import sqlalchemy as db
from sqlalchemy.engine.base import Engine, Connection


class DatabaseConnector:
    LOCAL: str = (
        "mssql+pyodbc://DESKTOP-SPUR71A/max_dev?driver=ODBC+Driver+17+for+SQL+Server"
    )

    def __enter__(self) -> Connection:
        self.engine: Engine = db.create_engine(self.LOCAL, echo=False)
        self.connected_engine: Connection = self.engine.connect()
        return self.connected_engine

    def __exit__(self, exception_type, exception_value, traceback):
        self.connected_engine.close()
        self.engine.dispose()

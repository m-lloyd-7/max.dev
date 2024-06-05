import sqlalchemy as db

from sqlalchemy.engine.base import Engine, Connection
from enum import Enum


class DBChoice(Enum):
    LOCAL: str = (
        "mssql+pyodbc://DESKTOP-SPUR71A/max_dev?driver=ODBC+Driver+17+for+SQL+Server"
    )


class DatabaseConnector:

    def __init__(self, db_choice: DBChoice) -> None:
        self.db_choice: DBChoice = db_choice

    def __enter__(self) -> Connection:
        self.engine: Engine = db.create_engine(self.db_choice.value, echo=False)
        self.connected_engine: Connection = self.engine.connect()
        return self.connected_engine

    def __exit__(self, exception_type, exception_value, traceback):
        self.connected_engine.close()
        self.engine.dispose()

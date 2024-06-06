import sqlalchemy as db
import pandas as pd

from sqlalchemy.engine.base import Engine, Connection
from enum import Enum
from typing import Callable
from functools import wraps


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


class ExecuteQuery:
    def __init__(
        self, db_choice: DBChoice = DBChoice.LOCAL, ALLOW_EMPTY: bool = True
    ) -> None:
        self.db_choice: DBChoice = db_choice
        self.ALLOW_EMPTY: bool = ALLOW_EMPTY

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def executor(*args, **kwargs) -> pd.DataFrame:
            # Retrieving the query from the function
            query: db.TextClause = func(*args, **kwargs)

            # Executing the query
            with DatabaseConnector(db_choice=self.db_choice) as connection:
                data: pd.DataFrame = pd.read_sql(query, connection)

            if not self.ALLOW_EMPTY:
                if data.empty:
                    raise ValueError("Returning empty data. Check validity of query")

            return data

        return executor

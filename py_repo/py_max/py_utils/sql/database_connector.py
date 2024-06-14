import sqlalchemy as db
import pandas as pd
import json

from sqlalchemy.engine.base import Engine, Connection
from enum import Enum
from typing import Callable, Dict
from functools import wraps

KEYS_PATH: str = """C:/Users/User/Documents/Data/keys.json"""
with open(KEYS_PATH) as file:
    key_data: Dict[str, str] = json.load(file)
    local_connection_string: str = key_data["DB_CONNECTION"]


class DBChoice(Enum):
    LOCAL: str = local_connection_string


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

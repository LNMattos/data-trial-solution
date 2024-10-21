from scripts.data_processor.interfaces.processor_interface import DataProcessorInterface
import pandas as pd
from typing import Any, List, Dict


class DuckDBDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.connection = duckdb.connect(database=':memory:')
        self.table_name = 'data_table'

    def load_data(self, source: Any) -> None:
        if isinstance(source, str):
            # Supondo que 'source' seja um caminho para um arquivo CSV
            self.connection.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM read_csv_auto('{source}')")
        elif isinstance(source, pd.DataFrame):
            self.connection.register('temp_view', source)
            self.connection.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM temp_view")
        else:
            raise ValueError("Unsupported source type for DuckDB.")

    def remove_columns(self, columns: List[str]) -> None:
        cols = [col for col in self.get_columns() if col not in columns]
        cols_str = ", ".join(cols)
        self.connection.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT {cols_str} FROM {self.table_name}")

    def remove_rows(self, condition: Any) -> None:
        # Supondo que 'condition' seja uma expressão SQL
        self.connection.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM {self.table_name} WHERE NOT ({condition})")

    def convert_types(self, column_type_map: Dict[str, Any]) -> None:
        set_clause = ", ".join([f"{col}::{dtype}" for col, dtype in column_type_map.items()])
        self.connection.execute(f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT {set_clause} FROM {self.table_name}")

    def create_column(self, column_name: str, expression: str) -> None:
        self.connection.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} AS ({expression})")

    def get_data(self) -> pd.DataFrame:
        return self.connection.execute(f"SELECT * FROM {self.table_name}").fetchdf()

    def get_columns(self) -> List[str]:
        return [desc[0] for desc in self.connection.execute(f"DESCRIBE {self.table_name}").fetchall()]

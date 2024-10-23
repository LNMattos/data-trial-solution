from scripts.data_processor.interfaces.processor_interface import DataProcessorInterface
import pandas as pd
from typing import Any, List, Dict, Callable

class PandasDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.df = pd.DataFrame()

    def load_data(self, source: Any, **kwargs) -> None:
        if isinstance(source, str):
            self.df = pd.read_csv(source, **kwargs)
        elif isinstance(source, pd.DataFrame):
            self.df = source
        else:
            raise ValueError("Unsupported source type for pandas.")

    def remove_columns(self, columns: List[str]) -> None:
        self.df.drop(columns=columns, inplace=True, errors='ignore')

    def filter_rows(self, condition: Any) -> None:
        self.df = self.df[condition(self.df)]

    def convert_types(self, column_type_map: Dict[str, Any]) -> None:
        self.df = self.df.astype(column_type_map)

    def create_column(self, column_name: str, func: Callable) -> None:
        self.df[column_name] = func(self.df)

    def apply_function(self, func: Callable[['pd.DataFrame'], 'pd.DataFrame'], **kwargs) -> None:
        self.df = func(self.df, **kwargs)

    def convert_column_to_datetime(self, column: str, format: str = None, **kwargs) -> None:
        self.df[column] = pd.to_datetime(self.df[column], format=format, **kwargs)

    def drop_all_null_columns(self) -> None:
        self.df.dropna(axis=1, how='all', inplace=True)

    def get_data(self) -> pd.DataFrame:
        return self.df

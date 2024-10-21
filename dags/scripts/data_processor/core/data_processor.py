from scripts.data_processor.factory.processor_factory import DataProcessorFactory
from typing import Any, List, Dict, Callable

class DataProcessor:
    def __init__(self, backend: str):
        self.processor = DataProcessorFactory.create_processor(backend)

    def load_data(self, source: Any) -> None:
        self.processor.load_data(source)

    def remove_columns(self, columns: List[str]) -> None:
        self.processor.remove_columns(columns)

    def remove_rows(self, condition: Any) -> None:
        self.processor.remove_rows(condition)

    def convert_types(self, column_type_map: Dict[str, Any]) -> None:
        self.processor.convert_types(column_type_map)

    def create_column(self, column_name: str, func_or_expression: Any) -> None:
        self.processor.create_column(column_name, func_or_expression)

    def apply_function(self, func: Callable, **kwargs) -> None:
        self.processor.apply_function(func, **kwargs)

    def convert_column_to_datetime(self, column: str, format: str = None, **kwargs) -> None:
        self.processor.convert_column_to_datetime(column, format=format, **kwargs)

    def drop_all_null_columns(self) -> None:
        self.processor.drop_all_null_columns()

    def get_data(self) -> Any:
        return self.processor.get_data()

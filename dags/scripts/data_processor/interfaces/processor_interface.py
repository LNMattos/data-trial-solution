from abc import ABC, abstractmethod
from typing import Any, List, Dict, Callable

class DataProcessorInterface(ABC):
    @abstractmethod
    def load_data(self, source: Any) -> None:
        pass

    @abstractmethod
    def remove_columns(self, columns: List[str]) -> None:
        pass

    @abstractmethod
    def remove_rows(self, condition: Any) -> None:
        pass

    @abstractmethod
    def convert_types(self, column_type_map: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def create_column(self, column_name: str, func: Callable) -> None:
        pass

    @abstractmethod
    def apply_function(self, func: Callable, **kwargs) -> None:
        pass

    @abstractmethod
    def convert_column_to_datetime(self, column: str, format: str = None) -> None:
        pass

    @abstractmethod
    def drop_all_null_columns(self) -> None:
        pass

    @abstractmethod
    def get_data(self) -> Any:
        pass

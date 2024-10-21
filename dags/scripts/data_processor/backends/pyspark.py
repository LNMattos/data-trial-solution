from scripts.data_processor.interfaces.processor_interface import DataProcessorInterface
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_timestamp, col, from_unixtime, coalesce
from typing import Any, List, Dict, Callable, Optional

class SparkDataProcessor(DataProcessorInterface):
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.df = self.spark.createDataFrame([], schema=None)

    def load_data(self, source: Any, **kwargs) -> None:
        if isinstance(source, str):
            self.df = self.spark.read.csv(source, header=True, inferSchema=True, **kwargs)
        elif isinstance(source, DataFrame):
            self.df = source
        else:
            raise ValueError("Unsupported source type for Spark.")

    def remove_columns(self, columns: List[str]) -> None:
        self.df = self.df.drop(*columns)

    def remove_rows(self, condition: Any) -> None:
        self.df = self.df.filter(~condition(self.df))

    def convert_types(self, column_type_map: Dict[str, Any]) -> None:
        for col_name, col_type in column_type_map.items():
            if col_name in self.df.columns:
                self.df = self.df.withColumn(col_name, col(col_name).cast(col_type))

    def create_column(self, column_name: str, func: Callable[[DataFrame], Any]) -> None:
        self.df = self.df.withColumn(column_name, func(self.df))

    def apply_function(self, func: Callable[[DataFrame, Dict], DataFrame], **kwargs) -> None:
        self.df = func(self.df, **kwargs)

    def convert_column_to_datetime(
        self, 
        column: str, 
        unit: Optional[str] = None, 
        formats: Optional[List[str]] = None
    ) -> None:
        if unit:
            if unit == 's':
                self.df = self.df.withColumn(column, from_unixtime(col(column)).cast("timestamp"))
            elif unit == 'ms':
                self.df = self.df.withColumn(column, (col(column) / 1000).cast("timestamp"))
            elif unit == 'ns':
                self.df = self.df.withColumn(column, (col(column) / 1e9).cast("timestamp"))
            else:
                raise ValueError(f"Unsupported unit: {unit}")

        if formats:
            timestamp_cols = [to_timestamp(col(column), fmt) for fmt in formats]
            self.df = self.df.withColumn(column, coalesce(*timestamp_cols))

    def drop_all_null_columns(self) -> None:
        non_null_cols = [c for c in self.df.columns if not self.df.select(c).dropna().rdd.isEmpty()]
        self.df = self.df.select(non_null_cols)

    def get_data(self) -> DataFrame:
        return self.df

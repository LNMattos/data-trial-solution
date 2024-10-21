# interfaces/data_reader.py

from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

class DataReader(ABC):
    @abstractmethod
    def read(self, path: str, **kwargs) -> Union[pd.DataFrame, SparkDataFrame]:
        pass
# interfaces/data_writer.py

from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

class DataWriter(ABC):
    @abstractmethod
    def write(self, data: Union[pd.DataFrame, SparkDataFrame], path: str, **kwargs):
        pass
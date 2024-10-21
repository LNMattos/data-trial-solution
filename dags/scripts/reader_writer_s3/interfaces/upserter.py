# interfaces/upserter.py

from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

class Upserter(ABC):
    @abstractmethod
    def upsert(self, existing_df: Union[pd.DataFrame, SparkDataFrame], new_df: Union[pd.DataFrame, SparkDataFrame]) -> Union[pd.DataFrame, SparkDataFrame]:
        pass

from scripts.data_processor.interfaces.processor_interface import DataProcessorInterface
from scripts.data_processor.backends.pandas import PandasDataProcessor
from scripts.data_processor.backends.pyspark import SparkDataProcessor

class DataProcessorFactory:
    @staticmethod
    def create_processor(backend: str) -> DataProcessorInterface:
        if backend.lower() == 'pandas':
            return PandasDataProcessor()
        elif backend.lower() == 'pyspark':
            return SparkDataProcessor()
        else:
            raise ValueError(f"Unsupported backend: {backend}")


from scripts.reader_writer_s3.interfaces.data_reader import DataReader
from scripts.reader_writer_s3.interfaces.data_writer import DataWriter
from scripts.reader_writer_s3.client.s3 import S3Client
from scripts.reader_writer_s3.backends.pandas import PandasDataReader, PandasDataWriter
from scripts.reader_writer_s3.backends.pyspark import PySparkDataReader, PySparkDataWriter
from pyspark.sql import SparkSession

class DataReaderWriterFactory:
    def __init__(self, backend: str, s3_client: S3Client, spark: SparkSession = None):
        self.backend = backend.lower()
        self.s3_client = s3_client
        self.spark = spark

    def get_reader(self) -> DataReader:
        if self.backend == 'pandas':
            return PandasDataReader(self.s3_client)
        elif self.backend == 'pyspark':
            if not self.spark:
                raise ValueError("SparkSession is required for PySpark backend.")
            return PySparkDataReader(self.spark)
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")

    def get_writer(self) -> DataWriter:
        if self.backend == 'pandas':
            return PandasDataWriter(self.s3_client)
        elif self.backend == 'pyspark':
            if not self.spark:
                raise ValueError("SparkSession is required for PySpark backend.")
            return PySparkDataWriter(self.spark)
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")
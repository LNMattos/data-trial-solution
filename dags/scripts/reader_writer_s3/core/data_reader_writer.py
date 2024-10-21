from scripts.reader_writer_s3.factory.reader_writer_factory import DataReaderWriterFactory
from scripts.reader_writer_s3.client.s3 import S3Client
from scripts.reader_writer_s3.backends.pyspark import get_spark_session
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

class DataReaderWriter:
    def __init__(self, 
                 backend: str,
                 bucket_name: str = "clever-datalake", 
                 endpoint_url: str = "http://minio:9000", 
                 access_key: str = "minioadmin", 
                 secret_key: str = "minioadmin"):
        self.bucket_name = bucket_name
        self.s3_client = S3Client(endpoint_url, access_key, secret_key)
        self.backend = backend.lower()

        if self.backend == 'pyspark':
            self.spark = get_spark_session()
        else:
            self.spark = None

        self.factory = DataReaderWriterFactory(
            backend=self.backend, 
            s3_client=self.s3_client, 
            spark=self.spark
        )

    def read(self, path: str, **kwargs) -> Union[pd.DataFrame, SparkDataFrame]:

        reader = self.factory.get_reader()
        full_path = self._build_s3_path(path)
        if self.backend == 'pyspark':
            return reader.read(full_path, **kwargs)
        else:
            return reader.read(full_path, **kwargs)

    def write(self, data: Union[pd.DataFrame, SparkDataFrame], path: str, **kwargs):

        writer = self.factory.get_writer()
        full_path = self._build_s3_path(path)
        if self.backend == 'pyspark':
            writer.write(data, full_path, **kwargs)
        else:
            writer.write(data, full_path, **kwargs)

    def _build_s3_path(self, path: str) -> str:

        relative_path = path.lstrip('/')
        full_path = f"s3://{self.bucket_name}/{relative_path}"
        return full_path

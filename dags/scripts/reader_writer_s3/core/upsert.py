from scripts.reader_writer_s3.factory.upsert_factory import UpsertFactory
from scripts.reader_writer_s3.factory.reader_writer_factory import DataReaderWriterFactory
from scripts.reader_writer_s3.client.s3 import S3Client
from pyspark.sql import SparkSession
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


class Upsert:
    def __init__(self, 
                 backend: str, 
                 bucket_name: str = "clever-datalake",
                 endpoint_url: str = "http://minio:9000", 
                 access_key: str = "minioadmin", 
                 secret_key: str = "minioadmin"):
        """
        Inicializa a classe Upsert com o nome do bucket e configurações do S3.

        :param bucket_name: Nome do bucket S3.
        :param backend: Backend a ser utilizado ('pyspark' ou outro).
        :param endpoint_url: URL do endpoint S3.
        :param access_key: Chave de acesso S3.
        :param secret_key: Chave secreta S3.
        """
        self.bucket_name = bucket_name
        self.s3_client = S3Client(endpoint_url, access_key, secret_key)
        self.backend = backend.lower()

        if self.backend == 'pyspark':
            self.spark = self._get_spark_session()
        else:
            self.spark = None

        self.factory = DataReaderWriterFactory(
            backend=self.backend, 
            s3_client=self.s3_client, 
            spark=self.spark
        )

    def upsert(self, 
               new_data: Union[pd.DataFrame, SparkDataFrame], 
               target_path: str, 
               upsert_method: str, 
               id_columns: list, 
               modification_column: str = None) -> None:
        """
        Realiza o upsert dos dados no bucket especificado.

        :param new_data: Novos dados a serem inseridos ou atualizados (pandas DataFrame ou Spark DataFrame).
        :param target_path: Caminho relativo dentro do bucket onde os dados alvo estão armazenados.
        :param upsert_method: Método de upsert a ser utilizado.
        :param id_columns: Lista de colunas que identificam unicamente os registros.
        :param modification_column: Coluna que indica a modificação nos registros (opcional).
        """
        try:
            existing_data = self._read_data(target_path)
        except Exception as e:
            existing_data = None

        upserter = UpsertFactory.get_upserter(
            backend=self.backend, 
            upsert_method=upsert_method, 
            id_columns=id_columns, 
            modification_column=modification_column
        )

        if existing_data is None:
            result_data = new_data
        else:
            result_data = upserter.upsert(existing_data, new_data)

        self._write_data(result_data, target_path)

    def _read_data(self, path: str) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Lê dados do bucket especificado.

        :param path: Caminho relativo dentro do bucket.
        :return: DataFrame do pandas ou Spark.
        """
        reader = self.factory.get_reader()
        full_path = self._build_s3_path(path)
        if self.backend == 'pyspark':
            return reader.read(full_path)
        else:
            return reader.read(full_path)

    def _write_data(self, data: Union[pd.DataFrame, SparkDataFrame], path: str) -> None:
        """
        Escreve dados no bucket especificado.

        :param data: Dados a serem escritos (pandas DataFrame ou Spark DataFrame).
        :param path: Caminho relativo dentro do bucket.
        """
        writer = self.factory.get_writer()
        full_path = self._build_s3_path(path)
        if self.backend == 'pyspark':
            writer.write(data, full_path)
        else:
            writer.write(data, full_path)

    def _build_s3_path(self, path: str) -> str:
        relative_path = path.lstrip('/')
        full_path = f"s3://{self.bucket_name}/{relative_path}"
        return full_path

    @staticmethod
    def _get_spark_session() -> SparkSession:
        from reader_writer_s3.backends.pyspark import get_spark_session
        return get_spark_session()


from scripts.reader_writer_s3.interfaces.data_reader import DataReader
from scripts.reader_writer_s3.interfaces.data_writer import DataWriter
from scripts.reader_writer_s3.interfaces.upserter import Upserter
from scripts.reader_writer_s3.client.s3 import S3Client
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import io

class PandasDataReader(DataReader):
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def read(self, path: str, **kwargs) -> pd.DataFrame:
        bucket, key = self._parse_path(path)
        with io.BytesIO() as f:
            self.s3_client.download_fileobj(bucket, key, f)
            f.seek(0)
            if key.endswith('.csv'):
                return pd.read_csv(f, **kwargs)
            elif key.endswith('.parquet'):
                return pd.read_parquet(f, **kwargs)
            else:
                raise ValueError("Unsupported file format for pandas reader.")

    @staticmethod
    def _parse_path(path: str):
        if path.startswith("s3://"):
            path = path.replace("s3://", "")
        parts = path.split('/', 1)
        if len(parts) != 2:
            raise ValueError("Path must be in the format s3://bucket/key")
        return parts[0], parts[1]

class PandasDataWriter(DataWriter):
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def write(self, data: pd.DataFrame, path: str, **kwargs):
        bucket, key = self._parse_path(path)
        with io.BytesIO() as f:
            if key.endswith('.csv'):
                data.to_csv(f, index=False, **kwargs)
            elif key.endswith('.parquet'):
                data.to_parquet(f, index=False, **kwargs)
            else:
                raise ValueError("Unsupported file format for pandas writer.")
            f.seek(0)
            self.s3_client.upload_fileobj(f, bucket, key)

    @staticmethod
    def _parse_path(path: str):
        if path.startswith("s3://"):
            path = path.replace("s3://", "")
        parts = path.split('/', 1)
        if len(parts) != 2:
            raise ValueError("Path must be in the format s3://bucket/key")
        return parts[0], parts[1]

class PandasUpserter(Upserter):
    def __init__(self, strategy: str, id_columns: list, modification_column: str = None):
        self.strategy = strategy
        self.id_columns = id_columns if isinstance(id_columns, list) else [id_columns] 
        self.modification_column = modification_column

    def upsert(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        # Check if id_columns exist in both dataframes
        missing_ids_existing = set(self.id_columns) - set(existing_df.columns)
        missing_ids_new = set(self.id_columns) - set(new_df.columns)
        if missing_ids_existing:
            raise ValueError(f"Existing dataframe is missing id columns: {missing_ids_existing}")
        if missing_ids_new:
            raise ValueError(f"New dataframe is missing id columns: {missing_ids_new}")

        if self.strategy == 'by_id':
            return self._upsert_by_id(existing_df, new_df)
        elif self.strategy == 'by_id_and_modification':
            if not self.modification_column:
                raise ValueError("modification_column should be defined")
            # Check if modification_column exists in both dataframes
            if self.modification_column not in existing_df.columns:
                raise ValueError(f"Existing dataframe is missing modification column: {self.modification_column}")
            if self.modification_column not in new_df.columns:
                raise ValueError(f"New dataframe is missing modification column: {self.modification_column}")
            return self._upsert_by_id_and_modification(existing_df, new_df)
        else:
            raise ValueError(f"Strategy '{self.strategy}' not supported.")

    def _upsert_by_id(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=self.id_columns, keep='last')
        return combined_df

    def _upsert_by_id_and_modification(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        combined_df = pd.concat([existing_df, new_df])
        combined_df.sort_values(by=self.modification_column, ascending=False, inplace=True)
        combined_df.drop_duplicates(subset=self.id_columns, keep='first', inplace=True)
        return combined_df

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, inspect, Table, Column, MetaData
from sqlalchemy.types import (
    Integer, Float, String, Boolean, Date, DateTime, LargeBinary
)
import scripts.constants as c  # Assuming this module contains PostgreSQL credentials
import boto3
import io
import sqlalchemy

class ParquetToPostgres:
    def __init__(
        self,
        backend: str = "pandas",
        bucket_name: str = "clever-datalake",
        endpoint_url: str = "http://minio:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        postgres_user: str = c.postgres_user,
        postgres_password: str = c.postgres_password,
        postgres_host: str = c.postgres_host,
        postgres_port: int = c.postgres_port,
        postgres_dbname: str = c.postgres_dbname
    ):
        """
        Initializes the class with S3 and PostgreSQL credentials.

        :param backend: Choice of backend for processing ('pandas' or 'pyspark').
        :param bucket_name: Name of the S3 bucket.
        :param endpoint_url: S3 endpoint URL.
        :param access_key: S3 access key.
        :param secret_key: S3 secret key.
        :param postgres_user: PostgreSQL user.
        :param postgres_password: PostgreSQL password.
        :param postgres_host: PostgreSQL host.
        :param postgres_port: PostgreSQL port.
        :param postgres_dbname: PostgreSQL database name.
        """
        self.backend = backend.lower()
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

        # Configure PostgreSQL connection
        self.engine = create_engine(
            f'postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_dbname}'
        )

        # Initialize Spark session if needed
        if self.backend == "pyspark":
            self.spark = self.get_spark_session()

    def get_spark_session(self, app_name: str = "ParquetToPostgres") -> SparkSession:
        """
        Creates and returns a Spark session configured to access S3.

        :param app_name: Name of the Spark application.
        :return: Configured SparkSession.
        """
        jars = [
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/airflow/jars/commons-pool2-2.11.1.jar",
            "/opt/airflow/jars/spark-hadoop-cloud_2.12-3.4.1.jar"
        ]

        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", ",".join(jars)) \
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint_url) \
            .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()

        return spark

    def execute(
        self,
        parquet_path: str,
        table_name: str,
        action: str = "append",
        partition_cols: list = None
    ):
        """
        Executes the action of loading data from Parquet to PostgreSQL.

        :param parquet_path: Path to the Parquet file within the S3 bucket.
        :param table_name: Name of the table in PostgreSQL.
        :param action: Action to be performed ('append' or 'replace').
        :param partition_cols: Partition columns (optional, for PySpark).
        """
        if self.backend == "pandas":
            self._execute_pandas(parquet_path, table_name, action)
        elif self.backend == "pyspark":
            self._execute_pyspark(parquet_path, table_name, action, partition_cols)
        else:
            raise ValueError("Unsupported backend. Use 'pandas' or 'pyspark'.")

    def _execute_pandas(self, parquet_key: str, table_name: str, action: str):
        """
        Loads data using pandas and inserts into PostgreSQL with automatic schema inference.

        :param parquet_key: Key to the Parquet file within the S3 bucket.
        :param table_name: Name of the table in PostgreSQL.
        :param action: Action to be performed ('append' or 'replace').
        """
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

        # Construct full S3 path
        bucket = self.bucket_name
        key = parquet_key

        # Download Parquet file into memory
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_data = response['Body'].read()

        # Load DataFrame with pandas
        df = pd.read_parquet(io.BytesIO(parquet_data))

        # Infer and align schema
        self._ensure_table_schema_pandas(table_name, df)

        # Determine write mode
        if action == "replace":
            if_exists = 'replace'
        elif action == "append":
            if_exists = 'append'
        else:
            raise ValueError("Invalid action. Use 'append' or 'replace'.")

        # Insert data into PostgreSQL
        df.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=False, method='multi')

    def _execute_pyspark(self, parquet_key: str, table_name: str, action: str, partition_cols: list):
        """
        Loads data using PySpark and inserts into PostgreSQL with automatic schema inference.

        :param parquet_key: Key to the Parquet file within the S3 bucket.
        :param table_name: Name of the table in PostgreSQL.
        :param action: Action to be performed ('append' or 'replace').
        :param partition_cols: Partition columns (optional).
        """
        # Define full S3 path
        s3_path = f"s3a://{self.bucket_name}/{parquet_key}"

        # Load DataFrame with Spark
        df = self.spark.read.parquet(s3_path)

        # Infer and align schema
        self._ensure_table_schema_pyspark(table_name, df)

        # Define JDBC connection properties
        jdbc_url = f'jdbc:postgresql://{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
        connection_properties = {
            "user": c.postgres_user,
            "password": c.postgres_password,
            "driver": "org.postgresql.Driver"
        }

        # Determine write mode
        if action == "replace":
            write_mode = 'overwrite'
        elif action == "append":
            write_mode = 'append'
        else:
            raise ValueError("Invalid action. Use 'append' or 'replace'.")

        # Write data to PostgreSQL
        df.write.jdbc(url=jdbc_url, table=table_name, mode=write_mode, properties=connection_properties)

    def _ensure_table_schema_pandas(self, table_name: str, df: pd.DataFrame):
        """
        Ensures that the table in PostgreSQL has the correct schema based on the pandas DataFrame.

        :param table_name: Name of the table in PostgreSQL.
        :param df: pandas DataFrame.
        """
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            # Optional: Verify if the schema matches
            # This can be complex; for simplicity, we assume schemas are compatible
            pass
        else:
            # Map pandas types to SQLAlchemy
            dtype_mapping = self._map_pandas_dtypes_to_sqlalchemy(df.dtypes)
            metadata = MetaData()
            columns = [Column(col, dtype, nullable=True) for col, dtype in dtype_mapping.items()]
            table = Table(table_name, metadata, *columns)
            metadata.create_all(self.engine)

    def _ensure_table_schema_pyspark(self, table_name: str, df):
        """
        Ensures that the table in PostgreSQL has the correct schema based on the PySpark DataFrame.

        :param table_name: Name of the table in PostgreSQL.
        :param df: PySpark DataFrame.
        """
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            # Optional: Verify if the schema matches
            # This can be complex; for simplicity, we assume schemas are compatible
            pass
        else:
            # Convert Spark schema to pandas for easier mapping
            pandas_df = df.toPandas()
            self._ensure_table_schema_pandas(table_name, pandas_df)

    def _map_pandas_dtypes_to_sqlalchemy(self, dtypes: pd.Series):
        """
        Maps pandas data types to SQLAlchemy types.

        :param dtypes: pandas data types Series.
        :return: Dictionary mapping columns to SQLAlchemy types.
        """
        dtype_mapping = {}
        for column, dtype in dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                dtype_mapping[column] = Integer
            elif pd.api.types.is_float_dtype(dtype):
                dtype_mapping[column] = Float
            elif pd.api.types.is_bool_dtype(dtype):
                dtype_mapping[column] = Boolean
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                dtype_mapping[column] = DateTime
            elif pd.api.types.is_object_dtype(dtype):
                dtype_mapping[column] = String
            elif pd.api.types.is_categorical_dtype(dtype):
                dtype_mapping[column] = String
            else:
                dtype_mapping[column] = String  # Default type
        return dtype_mapping

    @staticmethod
    def _parse_s3_path(s3_path: str):
        """
        Splits the S3 path into bucket and key.

        :param s3_path: S3 path in the format 's3://bucket_name/path/to/file'.
        :return: Tuple (bucket_name, key).
        """
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]
        parts = s3_path.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        return bucket, key

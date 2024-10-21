# backends/pyspark_data_reader.py

from scripts.reader_writer_s3.interfaces.data_reader import DataReader
from scripts.reader_writer_s3.interfaces.upserter import Upserter
from scripts.reader_writer_s3.interfaces.data_writer import DataWriter
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql import Window


class PySparkDataReader(DataReader):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str, **kwargs) -> SparkDataFrame:
        print(path)
        if path.endswith('.csv'):
            return self.spark.read.csv(path, header=True, inferSchema=True, **kwargs)
        elif path.endswith('.parquet'):
            return self.spark.read.parquet(path, **kwargs)
        else:
            raise ValueError("Unsupported file format for PySpark reader.")

class PySparkDataWriter(DataWriter):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(self, data: SparkDataFrame, path: str, **kwargs):
        if path.endswith('.csv'):
            data.write.csv(path, header=True, mode='overwrite', **kwargs)
        elif path.endswith('.parquet'):
            data.write.parquet(path, mode='overwrite', **kwargs)
        else:
            raise ValueError("Unsupported file format for PySpark writer.")

class PySparkUpserter(Upserter):
    def __init__(self, strategy: str, id_columns: list, modification_column: str = None):
        self.strategy = strategy
        self.id_columns = id_columns
        self.modification_column = modification_column

    def upsert(self, existing_df: SparkDataFrame, new_df: SparkDataFrame) -> SparkDataFrame:
        if self.strategy == 'by_id':
            return self._upsert_by_id(existing_df, new_df)
        elif self.strategy == 'by_id_and_modification':
            return self._upsert_by_id_and_modification(existing_df, new_df)
        else:
            raise ValueError(f"Strategy '{self.strategy}' não suportada.")

    def _upsert_by_id(self, existing_df: SparkDataFrame, new_df: SparkDataFrame) -> SparkDataFrame:
        join_condition = [existing_df[col] == new_df[col] for col in self.id_columns]
        combined_df = existing_df.union(new_df).dropDuplicates(self.id_columns)
        return combined_df

    def _upsert_by_id_and_modification(self, existing_df: SparkDataFrame, new_df: SparkDataFrame) -> SparkDataFrame:
        if self.modification_column is None:
            raise ValueError("modification_column deve ser fornecida para a estratégia 'by_id_and_modification'.")
        
        combined_df = existing_df.union(new_df)
        window = Window.partitionBy(*self.id_columns).orderBy(col(self.modification_column).desc())
        ranked_df = combined_df.withColumn("rank", spark_max(self.modification_column).over(window))
        result_df = ranked_df.filter(col("rank") == col(self.modification_column)).drop("rank")
        return result_df



def get_spark_session(app_name: str = "DataReaderWriterApp") -> SparkSession: 
    jars = "/opt/airflow/jars/hadoop-aws-3.3.4.jar," \
           "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar," \
           "/opt/airflow/jars/commons-pool2-2.11.1.jar," \
           "/opt/airflow/jars/spark-hadoop-cloud_2.12-3.4.1.jar"
    
    spark = SparkSession.builder \
        .appName("ReaderWriterS3") \
        .config("spark.jars", jars) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark

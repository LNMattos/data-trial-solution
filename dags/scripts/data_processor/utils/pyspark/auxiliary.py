from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import FloatType, DoubleType, IntegerType
from pyspark.sql import functions as F

def clean_id_cols(df: DataFrame, id_cols: list) -> DataFrame:
    for col_name in id_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name).cast("double").isNotNull(),
                     F.format_number(col(col_name).cast("double"), 0).cast("string"))
                .otherwise(col(col_name))
            )
    return df

def cast_cols_to_numeric(df: DataFrame, cols_to_cast: list) -> DataFrame:
    for col_name in cols_to_cast:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df

def drop_all_na_rows(df: DataFrame) -> DataFrame:
    return df.dropna(how='all')

def drop_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()

def fill_na_values(df: DataFrame, cols_to_fill: list, fill_value=None) -> DataFrame:

    fill_dict = {col_name: fill_value for col_name in cols_to_fill if col_name in df.columns}
    return df.fillna(fill_dict)

def convert_bool_or_str_to_numeric(df: DataFrame, cols_to_convert: list) -> DataFrame:

    mapping = {
        'True': 1,
        'true': 1,
        'False': 0,
        'false': 0,
        True: 1,
        False: 0
    }
    
    for col_name in cols_to_convert:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name).isin(['True', 'true', True]), 1)
                .when(col(col_name).isin(['False', 'false', False]), 0)
                .otherwise(0)
                .cast(IntegerType())
            )
    return df

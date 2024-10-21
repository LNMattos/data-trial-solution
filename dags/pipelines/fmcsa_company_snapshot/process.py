import os
from datetime import datetime
from scripts.data_processor.core.data_processor import DataProcessor
from scripts.data_processor.utils.pandas.auxiliary import (
    drop_all_na_rows, drop_duplicates, convert_bool_or_str_to_numeric,
    fill_na_values, cast_cols_to_numeric, clean_id_cols
)
from scripts.manager_s3.services.minio_service import MinioService
from scripts.reader_writer_s3.core.data_reader_writer import DataReaderWriter
from scripts.reader_writer_s3.core.upsert import Upsert
from scripts.parquet_to_postgresql import ParquetToPostgres


def process_data(config):
    backend = config.get("backend", "pandas")
    
    file_name = config["file_name"]
    local_file_extension = config["local_file_extension"]
    local_path = config["local_path"]
    local_file_path = os.path.join(local_path, f"{file_name}.{local_file_extension}")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    local_file_path = os.path.join(
        base_dir,
        '..',
        '..',
        local_path,  # 'scripts/data_examples'
        f"{file_name}.{local_file_extension}"
    )
    
    s3_manager = MinioService()
    s3_rw = DataReaderWriter(backend)
    
    date_now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    bucket_raw_ini_dir = f"{config['bucket_raw_ini_dir']}/{file_name}/"
    bucket_raw_end_dir = f"{config['bucket_raw_end_dir']}/{file_name}/{date_now}/"
    
    bucket_ini_file_path = f"{bucket_raw_ini_dir}{file_name}.{local_file_extension}"
    bucket_end_file_path = f"{bucket_raw_end_dir}{file_name}.{local_file_extension}"
    
    s3_manager.upload_file(local_file_path, bucket_ini_file_path)
    
    pd_processor = DataProcessor(backend)
    pd_processor.load_data(s3_rw.read(bucket_ini_file_path))
    
    pd_processor.drop_all_null_columns()
    pd_processor.apply_function(drop_all_na_rows)
    pd_processor.apply_function(drop_duplicates)
    pd_processor.apply_function(
        convert_bool_or_str_to_numeric, 
        cols_to_convert=['hhg_authorization']
    )
    pd_processor.apply_function(
        fill_na_values, 
        cols_to_fill=[
            "total_complaints_2021",
            "total_complaints_2022", 
            "total_complaints_2023", 
            "num_of_trucks", 
            "num_of_tractors", 
            "num_of_trailers"
        ], 
        fill=0
    )
    pd_processor.apply_function(
        cast_cols_to_numeric, 
        cols_to_cast=[
            'total_complaints_2021', 
            'total_complaints_2022', 
            'total_complaints_2023'
        ]
    )
    pd_processor.convert_types({
        "total_complaints_2021": "int32",
        "total_complaints_2022": "int32",
        "total_complaints_2023": "int32",
        "num_of_trucks": "int32",
        "num_of_tractors": "int32",
        "num_of_trailers": "int32"
    })
    pd_processor.convert_column_to_datetime('date_updated')
    pd_processor.convert_column_to_datetime('date_created')

    pd_processor.apply_function(clean_id_cols, id_cols=[
        'usdot_num',
        'mc_num',
    ])
    
    s3_manager.move_file(bucket_ini_file_path, bucket_end_file_path)
    
    bucket_silver_dir = config["bucket_silver_dir"]
    bucket_file_extension = config["bucket_file_extension"]
    bucket_silver_file_path = f"{bucket_silver_dir}/{file_name}.{bucket_file_extension}"
    
    upserter = Upsert(backend)
    new_data = pd_processor.get_data()
    upserter.upsert(
        new_data, 
        target_path=bucket_silver_file_path, 
        upsert_method="by_id", 
        id_columns="usdot_num"
    )
    
    postgres_uploader = ParquetToPostgres()
    postgres_uploader.execute(
        bucket_silver_file_path, 
        config["postgres_table"], 
        action=config["postgres_action"]
    )

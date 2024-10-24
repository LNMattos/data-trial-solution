import os
import logging
from datetime import datetime

from scripts.data_processor.core.data_processor import DataProcessor
from scripts.data_processor.utils.pandas.auxiliary import (
    drop_all_na_rows,
    drop_duplicates,
    convert_bool_or_str_to_numeric,
    fill_na_values,
    cast_cols_to_numeric,
    clean_id_cols
)
from scripts.manager_s3.services.minio_service import MinioService
from scripts.reader_writer_s3.core.data_reader_writer import DataReaderWriter
from scripts.reader_writer_s3.core.upsert import Upsert
from scripts.parquet_to_postgresql import ParquetToPostgres

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_local_file_path(config):
    """
    Set up the local file path based on the configuration.

    Args:
        config (dict): Configuration dictionary containing file details.

    Returns:
        str: The absolute path to the local file.
    """
    file_name = config["file_name"]
    local_file_extension = config["local_file_extension"]
    local_path = config["local_path"]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    local_file_path = os.path.join(
        base_dir,
        '..',
        '..',
        local_path,
        f"{file_name}.{local_file_extension}"
    )
    logging.info(f"Local file path set to: {local_file_path}")
    return local_file_path


def initialize_services(config):
    """
    Initialize necessary services like S3 manager and data reader/writer.

    Args:
        config (dict): Configuration dictionary.

    Returns:
        tuple: Initialized S3 manager, data reader/writer, and backend type.
    """
    backend = config.get("backend", "pandas")
    s3_manager = MinioService()
    s3_rw = DataReaderWriter(backend)
    logging.info("Initialized MinioService and DataReaderWriter")
    return s3_manager, s3_rw, backend


def upload_initial_file(s3_manager, local_file_path, config):
    """
    Upload the initial local file to the raw S3 bucket.

    Args:
        s3_manager (MinioService): Instance of MinioService.
        local_file_path (str): Path to the local file.
        config (dict): Configuration dictionary.

    Returns:
        tuple: S3 initial file path and the current date-time string.
    """
    date_now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    file_name = config["file_name"]
    local_file_extension = config["local_file_extension"]
    bucket_raw_ini_dir = f"{config['bucket_raw_ini_dir']}/{file_name}/"
    bucket_ini_file_path = f"{bucket_raw_ini_dir}{file_name}.{local_file_extension}"

    s3_manager.upload_file(local_file_path, bucket_ini_file_path)
    logging.info(f"Uploaded file to S3 at: {bucket_ini_file_path}")
    return bucket_ini_file_path, date_now


def move_initial_file(s3_manager, bucket_ini_file_path, config, date_now):
    """
    Move the initial file to the processed raw S3 bucket.

    Args:
        s3_manager (MinioService): Instance of MinioService.
        bucket_ini_file_path (str): Initial S3 file path.
        config (dict): Configuration dictionary.
        date_now (str): Current date-time string.

    Returns:
        str: S3 end file path after moving.
    """
    file_name = config["file_name"]
    local_file_extension = config["local_file_extension"]
    bucket_raw_end_dir = f"{config['bucket_raw_end_dir']}/{file_name}/{date_now}/"
    bucket_end_file_path = f"{bucket_raw_end_dir}{file_name}.{local_file_extension}"

    s3_manager.move_file(bucket_ini_file_path, bucket_end_file_path)
    logging.info(f"Moved file from {bucket_ini_file_path} to {bucket_end_file_path}")
    return bucket_end_file_path


def process_silver(s3_rw, bucket_end_file_path, backend):
    """
    Process data for the Silver layer by cleaning and transforming.

    Args:
        s3_rw (DataReaderWriter): Instance of DataReaderWriter.
        bucket_ini_file_path (str): S3 initial file path.
        backend (str): Backend type (e.g., 'pandas').

    Returns:
        DataProcessor: Processed DataProcessor instance.
    """
    pd_processor = DataProcessor(backend)
    data = s3_rw.read(bucket_end_file_path)
    pd_processor.load_data(data)
    logging.info("Loaded data into DataProcessor for Silver processing")

    # Apply data cleaning functions
    pd_processor.drop_all_null_columns()
    pd_processor.apply_function(drop_all_na_rows)
    pd_processor.apply_function(drop_duplicates)
    pd_processor.apply_function(
        convert_bool_or_str_to_numeric,
        cols_to_convert=['hhg_authorization']
    )
    pd_processor.apply_function(
        fill_na_values,
        cols_to_fill=["complaint_count"],
        fill=0
    )
    pd_processor.apply_function(
        cast_cols_to_numeric,
        cols_to_cast=['complaint_count']
    )
    pd_processor.convert_types({
        "complaint_count": "int32",
    })
    pd_processor.convert_column_to_datetime('date_created')
    pd_processor.apply_function(clean_id_cols, id_cols=[
        'usdot_num',
        'id',
    ])
    logging.info("Applied Silver processing transformations")
    return pd_processor


def upsert_silver(pd_processor, config, backend):
    """
    Upsert the processed Silver data to the Silver S3 bucket and Postgres.

    Args:
        pd_processor (DataProcessor): Processed DataProcessor instance.
        config (dict): Configuration dictionary.
        backend (str): Backend type (e.g., 'pandas').

    Returns:
        str: S3 Silver file path after upserting.
    """
    bucket_silver_dir = config["bucket_silver_dir"]
    bucket_file_extension = config["bucket_file_extension"]
    file_name = config["file_name"]
    bucket_silver_file_path = f"{bucket_silver_dir}/{file_name}.{bucket_file_extension}"

    upserter = Upsert(backend)
    new_data = pd_processor.get_data()
    upserter.upsert(
        new_data,
        target_path=bucket_silver_file_path,
        upsert_method="by_id",
        id_columns="usdot_num"
    )
    logging.info(f"Upserted Silver data to {bucket_silver_file_path}")

    postgres_uploader = ParquetToPostgres()
    postgres_uploader.execute(
        bucket_silver_file_path,
        config["postgres_table"],
        action=config["postgres_action"]
    )
    logging.info("Uploaded Silver data to Postgres")
    return bucket_silver_file_path


def process_pipeline(config):
    """
    Orchestrator function to manage the data processing workflow.

    Args:
        config (dict): Configuration dictionary containing all necessary parameters.
    """
    try:
        # Setup local file path
        local_file_path = setup_local_file_path(config)

        # Initialize services
        s3_manager, s3_rw, backend = initialize_services(config)

        # Upload initial file to S3
        bucket_ini_file_path, date_now = upload_initial_file(s3_manager, local_file_path, config)

        # Move initial file to processed raw directory
        bucket_end_file_path = move_initial_file(s3_manager, bucket_ini_file_path, config, date_now)

        logging.info("Started Silver processing")
        # Silver processing
        pd_silver = process_silver(s3_rw, bucket_end_file_path, backend)
        bucket_silver_file_path = upsert_silver(pd_silver, config, backend)
        logging.info("Finished Silver processing")

    except Exception as e:
        logging.error(f"An error occurred during data processing: {e}")
        raise


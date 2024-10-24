import os
import logging
from datetime import datetime

from scripts.data_processor.core.data_processor import DataProcessor
from scripts.data_processor.utils.pandas.json_extractor import add_working_hours_columns, add_about_columns
from scripts.data_processor.utils.pandas.auxiliary import (
    drop_all_na_rows,
    drop_duplicates,
    fill_na_values,
    cast_cols_to_numeric,
    clean_id_cols,
    convert_bool_or_str_to_numeric
)
from scripts.data_processor.utils.pandas.score import calculate_company_score

from scripts.manager_s3.services.minio_service import MinioService
from scripts.reader_writer_s3.core.data_reader_writer import DataReaderWriter
from scripts.reader_writer_s3.core.upsert import Upsert
from scripts.parquet_to_postgresql import ParquetToPostgres

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_local_file_path(config):
    """
    Set up the local file path based on the configuration.
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
    """
    backend = config.get("backend", "pandas")
    s3_manager = MinioService()
    s3_rw = DataReaderWriter(backend)
    logging.info("Initialized MinioService and DataReaderWriter")
    return s3_manager, s3_rw, backend


def upload_initial_file(s3_manager, local_file_path, config):
    """
    Upload the initial local file to the raw S3 bucket.
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
    """
    file_name = config["file_name"]
    local_file_extension = config["local_file_extension"]
    bucket_raw_end_dir = f"{config['bucket_raw_end_dir']}/{file_name}/{date_now}/"
    bucket_end_file_path = f"{bucket_raw_end_dir}{file_name}.{local_file_extension}"
    
    s3_manager.move_file(bucket_ini_file_path, bucket_end_file_path)
    logging.info(f"Moved file from {bucket_ini_file_path} to {bucket_end_file_path}")
    return bucket_end_file_path


def process_silver(s3_rw, bucket_ini_file_path, backend):
    """
    Process data for the Silver layer by cleaning and transforming.
    """
    pd_processor = DataProcessor(backend)
    data = s3_rw.read(bucket_ini_file_path, escapechar="\\")
    pd_processor.load_data(data)
    logging.info("Loaded data into DataProcessor for Silver processing")
    
    # Apply data cleaning functions
    pd_processor.drop_all_null_columns()
    pd_processor.apply_function(drop_all_na_rows)
    pd_processor.apply_function(drop_duplicates)
    pd_processor.apply_function(
        fill_na_values, 
        cols_to_fill=["reviews", "photos_count"], 
        fill=0
    )
    pd_processor.apply_function(
        cast_cols_to_numeric, 
        cols_to_cast=["latitude", "longitude", "rating", "photos_count", "postal_code"]
    )
    pd_processor.convert_types({
        "latitude": "float64",
        "longitude": "float64",
        "rating": "float32",
        "reviews": "int64",
        "photos_count": "int64"
    })
    pd_processor.apply_function(
        clean_id_cols, 
        id_cols=[
            "google_id",
            "owner_id",
            "place_id",
            "cid",
            "reviews_id",
            "located_google_id",
            "postal_code"
        ]
    )
    logging.info("Applied Silver processing transformations")
    return pd_processor


def upsert_silver(pd_processor, config, backend):
    """
    Upsert the processed Silver data to the Silver S3 bucket and Postgres.
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
        id_columns=["google_id"]
    )
    logging.info(f"Upserted Silver data to {bucket_silver_file_path}")
    
    postgres_uploader = ParquetToPostgres()
    postgres_uploader.execute(
        bucket_silver_file_path, 
        f"trusted_{file_name}", 
        action=config["postgres_action_silver"]
    )
    logging.info("Uploaded Silver data to Postgres")
    return bucket_silver_file_path


def process_gold(s3_rw, bucket_silver_file_path, backend):
    """
    Process data for the Gold layer by applying JSON extraction and transformations.
    """
    company_profiles_cols = [
        "google_id",
        "name",
        "site",
        "subtypes",
        "type",
        "category",
        "phone",
        "full_address",
        "borough",
        "street",
        "city",
        "postal_code",
        "state",
        "us_state",
        "country",
        "country_code",
        "latitude",
        "longitude",
        "time_zone",
        "area_service",
        "reviews_tags",
        "photo",
        "photos_count",
        "street_view",
        "located_in",
        "working_hours",
        "other_hours",
        "business_status",
        "about",
        "posts",
        "logo",
        "description",
        "verified",
        "owner_id",
        "owner_title",
        "owner_link",
        "location_link",
        "place_id",
        "cid",
        "reviews",
        "rating"
    ]
    
    pd_processor = DataProcessor(backend)
    data = s3_rw.read(bucket_silver_file_path)[company_profiles_cols]
    pd_processor.load_data(data)
    logging.info("Loaded data into DataProcessor for Gold processing")
    
    # Apply transformations
    pd_processor.filter_rows(lambda df: df['business_status'] == "OPERATIONAL")
    pd_processor.apply_function(add_working_hours_columns, json_column='working_hours')
    pd_processor.apply_function(add_about_columns, json_column="about")
    pd_processor.apply_function(convert_bool_or_str_to_numeric, cols_to_convert=["verified"])
    logging.info("Applied Gold processing transformations")
    
    return pd_processor


def upsert_gold(pd_processor, config, backend):
    """
    Upsert the processed Gold data to the Gold S3 bucket and Postgres.
    """
    bucket_gold_dir = config["bucket_gold_dir"]
    bucket_file_extension = config["bucket_file_extension"]
    file_name = config["file_name"]
    bucket_gold_file_path = f"{bucket_gold_dir}/{file_name}.{bucket_file_extension}"
    
    upserter = Upsert(backend)
    new_data = pd_processor.get_data()
    upserter.upsert(
        new_data, 
        target_path=bucket_gold_file_path,
        upsert_method="by_id",
        id_columns=["google_id"],
    )
    logging.info(f"Upserted Gold data to {bucket_gold_file_path}")
    
    postgres_uploader = ParquetToPostgres()
    postgres_uploader.execute(
        bucket_gold_file_path, 
        config["postgres_table_gold"], 
        action=config["postgres_action_gold"]
    )
    logging.info("Uploaded Gold data to Postgres")
    return bucket_gold_file_path


def process_score(pd_processor):
    """
    Calculate company profile scores.
    """
    pd_processor.apply_function(calculate_company_score, drop_intermediate_cols=False, drop_non_used_cols=True)
    pd_processor.filter_rows(lambda df: df['company_profile_score'].notna())
    logging.info("Calculated company profile scores")
    return pd_processor


def upsert_score(pd_processor, config, backend):
    """
    Upsert the Score data to the Score S3 bucket and Postgres.
    """
    bucket_gold_dir = config["bucket_gold_dir"]
    bucket_file_extension = config["bucket_file_extension"]
    score_file_name = "score_company_profiles_google_maps"
    score_file_path = f"{bucket_gold_dir}/{score_file_name}.{bucket_file_extension}"
    
    upserter = Upsert(backend)
    new_data = pd_processor.get_data()
    upserter.upsert(
        new_data, 
        target_path=score_file_path,
        upsert_method="by_id",
        id_columns=["google_id"],
    )
    logging.info(f"Upserted Score data to {score_file_path}")
    
    postgres_uploader = ParquetToPostgres()
    postgres_uploader.execute(
        score_file_path, 
        config["postgres_table_score"], 
        action=config["postgres_action_score"]
    )
    logging.info("Uploaded Score data to Postgres")


def process_pipeline(config):
    """
    Orchestrator function to manage the data processing workflow.
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
        
        logging.info("Started Gold processing")
        # Gold processing
        pd_gold = process_gold(s3_rw, bucket_silver_file_path, backend)
        bucket_gold_file_path = upsert_gold(pd_gold, config, backend)
        logging.info("Finished Gold processing")
        
        logging.info("Started Score processing")
        # Score processing
        pd_score = process_score(pd_gold)
        upsert_score(pd_score, config, backend)
        logging.info("Finished Score processing")
        
    except Exception as e:
        logging.error(f"An error occurred during data processing: {e}")
        raise
# services/minio_service.py
import boto3
from botocore.exceptions import ClientError
from typing import List
from scripts.manager_s3.interfaces.s3_interface import S3Interface

class MinioService(S3Interface):
    def __init__(self,
                 endpoint_url: str = "http://minio:9000", 
                 access_key: str = "minioadmin", 
                 secret_key: str = "minioadmin",
                 bucket_name: str = "clever-datalake"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def list_files(self, directory: str) -> List[str]:
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=directory)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            print(f"Error listing files: {e}")
            return []

    def move_file(self, source: str, destination: str) -> None:
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=destination)
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=source)
            print(f"File moved from {source} to {destination}")
        except ClientError as e:
            print(f"Erro moving file: {e}")

    def upload_file(self, file_path: str, bucket_path: str) -> None:
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, bucket_path)
            print(f"File {file_path} sent to {bucket_path}")
        except ClientError as e:
            print(f"Erro in file upload: {e}")

    def file_exists(self, file_key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"Error checking file {e}")
                raise

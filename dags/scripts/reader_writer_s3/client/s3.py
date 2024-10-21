import boto3

class S3Client:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def download_fileobj(self, Bucket: str, Key: str, Fileobj):
        self.s3.download_fileobj(Bucket, Key, Fileobj)

    def upload_fileobj(self, Fileobj, Bucket: str, Key: str):
        self.s3.upload_fileobj(Fileobj, Bucket, Key)

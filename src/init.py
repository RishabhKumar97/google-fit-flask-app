import os
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

s3_dir = 'activity-metric-plots'
local_dir = 'data_files'

class S3Client:
    def __init__(self, bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        self.s3 = boto3.client('s3', 
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key
        )
        self.bucket_name = bucket_name

    def get_files(self, s3_dir: str) -> list[str]:
        response = self.s3.list_objects_v2(Bucket=self.bucket_name)
        files = []
        for object in response.get('Contents', []):
            if object['Key'].startswith(s3_dir):
                files.append(object['Key'])
        return files

    def download_file(self, key: str, local_path: str) -> None:
        self.s3.download_file(self.bucket_name, key, local_path)
        print(f"Downloaded {key} to {local_path}")

def refresh_data_files_from_s3() -> None:
    os.makedirs(os.path.join(os.getcwd(), local_dir), exist_ok=True)

    s3_client = S3Client(
        bucket_name=os.getenv('AWS_S3_BUCKET_NAME'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    files = s3_client.get_files(s3_dir= s3_dir)
    file_download_futures = list()
    with ThreadPoolExecutor(len(files)) as e:
        for file_key in files:
            local_path = os.path.join(os.getcwd(), local_dir, os.path.basename(file_key))
            file_download_futures.append(
                e.submit(
                s3_client.download_file, 
                file_key, 
                local_path
                )
            )
    for future in as_completed(file_download_futures):
        future.result()

if __name__ == '__main__':
    refresh_data_files_from_s3()

    
        

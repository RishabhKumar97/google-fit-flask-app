import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from dotenv import load_dotenv

load_dotenv()

s3_dir = "activity-metric-plots"
local_dir = "data_files"


class S3Client:
    def __init__(
        self,
        bucket_name: str = os.getenv("AWS_S3_BUCKET_NAME"),
        aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY"),
    ) -> None:
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.bucket_name = bucket_name

    def get_files(self, s3_dir: str) -> list[str]:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        files = []
        for object in response.get("Contents", []):
            if object["Key"].startswith(s3_dir):
                files.append(object["Key"])
        return files

    def download_file_from_bucket(self, key: str, local_path: str) -> None:
        self.s3_client.download_file(self.bucket_name, key, local_path)
        print(f"Downloaded {key} to {local_path}")

    def refresh_data_files_from_s3(self) -> None:
        global s3_dir
        os.makedirs(os.path.join(os.getcwd(), local_dir), exist_ok=True)
        files = self.get_files(s3_dir=s3_dir)
        file_download_futures = list()
        with ThreadPoolExecutor(len(files)) as e:
            for file_key in files:
                local_path = os.path.join(
                    os.getcwd(), local_dir, os.path.basename(file_key)
                )
                file_download_futures.append(
                    e.submit(
                        self.download_file_from_bucket,
                        key=file_key,
                        local_path=local_path,
                    )
                )
        for future in as_completed(file_download_futures):
            future.result()

    def generate_presigned_file_upload_url(self, file_name: str, expiration=3000):
        """
        Generate a presigned URL S3 POST request to upload a file
        """

        # Generate a presigned S3 file upload URL

        try:
            response = self.s3_client.generate_presigned_post(
                self.bucket_name,
                file_name,
            )
            return response
        except Exception as e:
            print(f"Error generating presigned URL: {e}")
            return None


s3_client = S3Client()
if __name__ == "__main__":
    # print(s3_client.__dict__)
    s3_client.refresh_data_files_from_s3()
    # print(s3_client.generate_presigned_file_upload_url(file_name="test_upload.txt"))

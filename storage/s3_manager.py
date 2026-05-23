"""
ShopFlow — AWS S3 Storage Utility
Handles uploading and downloading Parquet files to/from S3.
Bucket layout:
    s3://<bucket>/raw/<table>/YYYY/MM/DD/<file>.parquet
    s3://<bucket>/processed/<table>/<file>.parquet
    s3://<bucket>/marts/<mart>/<file>.parquet
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from loguru import logger

from ingestion.config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
)


class S3Manager:
    def __init__(self):
        if not AWS_ACCESS_KEY_ID:
            raise ValueError("AWS credentials not set. Check your .env file.")
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        self.bucket = S3_BUCKET

    def create_bucket_if_not_exists(self) -> None:
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket {self.bucket} already exists")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                if AWS_REGION == "us-east-1":
                    self.s3.create_bucket(Bucket=self.bucket)
                else:
                    self.s3.create_bucket(
                        Bucket=self.bucket,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
                    )
                # Block all public access
                self.s3.put_public_access_block(
                    Bucket=self.bucket,
                    PublicAccessBlockConfiguration={
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    },
                )
                logger.success(f"Created S3 bucket: {self.bucket}")

    def upload_directory(self, local_dir: str, s3_prefix: str) -> list[str]:
        """Upload all files in a directory to S3 under s3_prefix."""
        uploaded = []
        for path in Path(local_dir).rglob("*.parquet"):
            relative = path.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{relative}".replace("\\", "/")
            self.s3.upload_file(str(path), self.bucket, s3_key)
            logger.info(f"  Uploaded s3://{self.bucket}/{s3_key}")
            uploaded.append(s3_key)
        return uploaded

    def download_file(self, s3_key: str, local_path: str) -> None:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self.s3.download_file(self.bucket, s3_key, local_path)
        logger.info(f"  Downloaded s3://{self.bucket}/{s3_key} → {local_path}")

    def list_files(self, prefix: str) -> list[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def upload_marts(self, marts_dir: str) -> None:
        """Upload all mart Parquet files to S3."""
        logger.info("Uploading marts to S3...")
        self.create_bucket_if_not_exists()
        keys = self.upload_directory(marts_dir, "marts")
        logger.success(f"Uploaded {len(keys)} mart files to S3")


if __name__ == "__main__":
    from ingestion.config import MARTS_DIR
    mgr = S3Manager()
    mgr.upload_marts(MARTS_DIR)

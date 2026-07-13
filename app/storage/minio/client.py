from minio import Minio
from minio.error import S3Error

from app.common.config.settings import settings
from app.common.logger.logger import logger


class MinioManager:
    """
    Manages the connection to the MinIO Object Storage (Data Lake).
    Ensures required buckets (bronze, silver, gold) exist upon startup.
    """
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        self.buckets = ["bronze", "silver", "gold"]

    def connect(self) -> None:
        """Verifies connection and creates base buckets if missing."""
        try:
            for bucket in self.buckets:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created missing MinIO bucket: {bucket}")
            logger.info("MinIO Data Lake connected successfully.")
        except S3Error as e:
            logger.error(f"MinIO Connection Error: {e}")
            raise e

# Global instance
minio_client = MinioManager()
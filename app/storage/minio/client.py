from minio import Minio
from minio.error import S3Error

from app.common.config.settings import settings
from app.common.logger.logger import logger


class MinioManager:
    """
    Manages the connection to MinIO and ensures required buckets are created on startup.
    This service is the source of truth for bucket creation.
    """
    # Bucket names are defined here to align with the system architecture.
    RAW_ASSETS_BUCKET = "raw-assets"
    EXPORTS_BUCKET = "exports"

    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=settings.MINIO_SECURE,
        )
        self.buckets_to_create = [
            self.RAW_ASSETS_BUCKET,
            self.EXPORTS_BUCKET,
        ]

    def connect(self) -> None:
        """Verifies connection to MinIO and creates buckets if they do not exist."""
        try:
            logger.info("Connecting to MinIO at %s and ensuring buckets exist...", settings.MINIO_ENDPOINT)

            for bucket in self.buckets_to_create:
                if not self.client.bucket_exists(bucket):
                    logger.info(
                        "MinIO bucket '%s' not found. Creating it now.",
                        bucket,
                    )
                    self.client.make_bucket(bucket)
                    logger.info("Successfully created MinIO bucket: '%s'", bucket)
                else:
                    logger.debug("MinIO bucket '%s' already exists. Skipping creation.", bucket)

            logger.info("Successfully connected to MinIO and verified buckets: %s", self.buckets_to_create)
        except S3Error as e:
            logger.error("Could not connect to MinIO or create buckets: %s", e)
            raise e

# Global instance
minio_client = MinioManager()

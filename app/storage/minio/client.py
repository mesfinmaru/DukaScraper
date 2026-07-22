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
        # This list should reflect the buckets created by the minio-init service
        self.buckets = settings.MINIO_BUCKETS.split(',') if settings.MINIO_BUCKETS else []

    def connect(self) -> None:
        """Verifies connection to the MinIO server."""
        try:
            # A lightweight check to confirm connectivity.
            # Bucket creation is handled by the 'minio-init' container.
            if self.buckets and not self.client.bucket_exists(self.buckets[0]):
                logger.warning(
                    "MinIO connected, but bucket '%s' not found. Is minio-init running?",
                    self.buckets[0],
                )
        except S3Error as e:
            logger.error(f"MinIO Connection Error: {e}")
            raise e

# Global instance
minio_client = MinioManager()

from minio import Minio

from app.common.config.settings import settings


def init_minio_buckets():
    """Initializes all required MinIO buckets for the scraping pipeline."""
    try:
        client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )

        # Collect buckets from your environment config
        buckets = [
            settings.MINIO_RAW_BUCKET,
            settings.MINIO_PARSED_BUCKET,
            "cleaned-data",
            "failed-data",
        ]

        for bucket in buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✅ Created MinIO bucket: {bucket}")
            else:
                print(f"ℹ️ MinIO bucket already exists: {bucket}")

    except Exception as e:
        print(f"❌ Failed to initialize MinIO buckets: {e}")


if __name__ == "__main__":
    init_minio_buckets()

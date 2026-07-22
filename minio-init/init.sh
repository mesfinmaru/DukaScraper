#!/bin/sh

# Exit immediately if a command exits with a non-zero status
set -e

echo "Starting MinIO initialization process..."

# 1. Configure the MinIO Client (mc) alias pointing to your MinIO service
# It falls back to default admin credentials if the environment variables are not loaded
ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}

echo "Connecting to MinIO server at http://minio:9000..."
mc alias set myminio http://minio:9000 "$ROOT_USER" "$ROOT_PASSWORD"

# 2. Wait until MinIO is fully responsive
echo "Waiting for MinIO service to become healthy..."
until mc ready myminio; do
    echo "MinIO is not ready yet. Retrying in 2 seconds..."
    sleep 2
done

echo "MinIO server is up and running!"

# 3. Create your application's default buckets (if they don't already exist)
# Adjust or add bucket names here depending on what your scraper expects
DEFAULT_BUCKETS="duka-raw-data duka-parsed-data duka-logs"

for BUCKET in $DEFAULT_BUCKETS; do
    if ! mc ls myminio/$BUCKET > /dev/null 2>&1; then
        echo "Creating bucket: $BUCKET"
        mc mb myminio/$BUCKET
        # Optional: Set the policy to download/public if needed by your frontend, 
        # otherwise leave it private (default).
        # mc policy set download myminio/$BUCKET
    else
        echo "Bucket '$BUCKET' already exists. Skipping creation."
    fi
done

echo "MinIO initialization completed successfully!"
exit 0
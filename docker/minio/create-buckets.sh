#!/bin/sh
# Waits for MinIO, then creates the buckets the pipeline uses.
set -e
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  echo "waiting for minio..."; sleep 2
done
mc mb -p local/bronze local/silver local/quarantine || true
echo "buckets ready: bronze, silver, quarantine"

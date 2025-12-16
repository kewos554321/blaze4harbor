"""Upload task directory to Google Cloud Storage."""

import logging
import sys
from pathlib import Path

from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# TODO: Read from environment variables
BUCKET_NAME = "tb-results"


def upload_task_dir_to_gcs(task_dir: Path, bucket_name: str = BUCKET_NAME) -> bool:
    """Upload task directory to GCS. Returns True on success."""
    logger.info("Uploading to GCS bucket '%s' from '%s'", bucket_name, task_dir)

    if not task_dir.exists() or not task_dir.is_dir():
        logger.error(
            "Task directory not found.\n"
            "  Path: %s\n"
            "  Please ensure the directory exists.",
            task_dir
        )
        return False

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        uploaded_count = 0
        error_count = 0

        for path in task_dir.rglob("*"):
            if not path.is_file():
                continue

            relative_path = path.relative_to(task_dir).as_posix()
            blob_name = f"{task_dir.name}/{relative_path}"

            try:
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(str(path))
                uploaded_count += 1
                logger.info("Uploaded %s", blob_name)
            except Exception as e:
                error_count += 1
                logger.error(
                    "Failed to upload file.\n"
                    "  File: %s\n"
                    "  Error: %s",
                    blob_name, e
                )

        if uploaded_count == 0:
            logger.warning("No files found in task directory")
            return False

        logger.info(
            "Successfully uploaded %d files to gs://%s/%s/",
            uploaded_count, bucket_name, task_dir.name
        )
        return error_count == 0

    except Exception as e:
        logger.error(
            "Failed to upload to GCS.\n"
            "  Bucket: %s\n"
            "  Error: %s",
            bucket_name, e
        )
        return False


def main(argv: list[str]) -> int:
    """CLI entry point. Returns exit code."""
    if len(argv) < 2:
        logger.error(
            "Missing required argument: task_dir\n"
            "  Usage: gcs_upload.py <task_dir> [bucket_name]\n"
            "  Default bucket: %s",
            BUCKET_NAME
        )
        return 1

    task_dir = Path(argv[1])
    bucket_name = argv[2] if len(argv) > 2 else BUCKET_NAME

    success = upload_task_dir_to_gcs(task_dir, bucket_name)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))

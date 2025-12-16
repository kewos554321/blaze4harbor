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

# TODO: Later, read this from .env / environment variables, e.g. os.environ["GCS_BUCKET_NAME"]
BUCKET_NAME = "tb-results"


def upload_task_dir_to_gcs(task_dir: Path) -> None:
    """Upload all files under the given task directory to a GCS bucket."""
    bucket_name = BUCKET_NAME
    logger.info("Uploading task directory '%s' to GCS bucket '%s'", task_dir, bucket_name)

    if not task_dir.exists() or not task_dir.is_dir():
        logger.warning("Task directory does not exist or is not a directory: %s", task_dir)
        return

    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    uploaded_count = 0
    for path in task_dir.rglob("*"):
        if not path.is_file():
            continue

        # object name: "<task_dir.name>/<relative/path/to/file>"
        relative_path = path.relative_to(task_dir).as_posix()
        blob_name = f"{task_dir.name}/{relative_path}"
        gcs_path = f"gs://{bucket_name}/{blob_name}"

        try:
            # Upload file to GCS
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(path))
            uploaded_count += 1
            logger.info("Uploaded %s -> %s", path, gcs_path)
        except Exception as e:
            logger.error("Error uploading file %s to %s: %s", path, gcs_path, e)

    if uploaded_count == 0:
        logger.warning("No files found to upload in task directory.")
    else:
        logger.info("Finished uploading %d files to gs://%s/%s/", uploaded_count, bucket_name, task_dir.name)


def main(argv: list[str]) -> None:
    """CLI entry point for uploading task directories to GCS."""
    if len(argv) < 2:
        logger.error("Missing required argument: task_dir\n  Usage: gcs_upload.py <task_dir>")
        sys.exit(1)

    task_dir = Path(argv[1])
    upload_task_dir_to_gcs(task_dir)


if __name__ == "__main__":
    main(sys.argv)


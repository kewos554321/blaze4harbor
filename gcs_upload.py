import sys
from pathlib import Path

from absl import app
from google3.pyglib import gfile


# TODO: Later, read this from .env / environment variables, e.g. os.environ["GCS_BUCKET_NAME"]
BUCKET_NAME = "tb-results"


def upload_task_dir_to_gcs(task_dir: Path) -> None:
    """
    Upload all files under the given task directory to a GCS bucket.

    Details:
    - Bucket name is taken from BUCKET_NAME constant.
    - Object name format: "<task_dir.name>/<relative_file_path>", e.g.
      task_dir = ".../2025-12-03__13-19-28/hello-world__NNnT6rY"
      result.json -> "hello-world__NNnT6rY/result.json"
    """
    bucket_name = BUCKET_NAME
    print(f"\nUploading task directory '{task_dir}' to GCS bucket '{bucket_name}' ...")

    if not task_dir.exists() or not task_dir.is_dir():
        print(f"Warning: task directory does not exist or is not a directory: {task_dir}", file=sys.stderr)
        return

    uploaded_count = 0
    for path in task_dir.rglob("*"):
        if not path.is_file():
            continue

        # object name: "<task_dir.name>/<relative/path/to/file>"
        relative_path = path.relative_to(task_dir).as_posix()
        blob_name = f"{task_dir.name}/{relative_path}"
        gcs_path = f"gs://{bucket_name}/{blob_name}"

        try:
            gfile.Copy(str(path), gcs_path)
            uploaded_count += 1
            print(f"Uploaded {path} -> {gcs_path}")
        except Exception as e:
            print(f"Error uploading file {path} to {gcs_path}: {e}", file=sys.stderr)

    if uploaded_count == 0:
        print("No files found to upload in task directory.", file=sys.stderr)
    else:
        print(f"Finished uploading {uploaded_count} files to gs://{bucket_name}/{task_dir.name}/")


def main(argv):
    """CLI entry point for uploading task directories to GCS."""
    if len(argv) < 2:
        print("Usage: gcs_upload.py <task_dir>", file=sys.stderr)
        sys.exit(1)

    task_dir = Path(argv[1])
    upload_task_dir_to_gcs(task_dir)


if __name__ == "__main__":
    app.run(main)

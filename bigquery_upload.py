"""Upload harbor results to BigQuery."""

import json
import logging
import sys
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Default BigQuery settings
# TODO: Read from environment variables
DATASET_ID = "tb_results"
TABLE_ID = "tb_results_table"
DATASET_LOCATION = "US"

# BigQuery table schema
SCHEMA = (
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("n_total_trials", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("stats", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("task_dir_name", "STRING", mode="NULLABLE"),
)


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str) -> bigquery.DatasetReference:
    """Ensure dataset exists, create if not found."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset '%s' exists", dataset_id)
    except NotFound:
        logger.info("Creating dataset '%s'", dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = DATASET_LOCATION
        client.create_dataset(dataset, exists_ok=True)
        logger.info("Created dataset '%s'", dataset_id)
    return dataset_ref


def ensure_table_exists(client: bigquery.Client, dataset_ref: bigquery.DatasetReference, table_id: str) -> bigquery.TableReference:
    """Ensure table exists, create if not found."""
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        logger.info("Table '%s' exists", table_id)
    except NotFound:
        logger.info("Creating table '%s'", table_id)
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)
        logger.info("Created table '%s'", table_id)
    return table_ref


def flatten_result_data(result_data: dict, task_dir: Path) -> dict:
    """Flatten nested JSON structure for BigQuery insertion."""
    stats = result_data.get("stats")
    return {
        "id": result_data.get("id"),
        "started_at": result_data.get("started_at"),
        "finished_at": result_data.get("finished_at"),
        "n_total_trials": result_data.get("n_total_trials"),
        "stats": json.dumps(stats) if stats else None,
        "task_dir_name": task_dir.name,
    }


def upload_result_to_bigquery(
    result_data: dict,
    task_dir: Path,
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
) -> bool:
    """Upload result data to BigQuery. Returns True on success."""
    logger.info("Uploading to BigQuery '%s.%s' from '%s'", dataset_id, table_id, task_dir)

    try:
        client = bigquery.Client()
        dataset_ref = ensure_dataset_exists(client, dataset_id)
        table_ref = ensure_table_exists(client, dataset_ref, table_id)

        row = flatten_result_data(result_data, task_dir)
        errors = client.insert_rows_json(table_ref, [row])

        if errors:
            logger.error(
                "Failed to insert row to BigQuery.\n"
                "  Table: %s.%s\n"
                "  Errors: %s",
                dataset_id, table_id, errors
            )
            return False

        logger.info("Successfully uploaded to %s.%s", dataset_id, table_id)
        return True

    except Exception as e:
        logger.error(
            "Failed to upload to BigQuery.\n"
            "  Table: %s.%s\n"
            "  Error: %s",
            dataset_id, table_id, e
        )
        return False


def load_result_json(task_dir: Path) -> dict | None:
    """Load result.json from task directory. Returns None on error."""
    result_json_path = task_dir / "result.json"

    try:
        with open(result_json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(
            "result.json not found.\n"
            "  Expected path: %s\n"
            "  Please ensure the task directory contains result.json.",
            result_json_path
        )
        return None
    except json.JSONDecodeError as e:
        logger.error(
            "Failed to parse result.json.\n"
            "  Path: %s\n"
            "  Error: %s",
            result_json_path, e
        )
        return None


def main(argv: list[str]) -> int:
    """CLI entry point. Returns exit code."""
    print("\n=== BigQuery Upload ===\n")

    if len(argv) < 2:
        logger.error(
            "Missing required argument: task_dir\n"
            "  Usage: bigquery_upload.py <task_dir> [dataset_id] [table_id]\n"
            "  Default dataset: %s\n"
            "  Default table: %s",
            DATASET_ID, TABLE_ID
        )
        return 1

    task_dir = Path(argv[1])
    dataset_id = argv[2] if len(argv) > 2 else DATASET_ID
    table_id = argv[3] if len(argv) > 3 else TABLE_ID

    logger.info("Task directory: %s", task_dir)
    logger.info("Target table: %s.%s", dataset_id, table_id)

    result_data = load_result_json(task_dir)
    if result_data is None:
        return 1

    success = upload_result_to_bigquery(result_data, task_dir, dataset_id, table_id)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))

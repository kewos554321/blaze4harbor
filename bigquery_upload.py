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


def upload_result_to_bigquery(
    result_data: dict,
    task_dir: Path,
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
) -> bool:
    """Upload result.json data to BigQuery. Returns True on success."""
    logger.info("Uploading result data from '%s' to BigQuery '%s.%s'", task_dir, dataset_id, table_id)

    result_json_path = task_dir / "result.json"
    if not result_json_path.exists():
        logger.warning("result.json not found at %s", result_json_path)
        return False

    # Load result data if not provided
    if result_data is None:
        try:
            with open(result_json_path, "r", encoding="utf-8") as f:
                result_data = json.load(f)
        except Exception as e:
            logger.error("Error reading result.json: %s", e)
            return False

    # Initialize BigQuery client
    client = bigquery.Client()

    # Ensure dataset exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset '%s' already exists", dataset_id)
    except NotFound:
        logger.info("Creating dataset '%s'", dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # TODO: Make configurable
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info("Created dataset '%s'", dataset_id)

    # Prepare data for BigQuery (flatten nested structures)
    row = flatten_result_data(result_data, task_dir)

    # Ensure table exists and get schema
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        logger.info("Table '%s' already exists", table_id)
    except NotFound:
        logger.info("Creating table '%s'", table_id)
        schema = get_bigquery_schema()
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info("Created table '%s'", table_id)

    # Insert row
    try:
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error("Error inserting row to BigQuery: %s", errors)
            return False
        logger.info("Successfully uploaded result data to %s.%s", dataset_id, table_id)
        return True
    except Exception as e:
        logger.error("Error uploading to BigQuery: %s", e)
        return False


def flatten_result_data(result_data: dict, task_dir: Path) -> dict:
    """Flatten nested JSON structure for BigQuery insertion."""
    return {
        "id": result_data.get("id"),
        "started_at": result_data.get("started_at"),
        "finished_at": result_data.get("finished_at"),
        "n_total_trials": result_data.get("n_total_trials"),
        "stats": json.dumps(result_data.get("stats", {})) if result_data.get("stats") else None,
        "task_dir_name": task_dir.name,
    }


def get_bigquery_schema() -> list[bigquery.SchemaField]:
    """Define BigQuery table schema for job results."""
    return [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("n_total_trials", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("stats", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("task_dir_name", "STRING", mode="NULLABLE"),
    ]


def main(argv: list[str]) -> int:
    """CLI entry point for uploading result.json to BigQuery. Returns exit code."""
    if len(argv) < 2:
        logger.error("Usage: bigquery_upload.py <task_dir> [dataset_id] [table_id]")
        logger.info("  Default dataset: %s", DATASET_ID)
        logger.info("  Default table: %s", TABLE_ID)
        return 1

    task_dir = Path(argv[1])
    dataset_id = argv[2] if len(argv) > 2 else DATASET_ID
    table_id = argv[3] if len(argv) > 3 else TABLE_ID

    # Load result.json
    result_json_path = task_dir / "result.json"
    if not result_json_path.exists():
        logger.error("result.json not found at %s", result_json_path)
        return 1

    with open(result_json_path, "r", encoding="utf-8") as f:
        result_data = json.load(f)

    success = upload_result_to_bigquery(result_data, task_dir, dataset_id, table_id)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))

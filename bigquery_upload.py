import sys
import json
from pathlib import Path
from typing import Optional

from absl import app
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# TODO: Later, read this from .env / environment variables, e.g. os.environ["BIGQUERY_DATASET"]
DATASET_ID = "tb_results"
TABLE_ID = "tb_results_table"


def upload_result_to_bigquery(result_data: dict, task_dir: Path, 
                              dataset_id: str = DATASET_ID, 
                              table_id: str = TABLE_ID) -> None:
    """
    Upload result.json data to BigQuery.
    """
    print(f"\nUploading result data from '{task_dir}' to BigQuery dataset '{dataset_id}.{table_id}' ...")

    result_json_path = task_dir / "result.json"
    if not result_json_path.exists():
        print(f"Warning: result.json not found at {result_json_path}", file=sys.stderr)
        return

    # Load result data if not provided
    if result_data is None:
        try:
            with open(result_json_path, "r", encoding="utf-8") as f:
                result_data = json.load(f)
        except Exception as e:
            print(f"Error reading result.json: {e}", file=sys.stderr)
            return

    # Initialize BigQuery client
    # Try to get project from dataset_id or use default
    # Client will use credentials from gcloud auth application-default login
    client = bigquery.Client()

    # Ensure dataset exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists")
    except NotFound:
        print(f"Creating dataset '{dataset_id}' ...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # TODO: Make configurable
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset '{dataset_id}'")

    # Prepare data for BigQuery (flatten nested structures)
    row = flatten_result_data(result_data, task_dir)

    # Ensure table exists and get schema
    table_ref = dataset_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        print(f"Table '{table_id}' already exists")
    except NotFound:
        print(f"Creating table '{table_id}' ...")
        schema = get_bigquery_schema()
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table '{table_id}'")

    # Insert row
    try:
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            print(f"Error inserting row to BigQuery: {errors}", file=sys.stderr)
        else:
            print(f"Successfully uploaded result data to {dataset_id}.{table_id}")
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}", file=sys.stderr)


def flatten_result_data(result_data: dict, task_dir: Path) -> dict:
    """
    Flatten nested JSON structure for BigQuery insertion.
    Converts nested objects to JSON strings for storage.
    """
    row = {
        "id": result_data.get("id"),
        "started_at": result_data.get("started_at"),
        "finished_at": result_data.get("finished_at"),
        "n_total_trials": result_data.get("n_total_trials"),
        
        # Store nested stats object as JSON string
        "stats": json.dumps(result_data.get("stats", {})) if result_data.get("stats") else None,
        
        # Additional metadata
        "task_dir_name": task_dir.name,
    }
    
    return row


def get_bigquery_schema() -> list[bigquery.SchemaField]:
    """Define BigQuery table schema for job results."""
    return [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("n_total_trials", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("stats", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("task_dir_name", "STRING", mode="NULLABLE"),
    ]


def main(argv: list[str]) -> None:
    """CLI entry point for uploading result.json to BigQuery."""
    if len(argv) < 2:
        print("Usage: bigquery_upload.py <task_dir> [dataset_id] [table_id]", file=sys.stderr)
        print(f"  Default dataset: {DATASET_ID}")
        print(f"  Default table: {TABLE_ID}")
        sys.exit(1)

    task_dir = Path(argv[1])
    dataset_id = argv[2] if len(argv) > 2 else DATASET_ID
    table_id = argv[3] if len(argv) > 3 else TABLE_ID

    # Load result.json
    result_json_path = task_dir / "result.json"
    result_data = None
    if result_json_path.exists():
        with open(result_json_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)
    else:
        print(f"Warning: result.json not found at {result_json_path}", file=sys.stderr)
        sys.exit(1)

    upload_result_to_bigquery(result_data, task_dir, dataset_id, table_id)


if __name__ == "__main__":
    main(sys.argv)


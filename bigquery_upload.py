import sys
import json
from pathlib import Path
from typing import Optional

from absl import app
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# TODO: Later, read this from .env / environment variables, e.g. os.environ["BIGQUERY_DATASET"]
DATASET_ID = "harbor_results"
TABLE_ID = "trial_results"


def upload_result_to_bigquery(result_data: dict, task_dir: Path, 
                              dataset_id: str = DATASET_ID, 
                              table_id: str = TABLE_ID) -> None:
    """
    Upload result.json data to BigQuery.

    Details:
    - Dataset and table names are taken from constants.
    - Creates dataset and table if they don't exist.
    - Flattens nested JSON structure for BigQuery compatibility.
    - Object name format: "<task_dir.name>/<relative_file_path>", e.g.
      task_dir = ".../2025-12-03__13-19-28/hello-world__NNnT6rY"
      result.json -> "hello-world__NNnT6rY/result.json"
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
        "task_name": result_data.get("task_name"),
        "trial_name": result_data.get("trial_name"),
        "trial_uri": result_data.get("trial_uri"),
        "source": result_data.get("source"),
        "task_checksum": result_data.get("task_checksum"),
        
        # Store nested objects as JSON strings
        "task_id": json.dumps(result_data.get("task_id", {})) if result_data.get("task_id") else None,
        "config": json.dumps(result_data.get("config", {})) if result_data.get("config") else None,
        "agent_info": json.dumps(result_data.get("agent_info", {})) if result_data.get("agent_info") else None,
        "agent_result": json.dumps(result_data.get("agent_result", {})) if result_data.get("agent_result") else None,
        "verifier_result": json.dumps(result_data.get("verifier_result", {})) if result_data.get("verifier_result") else None,
        "exception_info": json.dumps(result_data.get("exception_info")) if result_data.get("exception_info") else None,
        "environment_setup": json.dumps(result_data.get("environment_setup", {})) if result_data.get("environment_setup") else None,
        "agent_setup": json.dumps(result_data.get("agent_setup", {})) if result_data.get("agent_setup") else None,
        "agent_execution": json.dumps(result_data.get("agent_execution", {})) if result_data.get("agent_execution") else None,
        "verifier": json.dumps(result_data.get("verifier", {})) if result_data.get("verifier") else None,
        
        # Timestamps
        "started_at": result_data.get("started_at"),
        "finished_at": result_data.get("finished_at"),
        
        # Additional metadata
        "task_dir_name": task_dir.name,
    }
    
    return row


def get_bigquery_schema() -> list[bigquery.SchemaField]:
    """Define BigQuery table schema for trial results."""
    return [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("task_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("trial_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("trial_uri", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("task_checksum", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("task_id", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("config", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("agent_info", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("agent_result", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("verifier_result", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("exception_info", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("environment_setup", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("agent_setup", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("agent_execution", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("verifier", "STRING", mode="NULLABLE"),  # JSON string
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("task_dir_name", "STRING", mode="NULLABLE"),
    ]


def main(argv):
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
    app.run(main)


"""
BigQuery DataFrame Upload Demo

This is a simple demo script showing how to upload a pandas DataFrame to BigQuery.
Make sure you have authenticated with: gcloud auth application-default login
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# Arguments for BigQuery
PROJECT_ID = "ai-incubation-team-el-431120"
DATASET_ID = "tb_results"
TABLE_ID = "tb_results_table"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def upload_dataframe_to_bigquery(dataframe, table_ref):
    """
    Upload a dataframe to BigQuery table
    
    Args:
        dataframe: pandas DataFrame to upload
        table_ref: BigQuery table reference in format "project.dataset.table"
    """
    try:
        # Initialize BigQuery Client
        # Client would automatically use the certification provided by gcloud auth application-default login
        client = bigquery.Client()
        print("BigQuery Client initialized.")

        df = dataframe
        print(f"Uploading {len(df)} rows of data to BigQuery table: {table_ref}")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )

        # Upload the DataFrame to BigQuery
        job = client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        job.result()
        print(f"Uploading succeeded.")
        print(f"Table {table_ref} now has {client.get_table(table_ref).num_rows} data rows.")

    except Exception as e:
        print(f"Uploading failed: {e}")


def create_sample_dataframe():
    """
    Create a sample DataFrame for demonstration purposes.
    """
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'timestamp': [datetime.now()] * 5
    }
    df = pd.DataFrame(data)
    return df


def main():
    """
    Main function to demonstrate BigQuery upload.
    """
    print("=" * 60)
    print("BigQuery DataFrame Upload Demo")
    print("=" * 60)
    
    # Step 1: Create sample DataFrame
    print("\n[Step 1] Creating sample DataFrame...")
    df = create_sample_dataframe()
    print(f"Created DataFrame with {len(df)} rows:")
    print(df)
    print()
    
    # Step 2: Upload to BigQuery
    print(f"[Step 2] Uploading to BigQuery table: {TABLE_REF}")
    print()
    
    # Upload the DataFrame to BigQuery
    upload_dataframe_to_bigquery(df, TABLE_REF)


if __name__ == "__main__":
    main()


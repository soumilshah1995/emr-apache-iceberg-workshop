import os
import boto3
import duckdb

print("All Modules are loaded")


class DuckDBConnector:
    def __init__(self):
        self.conn = None

    def connect_and_setup(self) -> duckdb.DuckDBPyConnection:
        self.conn = duckdb.connect()
        extensions = ['iceberg', 'aws', 'httpfs']
        for ext in extensions:
            self.conn.execute(f"INSTALL {ext};")
            self.conn.execute(f"LOAD {ext};")
        self.conn.execute("CALL load_aws_credentials();")
        return self.conn


def get_latest_metadata_file(bucket_name: str, iceberg_path: str) -> str:
    s3 = boto3.client('s3')

    path_without_bucket = iceberg_path.replace(f's3://{bucket_name}/', '')
    metadata_prefix = f"{path_without_bucket.strip('/')}/metadata/"

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        metadata_files = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].endswith('.metadata.json')
        ]

        if metadata_files:
            latest_metadata = sorted(metadata_files)[-1]
            return f"s3://{bucket_name}/{latest_metadata}"
        else:
            print("No metadata files found")
            return None

    except Exception as e:
        print(f"Error listing objects: {str(e)}")
        return None


def main(iceberg_path: str, BUCKET: str):
    bucket_name = BUCKET
    latest_metadata_path = get_latest_metadata_file(bucket_name, iceberg_path)

    if not latest_metadata_path:
        print("No metadata files found.")
        return

    print("Latest Metadata File:", latest_metadata_path)

    connector = DuckDBConnector()
    conn = connector.connect_and_setup()

    try:
        query = f"SELECT * FROM iceberg_scan('{latest_metadata_path}')"
        result = conn.execute(query).fetchall()

        print("\nQuery Results:")
        for row in result:
            print(row)
    except duckdb.Error as e:
        print(f"Error executing DuckDB query: {e}")


if __name__ == "__main__":
    BUCKET = "XXXX"
    parquet_path = "s3://XXX/warehouse/icebergdb.db/silver_orders/"
    main(parquet_path, BUCKET)

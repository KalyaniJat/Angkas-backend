import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
import json

class ParseJSONToDict(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except Exception:
            return  # Skip invalid JSON lines

def read_gcs_file(file_path):
    """Reads a single GCS file and yields lines"""
    with FileSystems.open(file_path) as f:
        for line in f:
            yield line.decode("utf-8")

def list_gcs_files(bucket_path):
    """List all JSON files in the specified GCS bucket directory."""
    try:
        match_results = FileSystems.match([bucket_path])
        if not match_results:
            print("No files matched the pattern.")
            return []
        return [metadata.path for metadata in match_results[0].metadata_list]
    except Exception as e:
        print("Error listing files:", e)
        return []

def run():
    # Input: Cleaned JSON files from Silver layer in GCS
    BUCKET_PATH = "gs://angkas-silver-central1-bucket/processed-data/valid/*.json"
    input_files = list_gcs_files(BUCKET_PATH)

    if not input_files:
        print("No files to process. Exiting pipeline.")
        return

    # Output: BigQuery target location
    PROJECT_ID = "practicebigdataanalytics"
    DATASET = "sensor_dataset"
    TABLE = "aggregated_sensor_data"

    # Dataflow pipeline options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region="us-central1",
        job_name="silver-to-gold",
        temp_location="gs://angkas-gold-central1-bucket/temp/",
        staging_location="gs://angkas-gold-central1-bucket/staging/",
        save_main_session=True
    )

    # Target BigQuery table schema
    table_schema = {
        "fields": [
            {"name": "sensor_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "temperature", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "humidity", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "is_valid", "type": "BOOLEAN", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Create list of files" >> beam.Create(input_files)
            | "Read file content from GCS" >> beam.FlatMap(read_gcs_file)
            | "Parse JSON to Dict" >> beam.ParDo(ParseJSONToDict())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location="gs://angkas-gold-central1-bucket/temp/",
                method="STREAMING_INSERTS"
            )
        )

if __name__ == "__main__":
    run()

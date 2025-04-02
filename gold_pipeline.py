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
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at_sgt", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "attributes_user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mobile_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "trip_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "trip_status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "attributes_trip_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "service_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "attributes_iteration_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "final_fare", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "price_discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "promo_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "total_fare", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "attributes_app_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ui_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "incentive_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "demand_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "os_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "device_platform", "type": "STRING", "mode": "NULLABLE"},
            {"name": "device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "device_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "address_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "pickup_long", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "pickup_poi", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_angkas_place_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "list_count", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dropoff_lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dropoff_long", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dropoff_poi", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dropoff_angkas_place_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "heading_degree", "type": "STRING", "mode": "NULLABLE"},
            {"name": "percentage", "type": "STRING", "mode": "NULLABLE"},
            {"name": "speed", "type": "STRING", "mode": "NULLABLE"},
            {"name": "speed_measurement", "type": "STRING", "mode": "NULLABLE"},
            {"name": "is_enabled", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "is_plugged", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "is_power_safe_mode", "type": "INTEGER", "mode": "NULLABLE"}
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

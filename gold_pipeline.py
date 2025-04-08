import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
from datetime import datetime
import json

class ParseJSONToDict(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except Exception:
            return  # Skip invalid JSON lines

class AddCurrentTimestamp(beam.DoFn):
    def process(self, element):
        element["DayTime"] = datetime.utcnow().isoformat()  # Adds current UTC timestamp
        yield element

class CleanNullValues(beam.DoFn):
    def __init__(self, string_fields, numeric_fields):
        self.string_fields = string_fields
        self.numeric_fields = numeric_fields

    def process(self, element):
        for field in self.string_fields:
            if field not in element or element[field] is None or str(element[field]).strip() == "":
                element[field] = "invalid"
        for field in self.numeric_fields:
            if field not in element or element[field] is None or str(element[field]).strip() == "":
                element[field] = 0
        yield element

def read_gcs_file(file_path):
    with FileSystems.open(file_path) as f:
        for line in f:
            yield line.decode("utf-8")

def list_gcs_files(bucket_path):
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
    BUCKET_PATH = "gs://angkas-silver-central1-bucket/processed-data/valid/*.json"
    input_files = list_gcs_files(BUCKET_PATH)

    if not input_files:
        print("No files to process. Exiting pipeline.")
        return

    PROJECT_ID = "practicebigdataanalytics"
    DATASET = "sensor_dataset"
    TABLE = "aggregated_sensor_data"

    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region="us-central1",
        job_name="silver-to-gold1",
        temp_location="gs://angkas-gold-central1-bucket/temp/",
        staging_location="gs://angkas-gold-central1-bucket/staging/",
        save_main_session=True
    )

    string_fields = [
        "event_id", "user_id", "attributes_user_id", "mobile_number", "event_name", "trip_id", 
        "trip_status", "attributes_trip_id", "service_type", "attributes_iteration_id", "promo_code",
        "attributes_app_version", "ui_version", "title", "incentive_id", "demand_type", "os_version",
        "device_platform", "device_model", "device_name", "address_name", "pickup_poi", 
        "pickup_angkas_place_id", "location_id", "dropoff_poi", "dropoff_angkas_place_id", 
        "speed_measurement"
    ]

    numeric_fields = [
        "final_fare", "price_discount", "total_fare", "trip_distance", "latitude", "longitude", 
        "pickup_lat", "pickup_long", "list_count", "passenger_count", "dropoff_lat", 
        "dropoff_long", "heading_degree", "percentage", "speed"
    ]

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
            {"name": "list_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "location_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dropoff_lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dropoff_long", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dropoff_poi", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dropoff_angkas_place_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "heading_degree", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "percentage", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "speed", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "speed_measurement", "type": "STRING", "mode": "NULLABLE"},
            {"name": "is_enabled", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "is_plugged", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "is_power_safe_mode", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "is_valid", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "DayTime", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Create list of files" >> beam.Create(input_files)
            | "Read file content from GCS" >> beam.FlatMap(read_gcs_file)
            | "Parse JSON to Dict" >> beam.ParDo(ParseJSONToDict())
            | "Clean Nulls" >> beam.ParDo(CleanNullValues(string_fields, numeric_fields))
            | "Add current DayTime" >> beam.ParDo(AddCurrentTimestamp())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location="gs://angkas-gold-central1-bucket/temp/",
                method="FILE_LOADS"
            )
        )

if __name__ == "__main__":
    run()

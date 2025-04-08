import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
from google.cloud import bigquery
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
        element["DayTime"] = datetime.utcnow().isoformat()
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

    # Run pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Create list of files" >> beam.Create(input_files)
            | "Read file content from GCS" >> beam.FlatMap(read_gcs_file)
            | "Parse JSON to Dict" >> beam.ParDo(ParseJSONToDict())
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

    # Run BigQuery cleanup query directly
    print("Running BigQuery null cleanup...")
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        UPDATE `practicebigdataanalytics.sensor_dataset.aggregated_sensor_data`
        SET 
          promo_code = IF(promo_code IS NULL OR TRIM(promo_code) = '', 'invalid', promo_code),
          event_id = IF(event_id IS NULL OR TRIM(event_id) = '', 'invalid', event_id),
          user_id = IF(user_id IS NULL OR TRIM(user_id) = '', 'invalid', user_id),
          attributes_user_id = IF(attributes_user_id IS NULL OR TRIM(attributes_user_id) = '', 'invalid', attributes_user_id),
          mobile_number = IF(mobile_number IS NULL OR TRIM(mobile_number) = '', 'invalid', mobile_number),
          final_fare = IFNULL(final_fare, 0),
          price_discount = IFNULL(price_discount, 0)
        WHERE TRUE
    """
    query_job = client.query(query)
    query_job.result()
    print("BigQuery null cleanup completed.")

if __name__ == "__main__":
    run()

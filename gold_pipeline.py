import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json

class ParseJSONToDict(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except Exception:
            return  # Skip invalid JSON lines

def run():
    # Input: Cleaned JSON files from Silver layer in GCS
    INPUT_PATH = "gs://angkas-silver-bucket/processed-data/valid/silver.json"


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
        temp_location="gs://angkas-gold-bucket/temp/",
        staging_location="gs://angkas-gold-bucket/staging/",
        save_main_session=True
    )

    # Target BigQuery table schema
    table_schema = {
        "fields": [
            {"name": "sensor_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "temperature", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "humidity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read Cleaned JSON from GCS" >> beam.io.ReadFromText(INPUT_PATH)
            | "Parse JSON to Dict" >> beam.ParDo(ParseJSONToDict())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
                schema=table_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location="gs://angkas-gold-bucket/temp/",
                method="STREAMING_INSERTS"
            )
        )

if __name__ == "__main__":
    run()
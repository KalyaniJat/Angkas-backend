import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems

class ParseAndCleanseJSON(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)

            if not all(k in record for k in ("sensor_id", "temperature", "humidity", "timestamp")):
                return  # Skip records with missing fields

            record["temperature"] = int(record["temperature"])
            record["humidity"] = int(record["humidity"])

            if "T" not in record["timestamp"] or "Z" not in record["timestamp"]:
                return  # Skip invalid timestamps

            yield json.dumps(record)
        except Exception:
            return  # Skip corrupt records

def list_gcs_files(bucket_path):
    """List all JSON files in the specified GCS bucket directory."""
    try:
        filesystem = FileSystems.get_filesystem('gs')
        match_results = filesystem.match([bucket_path])
        if not match_results:
            print("No files matched the pattern.")
            return []
        
        return [metadata.path for metadata in match_results[0].metadata_list]
    except Exception as e:
        print("Error listing files:", e)
        return []

def run():
    BUCKET_PATH = "gs://angkas-bronze-bucket/raw-data/*.json"  # Folder containing JSON files
    OUTPUT_PATH = "gs://angkas-silver-bucket/cleaned-data/output"

    # List all matching files
    input_files = list_gcs_files(BUCKET_PATH)
    
    if not input_files:
        print("No files found. Exiting pipeline.")
        return
    
    # Beam pipeline options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project="practicebigdataanalytics",
        region="us-central1",
        job_name="angkas-bronze-silver", 
        temp_location="gs://angkas-silver-bucket/temp/",
        staging_location="gs://angkas-silver-bucket/staging/",
        save_main_session=True
    )

    # Define Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read JSON Files from GCS" >> beam.Create(input_files)  # Create PCollection of filenames
            | "Read File Contents" >> beam.FlatMap(lambda file: beam.io.ReadFromText(file))  # Read each file
            | "Parse and Cleanse JSON Data" >> beam.ParDo(ParseAndCleanseJSON())
            | "Write Cleaned JSON to Silver Bucket" >> WriteToText(OUTPUT_PATH, file_name_suffix=".json")
        )

if __name__ == "__main__":
    run()

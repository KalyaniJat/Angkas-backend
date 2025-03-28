import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio

# Configuration with your specific bucket names
CONFIG = {
    'project': "practicebigdataanalytics",
    'region': 'us-central1',
    'input_path': 'gs://angkas-bronze-central1-bucket/raw-data/*.json',
    'output_path': 'gs://angkas-silver-central1-bucket/processed-data/',
    'temp_location': 'gs://angkas-silver-central1-bucket/temp/',
    'staging_location': 'gs://angkas-silver-central1-bucket/staging/',
    'runner': 'DataflowRunner',
    'streaming': False,
}

class ProcessSensorData(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element)
            processed_data = {
                'sensor_id': str(data['sensor_id']),
                'temperature': float(data['temperature']),
                'humidity': float(data['humidity']),
                'timestamp': data['timestamp'],
                'processing_time': datetime.utcnow().isoformat() + 'Z',
                'is_valid': True
            }
            
            # Data quality checks
            if not (-50 <= processed_data['temperature'] <= 150):
                processed_data['is_valid'] = False
                processed_data['validation_error'] = 'Temperature out of range'
                
            if not (0 <= processed_data['humidity'] <= 100):
                processed_data['is_valid'] = False
                processed_data['validation_error'] = 'Humidity out of range'
                
            yield processed_data
            
        except Exception as e:
            logging.error(f"Error processing record: {element}, Error: {str(e)}")
            yield {
                'raw_data': element,
                'error': str(e),
                'processing_time': datetime.utcnow().isoformat() + 'Z',
                'is_valid': False
            }

def get_pipeline_options():
    return PipelineOptions(
        project=CONFIG['project'],
        runner=CONFIG['runner'],
        region=CONFIG['region'],
        temp_location=CONFIG['temp_location'],
        staging_location=CONFIG['staging_location'],
        save_main_session=True
    )

def run():
    # Verify GCP authentication in Cloud Shell
    logging.info(f"Starting pipeline with config: {CONFIG}")
    
    pipeline_options = get_pipeline_options()
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from bronze bucket
        raw_data = (
            pipeline
            | 'ReadFromBronze' >> fileio.MatchFiles(CONFIG['input_path'])
            | 'ReadMatches' >> fileio.ReadMatches()
            | 'ReadFileContents' >> beam.Map(lambda x: x.read_utf8().strip())
            | 'FilterEmptyLines' >> beam.Filter(lambda x: x != '')
        )
        
        # Process and validate data
        processed_data = (
            raw_data
            | 'ParseJSON' >> beam.ParDo(ProcessSensorData())
        )
        
        # Split into valid and invalid records
        valid_records = processed_data | 'FilterValid' >> beam.Filter(lambda x: x['is_valid'])
        invalid_records = processed_data | 'FilterInvalid' >> beam.Filter(lambda x: not x['is_valid'])
        
        # Write valid records with correct JSON extension
        _ = (
            valid_records
            | 'ConvertValidToJSON' >> beam.Map(json.dumps)
            | 'WriteValidToSilver' >> beam.io.WriteToText(
                CONFIG['output_path'] + 'valid/output',
                file_name_suffix='.json',  # Ensures .json at the end
                shard_name_template=''  # Ensures single file instead of multiple parts
            )
        )
        
        # Write invalid records with correct JSON extension
        _ = (
            invalid_records
            | 'ConvertInvalidToJSON' >> beam.Map(json.dumps)
            | 'WriteInvalidToErrors' >> beam.io.WriteToText(
                CONFIG['output_path'] + 'errors/error',
                file_name_suffix='.json',  # Ensures .json at the end
                shard_name_template=''  # Ensures single file instead of multiple parts
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

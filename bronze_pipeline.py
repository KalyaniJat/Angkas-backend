import apache_beam as beam
import json
import uuid
from datetime import datetime
from apache_beam import DoFn, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows


# Define pipeline options
pipeline_options = PipelineOptions(
    runner="DataflowRunner",
    project="practicebigdataanalytics",
    temp_location="gs://angkas-bronze-bucket/temp",
    region="us-central1",
    streaming=True,
    save_main_session=True
)

standard_options = pipeline_options.view_as(StandardOptions)
standard_options.streaming = True  # Enable streaming mode


class AddTimestamp(DoFn):
    """Extracts publish timestamp from Pub/Sub message."""

    def process(self, element, publish_time=DoFn.TimestampParam):
        try:
            record = json.loads(element.decode("utf-8"))  # Decode JSON
            timestamp = datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f")
            record["publish_time"] = timestamp  # Add timestamp to record
            yield record
        except Exception as e:
            print(f"Error processing message: {e}")


class WriteToGCS(DoFn):
    """Writes each message to a separate GCS file."""

    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        _, batch = key_value  # Extract message (batch will have only one element)
        
        for record in batch:
            window_start = window.start.to_utc_datetime().strftime("%Y-%m-%d_%H-%M-%S")
            unique_id = uuid.uuid4().hex  # Generate a unique file name
            filename = f"{self.output_path}message-{window_start}-{unique_id}.json"

            with io.gcsio.GcsIO().open(filename, mode="w") as f:
                f.write(json.dumps(record).encode())  # Write message as JSON


with Pipeline(options=pipeline_options) as p:
    (p
     | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription="projects/practicebigdataanalytics/subscriptions/raw-data-sub")
     | "Extract Timestamp" >> ParDo(AddTimestamp())
     | "Windowing" >> WindowInto(FixedWindows(60))  # ✅ Fix: Apply windowing before grouping
     | "Assign Unique Key" >> WithKeys(lambda _: str(uuid.uuid4()))  # Unique key per message
     | "Group by Key" >> beam.GroupByKey()  # ✅ Now allowed because we applied windowing
     | "Write to GCS" >> ParDo(WriteToGCS("gs://angkas-bronze-bucket/raw-data/"))
    )

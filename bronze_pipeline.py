import apache_beam as beam
import json
import random
from datetime import datetime
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
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

class GroupMessagesByFixedWindows(PTransform):
    """Group messages into fixed time windows and assign random shard keys."""

    def __init__(self, window_size, num_shards=5):
        self.window_size = int(window_size)  # Convert minutes to seconds
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            | "Window into Fixed Intervals" >> WindowInto(FixedWindows(self.window_size))
            | "Extract Timestamp" >> ParDo(AddTimestamp())
            | "Add Key for Sharding" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by Key" >> GroupByKey()
        )

class AddTimestamp(DoFn):
    """Extracts publish timestamp from Pub/Sub message."""

    def process(self, element, publish_time=DoFn.TimestampParam):
        try:
            record = json.loads(element.decode("utf-8"))  # Decode JSON
            timestamp = datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f")
            yield (json.dumps(record), timestamp)  # Return JSON with timestamp
        except Exception as e:
            print(f"Error processing message: {e}")

class WriteToGCS(DoFn):
    """Writes messages in a batch to GCS."""

    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        shard_id, batch = key_value
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%d_%H:%M:%S")
        window_end = window.end.to_utc_datetime().strftime("%Y-%m-%d_%H:%M:%S")

        filename = f"{self.output_path}window-{window_start}-{window_end}-shard-{shard_id}.json"

        with io.gcsio.GcsIO().open(filename, mode="w") as f:
            for message_body, publish_time in batch:
                f.write(f"{message_body}\n".encode())

with Pipeline(options=pipeline_options) as p:
    (p
     | "Read from Pub/Sub" >> io.ReadFromPubSub(topic="projects/practicebigdataanalytics/topics/raw-data-topic")
     | "Windowing and Grouping" >> GroupMessagesByFixedWindows(window_size=60, num_shards=5)
     | "Write to GCS" >> ParDo(WriteToGCS("gs://angkas-bronze-bucket/raw-data/"))
    )

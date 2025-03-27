import json
import os
from google.cloud import pubsub_v1


# Pub/Sub settings
PROJECT_ID = "practicebigdataanalytics"
TOPIC_ID = "raw-data-topic"

# Initialize Publisher Client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Load JSON data from file
JSON_FILE_PATH = "data.json"

try:
    with open(JSON_FILE_PATH, "r", encoding="utf-8") as file:
        sensor_data = json.load(file)  # Load JSON array

    for record in sensor_data:
        message_data = json.dumps(record).encode("utf-8")  # Convert to JSON and encode
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        print(f"✅ Published message ID: {message_id}")

    print("✅ All messages published successfully.")

except Exception as e:
    print(f"❌ Error: {e}")

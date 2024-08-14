# publisher.py
import os
from google.cloud import pubsub_v1

# Set up the environment variable to connect to the emulator
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

# Initialize the Pub/Sub client
publisher = pubsub_v1.PublisherClient()
project_id = "some-project-id"
topic_id = "csv-topic"
topic_path = publisher.topic_path(project_id, topic_id)

# Create the topic
publisher.create_topic(request={"name": topic_path})
print(f"Created topic {topic_id}")

# Publish the CSV files
with open('ratings.csv', 'rb') as f:
    publisher.publish(topic_path, f.read(), filename='ratings.csv')

with open('movies.csv', 'rb') as f:
    publisher.publish(topic_path, f.read(), filename='movies.csv')

print("CSV files published.")

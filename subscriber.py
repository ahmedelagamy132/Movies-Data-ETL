# subscriber.py
import os
from google.cloud import pubsub_v1

# Set up the environment variable to connect to the emulator
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

# Initialize the Pub/Sub client
subscriber = pubsub_v1.SubscriberClient()
project_id = "some-project-id"
subscription_id = "csv-subscription"
topic_id = "csv-topic"
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = subscriber.topic_path(project_id, topic_id)

# Create the subscription
subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
print(f"Created subscription {subscription_id}")

# Define the callback to process messages
def callback(message):
    filename = message.attributes['filename']
    with open(filename, 'wb') as f:
        f.write(message.data)
    print(f"Saved {filename}")
    message.ack()

# Start listening for messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped listening for messages.")

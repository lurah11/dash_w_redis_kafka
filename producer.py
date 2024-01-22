import argparse
from confluent_kafka import Producer, KafkaException
import socket 
import pandas as pd 
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main(min_index, max_index):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'acks': 'all',  # Wait for all replicas to acknowledge
        'delivery.report.only.error': False  # Report successful deliveries as well
    }

    # Initialize the producer
    producer = Producer(conf)

    topic = 'dataviz'

    df = pd.read_csv('earthquake_data.csv', parse_dates=True)

    # Filter DataFrame based on the provided min and max indices
    test_data = df.loc[min_index:max_index, ['Date', 'Longitude', 'Latitude', 'Magnitude']]

    for data in test_data.itertuples():
        data_obj = {
            'index': data.Index,
            'date': str(data.Date),
            'longitude': data.Longitude,
            'latitude': data.Latitude,
            'magnitude': data.Magnitude
        }

        data_obj_json = json.dumps(data_obj)

        # Produce the message and set the on_delivery callback
        producer.produce(topic, value=data_obj_json, callback=delivery_report)
        producer.poll(0)  # Trigger delivery reports
        time.sleep(0.5)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produce messages to Kafka with specified index range.')
    parser.add_argument('--min_index', type=int, default=0, help='Minimum index for DataFrame.')
    parser.add_argument('--max_index', type=int, default=100, help='Maximum index for DataFrame.')

    args = parser.parse_args()
    
    main(args.min_index, args.max_index)

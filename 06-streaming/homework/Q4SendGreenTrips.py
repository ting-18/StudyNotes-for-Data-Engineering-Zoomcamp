import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = './src/data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        t0 = time()
        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-trips"
            producer.send('green-trips', value=row)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()
    t1 = time()
    took = t1 - t0
    print(took)

if __name__ == "__main__":
    main()
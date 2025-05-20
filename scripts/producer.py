import pandas as pd
import json
import numpy as np
from kafka import KafkaProducer
import time

def create_producer(bootstrap_servers=['localhost:9092']):
    """
    Create and return a Kafka producer instance.

    Parameters:
    -----------
    bootstrap_servers : list of str, optional
        List of Kafka bootstrap server addresses (default is ['localhost:9092']).

    Returns:
    --------
    KafkaProducer or None
        A configured KafkaProducer object if successful, else None.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka producer created successfully.")
        return producer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return None

def send_data(producer, topic='happiness_data', data_path='../data/processed/cleaned_data.csv'):
    """
    Send selected feature data from a CSV file to a specified Kafka topic.

    Parameters:
    -----------
    producer : KafkaProducer
        The Kafka producer instance used to send messages.
    topic : str, optional
        The Kafka topic to which messages will be sent (default is 'happiness_data').
    data_path : str, optional
        Path to the CSV file containing the data (default is '../data/processed/cleaned_data.csv').

    Behavior:
    ---------
    - Reads the CSV file into a pandas DataFrame.
    - Selects the defined features (excluding the target Score).
    - Iterates over each row and sends it as a JSON message to Kafka.
    - Converts numpy int types to Python ints to ensure JSON serialization.
    - Includes a short delay between messages to simulate streaming.
    - Flushes the producer after all messages are sent.
    """
    try:
        df = pd.read_csv(data_path)
        features = [
            'GDP per capita',
            'Healthy life expectancy',
            'Freedom to make life choices',
            'Generosity',
            'Perceptions of corruption',
            'Continent_Africa',
            'Continent_America',
            'Continent_Europe'
        ]
        df = df[features]  # Select features only (exclude Score)

        for _, row in df.iterrows():
            message = {
                k: int(v) if isinstance(v, (np.int64, np.int32)) else v
                for k, v in row.to_dict().items()
            }
            producer.send(topic, message)
            print(f"Sent message: {message}")
            time.sleep(0.1)  # Simulate streaming delay

        producer.flush()
        print("All data sent successfully.")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

if __name__ == "__main__":
    producer = create_producer()
    if producer:
        send_data(producer)

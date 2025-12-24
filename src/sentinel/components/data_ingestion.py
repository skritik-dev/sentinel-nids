import os
import time
import json
import pandas as pd
from quixstreams import Application
from sentinel.logger import get_logger

# Initialize Logger
logger = get_logger("DataIngestion")

# Standard NSL-KDD column names
COLUMNS = [ 
    "duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes",
    "land", "wrong_fragment", "urgent", "hot", "num_failed_logins",
    "logged_in", "num_compromised", "root_shell", "su_attempted", "num_root",
    "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds",
    "is_host_login", "is_guest_login", "count", "srv_count", "serror_rate",
    "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate",
    "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
    "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
    "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
    "dst_host_rerror_rate", "dst_host_srv_rerror_rate", "label", "difficulty"
]

class DataIngestion:
    def __init__(self, input_file: str, topic_name: str, broker_addr: str = None):
        self.input_file = input_file
        self.topic_name = topic_name
        self.broker_addr = broker_addr or os.getenv("REDPANDA_BROKER", "localhost:9092")
        
        # Initialize Quix Application
        self.app = Application(broker_address=self.broker_addr, consumer_group="ingestion-producer")
        self.topic = self.app.topic(name=self.topic_name, value_serializer="json")

    def start_ingestion(self, sleep_time: float = 0.5):
        """
        Reads CSV and streams data to Redpanda.
        """
        logger.info(f"Starting ingestion from {self.input_file}")
        
        try:
            # Read CSV
            df = pd.read_csv(self.input_file, names=COLUMNS)
            logger.info(f"Dataset loaded. Records: {len(df)}")
        except FileNotFoundError:
            logger.error(f"File not found: {self.input_file}")
            return

        # Create Producer
        with self.app.get_producer() as producer:
            count = 0
            while True:
            # Pandas row -> dict -> JSON
                for _, row in df.iterrows():
                    
                    # Create Message
                    message = row.to_dict()
                    message['packet_id'] = str(count)
                    message['timestamp'] = time.time()  # Add ingestion time
                    
                    # Produce to Topic
                    producer.produce(
                        topic=self.topic.name,
                        key=message['protocol_type'],
                        value=json.dumps(message)
                    )
                    
                    logger.info(f"Produced [{count}]: {message['protocol_type']} | {message['label']}")
                    
                    count += 1
                    time.sleep(sleep_time)

            logger.info("Ingestion cycle complete")

if __name__ == "__main__":
    FILE_PATH = "/app/data/kdd_train.csv"
    TOPIC_NAME = "network-traffic"
    
    ingestor = DataIngestion(input_file=FILE_PATH, topic_name=TOPIC_NAME)
    
    ingestor.start_ingestion(sleep_time=0.5)  
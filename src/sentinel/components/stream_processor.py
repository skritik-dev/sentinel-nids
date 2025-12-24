import os
import csv
import json
import requests
import pandas as pd
from quixstreams import Application
from feast import FeatureStore
from sentinel.logger import get_logger

logger = get_logger("StreamProcessor")
API_URL = os.getenv("API_URL", "http://localhost:3000/predict")

class StreamProcessor:
    def __init__(self, topic_name="network-traffic", broker_addr: str =None):
        self.broker_addr = broker_addr or os.getenv("REDPANDA_BROKER", "localhost:9092")

        self.app = Application(
            broker_address=self.broker_addr,
            consumer_group="feature-processor",
            auto_offset_reset="latest"
        )
        
        self.topic = self.app.topic(name=topic_name, value_serializer="json")
        self.fs = FeatureStore(repo_path="features/")

    def log_prediction(self, packet_id, timestamp, prediction, score):
        file_exists = os.path.isfile("/app/data/predictions.csv")
        with open("/app/data/predictions.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "packet_id", "prediction", "score"])
            writer.writerow([timestamp, packet_id, prediction, score])

    def process_message(self, message):
        try:
            # Extract Features for model input
            payload = {
                "src_bytes": float(message.get("src_bytes", 0)),
                "dst_bytes": float(message.get("dst_bytes", 0)),
                "duration": float(message.get("duration", 0)),
                "count": float(message.get("count", 0)),
                "srv_count": float(message.get("srv_count", 0))
            }

            # Push to Feast 
            #? Feast expects a list of dictionaries or a DataFrame
            feature_row = payload.copy()
            feature_row["packet_id"] = str(message.get("packet_id", "unknown"))
            feature_row["event_timestamp"] = pd.Timestamp.now()
            feature_row["protocol_type"] = str(message.get("protocol_type", "unknown"))
            feature_row["service"] = str(message.get("service", "unknown"))
            feature_row["flag"] = str(message.get("flag", "unknown"))
            
            self.fs.push("packet_push_source", pd.DataFrame([feature_row]))

            response = requests.post(API_URL, json={"req": payload})
            
            if response.status_code == 200:
                result = response.json()
                pred = result["prediction"]
                res = result["score"]

                self.log_prediction(feature_row["packet_id"], feature_row["event_timestamp"], pred, res)
                
                if pred == "Anomaly":
                    logger.error(f"ALERT! Anomaly Detected in Packet {feature_row['packet_id']}! Score: {res}")
                else:
                    logger.info(f"Packet {feature_row['packet_id']} is Normal")
            else:
                logger.error(f"API Failed: {response.text}")

        except Exception as e:
            logger.error(f"Failed to process message: {e}")


    def start(self):
        logger.info("Starting Stream Processor")
        sdf = self.app.dataframe(self.topic)
        sdf = sdf.update(self.process_message)
        self.app.run()

if __name__ == "__main__":
    #! Note: Run the data_ingestion.py script in a separate terminal
    processor = StreamProcessor()
    processor.start()
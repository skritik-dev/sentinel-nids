import os
import csv
import json
import redis
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

        try:
            self.redis_client = redis.Redis(host="redis", port=6379, db=0)
            self.redis_client.ping() # Check connection
            logger.info("Connected to Redis for State Management")
        except Exception as e:
            logger.error(f"Redis Connection Failed: {e}")
            self.redis_client = None

    def log_training_data(self, payload):
        directory = "/app/data"
        file_path = f"{directory}/live_traffic.csv"
        
        # TODO: In a real time system, we need to save all the columns
        columns = ["src_bytes", "dst_bytes", "duration", "count", "srv_count"]
        
        try:
            file_exists = os.path.isfile(file_path)
            with open(file_path, "a", newline="", buffering=1) as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                
                if not file_exists:
                    writer.writeheader()
            
                writer.writerow(payload)
    
                f.flush()
                os.fsync(f.fileno())
                
        except Exception as e:
            logger.error(f"Failed to log training data: {e}")

    def log_prediction(self, packet_id, timestamp, prediction, score):
        file_exists = os.path.isfile("/app/data/predictions.csv")
        with open("/app/data/predictions.csv", "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "packet_id", "prediction", "score"])
            writer.writerow([timestamp, packet_id, prediction, score])

    def get_real_time_count(self, ip_identifier):
        """Uses Redis to count traffic in a 2-second sliding window."""
        if not self.redis_client:
            return 0
            
        key = f"count:{ip_identifier}"
        
        # Increment counter
        current_count = self.redis_client.incr(key)
        
        # Set expiry (TTL) on first packet to create a "Sliding Window"
        if current_count == 1:
            self.redis_client.expire(key, 2)
            
        return float(current_count)
    
    def process_message(self, message):
        try:
            simulated_ip = f"{message.get('protocol_type')}_{message.get('service')}"
            real_count = self.get_real_time_count(simulated_ip)
            
            # Extract Features for model input
            payload = {
                "src_bytes": float(message.get("src_bytes", 0)),
                "dst_bytes": float(message.get("dst_bytes", 0)),
                "duration": float(message.get("duration", 0)),
                "count": real_count,
                "srv_count": float(message.get("srv_count", 0))
            }

            self.log_training_data(payload)
            
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
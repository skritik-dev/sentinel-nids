import os
import time
import json
import random
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from sentinel.logger import get_logger

logger = get_logger("LiveProducer")

# Configuration
API_URL = "https://randomuser.me/api/"
TOPIC = "network-traffic"
BROKER = os.getenv("REDPANDA_BROKER", "localhost:9092")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "5"))

# Realistic network simulation data
SERVICES = ["http", "ftp", "smtp", "ssh", "dns", "telnet", "pop3", "imap"]
FLAGS = ["SF", "S0", "REJ", "RSTO", "RSTR", "SH", "S1", "S2", "RSTOS0"]

# Simulate different network behaviors
ATTACK_PATTERNS = {
    "normal": {"weight": 0.85},
    "port_scan": {"weight": 0.05},
    "dos": {"weight": 0.03},
    "probe": {"weight": 0.04},
    "r2l": {"weight": 0.02},
    "u2r": {"weight": 0.01}
}

class NetworkPacketSimulator:
    """Simulates realistic network packet structures"""
    
    def __init__(self):
        self.packet_counter = 0
        
    def generate_ip(self, is_internal=True):
        if is_internal:
            subnet = random.choice(["192.168", "10.0", "172.16"])
            return f"{subnet}.{random.randint(0,255)}.{random.randint(1,254)}"
        else:
            return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    
    def select_behavior(self):
        rand = random.random()
        cumulative = 0
        for behavior, config in ATTACK_PATTERNS.items():
            cumulative += config["weight"]
            if rand <= cumulative:
                return behavior
        return "normal"
    
    def generate_packet(self, behavior, api_data=None):
        """Generates a packet based on behavior type"""
        self.packet_counter += 1
        
        # Defaults for 'normal' traffic
        src_ip = self.generate_ip(is_internal=True)
        dst_ip = self.generate_ip(is_internal=False)
        src_port = random.randint(1024, 65535)
        dst_port = random.choice([80, 443, 22, 21, 25, 53])
        service = "http" if dst_port in [80, 443] else random.choice(SERVICES)
        flag = "SF"
        
        # --- BEHAVIOR MODIFIERS ---
        if behavior == "normal" and api_data:
            duration = api_data["duration"]
            src_bytes = api_data["bytes_sent"]
            dst_bytes = api_data["bytes_received"]
            
        elif behavior == "port_scan":
            src_ip = self.generate_ip(is_internal=False) # Attacker is outside
            dst_ip = self.generate_ip(is_internal=True)  # Target is inside
            dst_port = random.randint(1, 1024)
            flag = "S0" # Connection attempt seen, no reply
            duration = 0.0
            src_bytes = 0
            dst_bytes = 0
            
        elif behavior == "dos":
            src_ip = self.generate_ip(is_internal=False)
            dst_ip = self.generate_ip(is_internal=True)
            dst_port = 80
            flag = "S0"
            duration = 0.001
            src_bytes = random.randint(5000, 50000) # Huge payload
            dst_bytes = 0
            
        elif behavior == "probe":
            flag = "REJ" 
            duration = random.uniform(0.001, 0.1)
            src_bytes = random.randint(40, 200)
            dst_bytes = 0
            
        else:  
            duration = 0.1
            src_bytes = random.randint(200, 500)
            dst_bytes = random.randint(200, 500)

        return {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "packet_id": self.packet_counter,
            "behavior_type": behavior,
            
            "src_bytes": src_bytes,    
            "dst_bytes": dst_bytes,    
            "duration": duration,
            "service": service,
            "flag": flag,               
            
            # Metadata for Dashboard/Logs
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": "tcp",
            
            "count": 0,
            "srv_count": 0
        }

def get_live_traffic(simulator):
    """Fetches real API data to drive the simulation clock"""
    try:
        start_time = time.time()
        response = requests.get(API_URL, timeout=API_TIMEOUT)
        latency = time.time() - start_time
        
        if response.status_code == 200:
            api_data = {
                "duration": round(latency, 4),
                "bytes_sent": len(response.content) + random.randint(100, 500),
                "bytes_received": len(response.content)
            }
            
            # Decide: Attack or Normal?
            behavior = simulator.select_behavior()
            return simulator.generate_packet(behavior, api_data)
            
        return None
    except Exception as e:
        logger.error(f"Fetch Failed: {e}")
        return None

def start_producer():
    # Retry Logic for Kafka Connection
    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info(f"Connected to Redpanda at {BROKER}")
            break
        except Exception as e:
            logger.warning(f"Waiting for Redpanda. ({i+1}/10)")
            time.sleep(5)
            
    if not producer:
        logger.error("Could not connect to Redpanda. Exiting.")
        return

    simulator = NetworkPacketSimulator()
    logger.info(f"Live Producer Started!")
    
    while True:
        packet = get_live_traffic(simulator)
        
        if packet:
            producer.send(TOPIC, packet)
            logger.info(f"{packet['behavior_type'].upper()} | {packet['src_ip']} -> {packet['dst_ip']} | Bytes: {packet['src_bytes']}")
            
        time.sleep(random.uniform(0.1, 0.8))

if __name__ == "__main__":
    start_producer()
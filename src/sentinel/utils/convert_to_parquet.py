import pandas as pd
import uuid
from datetime import datetime, timedelta
from sentinel.logger import get_logger

logger = get_logger("ConvertToParquet")

def convert_csv_to_parquet(input_path="data/kdd_train.csv", output_path="data/kdd_train.parquet"):
    columns = [
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
    
    df = pd.read_csv(input_path, names=columns)
    
    # Add FEAST requirements
    # 1. Event Timestamp: We simulate that data arrived over the last 30 days
    # (In prod, this is the actual time)
    df['event_timestamp'] = [datetime.now() - timedelta(minutes=i) for i in range(len(df))]
    
    # 2. Entity ID: Generate a unique ID for each packet
    df['packet_id'] = [str(i) for i in range(len(df))]
    
    df.to_parquet(output_path)
    logger.info(f"Converted {input_path} to {output_path} with {len(df)} rows.")

if __name__ == "__main__":
    convert_csv_to_parquet()
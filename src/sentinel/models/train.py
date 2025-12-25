import os
import sys
import bentoml
import pandas as pd
from pathlib import Path
from sklearn.ensemble import IsolationForest
from sentinel.logger import get_logger

logger = get_logger("ModelTraining")

BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data"
OLD_DATA_PATH = DATA_DIR / "kdd_train.parquet"
NEW_DATA_PATH = DATA_DIR / "live_traffic.csv"

#? I'll use numerical features for my Isolation Forest
FEATURES = [
    "src_bytes",
    "dst_bytes",
    "duration",
    "count",
    "srv_count"
]

def load_combined_data():
    combined_df = pd.DataFrame()

    # Load old data
    if os.path.exists(OLD_DATA_PATH):
        logger.info(f"Loading old data from {OLD_DATA_PATH}")
        try:
            df_history = pd.read_parquet(OLD_DATA_PATH)
            df_history = df_history[FEATURES]
            combined_df = pd.concat([combined_df, df_history], ignore_index=True)
            logger.info(f"Added {len(df_history)} old records")
        except Exception as e:
            logger.error(f"Failed to load old data: {e}")
    else:
        logger.warning(f"Old data not found at {OLD_DATA_PATH}")

    # Load new data
    if os.path.exists(NEW_DATA_PATH):
        logger.info(f"Loading Live Traffic from {NEW_DATA_PATH}")
        try:
            # The CSV has headers because we wrote them in the processor
            df_live = pd.read_csv(NEW_DATA_PATH)
            if list(df_live.columns) != FEATURES:
                df_live.columns = FEATURES
                
            df_live = df_live[FEATURES]
            combined_df = pd.concat([combined_df, df_live], ignore_index=True)
            logger.info(f"Added {len(df_live)} new live records")
        except Exception as e:
            logger.error(f"Failed to load live data: {e}")
    else:
        logger.info("No live data found!")

    return combined_df

def train_model():
    logger.info("Starting Model Training Job")
    X = load_combined_data()

    if X.empty:
        logger.error("No data found! Cannot train model. Check if 'data/kdd_train.parquet' is pushed to GitHub.")
        sys.exit(1)  
    
    # Train Isolation Forest
    #? Contamination = 0.01 means we expect ~1% of traffic to be malicious, but in real life this should be ~6-7%
    logger.info("Training Isolation Forest")
    model = IsolationForest(
        n_estimators=100,
        contamination=0.01, 
        random_state=42, 
        n_jobs=-1
    )
    model.fit(X)
    
    # Save to BentoML
    logger.info("Saving model to BentoML Model Store")
    bento_model = bentoml.sklearn.save_model(
        "sentinel_model", 
        model,
        signatures={
            "predict": {"batchable": True} # Optimizes for high-throughput
        },
        metadata={
            "metrics": "unsupervised",
            "features": FEATURES
        }
    )
    
    logger.info(f"Model saved: {bento_model.tag}")
    logger.info(f"Model path: {bento_model.path}")

if __name__ == "__main__":
    train_model()
import pandas as pd
import bentoml
from sklearn.ensemble import IsolationForest
from sentinel.logger import get_logger

logger = get_logger("ModelTraining")

#? I'll use numerical features for my Isolation Forest
FEATURES = [
    "src_bytes",
    "dst_bytes",
    "duration",
    "count",
    "srv_count"
]

def train_model():
    logger.info("Loading historical data for training")
    
    try:
        df = pd.read_parquet("data/kdd_train.parquet")
    except FileNotFoundError:
        logger.error("Data not found!")
        return

    X = df[FEATURES]
    
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

if __name__ == "__main__":
    train_model()
import bentoml
import numpy as np

@bentoml.service(name="sentinel_nids")
class SentinelService:
    def __init__(self):
        self.model = bentoml.sklearn.load_model("sentinel_model:latest")

    @bentoml.api
    def predict(self, req: dict) -> dict:
        """
        Real-time Inference Endpoint.
        """
        # Prepare Vector (Order must match with order during training)
        features = [req.src_bytes, req.dst_bytes, req.duration, req.count, req.srv_count]
        vector = np.array([features])
        
        # Prediction
        prediction = self.model.predict(vector)
        result = "Anomaly" if prediction[0] == -1 else "Normal"
        
        return {
            "prediction": result,
            "score": int(prediction[0]),
            "input_echo": {
                "src_bytes": req.src_bytes,
                "dst_bytes": req.dst_bytes,
                "duration": req.duration,
                "count": req.count,
                "srv_count": req.srv_count
            }
        }
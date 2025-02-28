import numpy as np
from sklearn.ensemble import IsolationForest
import pickle
import os
from pathlib import Path


def train_anomaly_detector(model_path="models/anomaly_detector.pkl"):
    print("Training anomaly detector")
    data = np.concatenate([
        np.random.uniform(20, 40, 1000),
        np.random.uniform(50, 60, 50)
    ]).reshape(-1, 1)
    model = IsolationForest(contamination=0.1, random_state=42, n_jobs=-1)
    model.fit(data)

    # Ensure models directory exists
    model_path = Path(__file__).resolve().parent.parent.parent / model_path
    model_dir = model_path.parent
    model_dir.mkdir(parents=True, exist_ok=True)

    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"Model saved to {model_path}")


if __name__ == "__main__":
    train_anomaly_detector()

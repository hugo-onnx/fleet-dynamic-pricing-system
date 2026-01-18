import argparse
from pathlib import Path

import pandas as pd

from features.reconstruct import reconstruct_features
from split import time_split
from train_eta import train_eta_model
from eval import evaluate
from data.schema import FEATURE_COLUMNS, LABEL_COLUMN


DATA_PATH = "data/trips_madrid.parquet"
MODEL_PATH = "../services/inference-api/models/eta_model.joblib"


def main(data_path: str = DATA_PATH, model_path: str = MODEL_PATH):
    print(f"Loading data from {data_path}...")
    trips = pd.read_parquet(data_path)
    print(f"Loaded {len(trips)} trip records")

    print("Reconstructing features...")
    dataset = reconstruct_features(trips)
    print(f"Dataset size after cleaning: {len(dataset)}")

    print("Splitting data (80/20 time-based split)...")
    train, val = time_split(dataset)
    print(f"Train: {len(train)}, Val: {len(val)}")

    print("Training XGBoost model...")
    model = train_eta_model(train, val, model_path)

    print("Evaluating model...")
    metrics = evaluate(model, val, FEATURE_COLUMNS, LABEL_COLUMN)

    print("\n" + "=" * 40)
    print("ETA MODEL METRICS")
    print("=" * 40)
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    print("=" * 40)

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train ETA prediction model")
    parser.add_argument(
        "--data", 
        default=DATA_PATH, 
        help="Path to trips parquet file"
    )
    parser.add_argument(
        "--output", 
        default=MODEL_PATH, 
        help="Path to save trained model"
    )
    args = parser.parse_args()
    
    main(data_path=args.data, model_path=args.output)
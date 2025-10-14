from __future__ import annotations
import pandas as pd
from sklearn.model_selection import train_test_split

def simple_split(df: pd.DataFrame, y_col: str, test_size: float = 0.2, seed: int = 42):
    X = df.drop(columns=[y_col])
    y = df[y_col]
    return train_test_split(X, y, test_size=test_size, random_state=seed)

#!/usr/bin/env python3
"""Load the Enron emails dataset from Kaggle into a pandas DataFrame."""

import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter

DATASET_HANDLE = "wcukierski/enron-email-dataset"
DATASET_FILE = "emails.csv"


def load_dataframe() -> pd.DataFrame:
    print(f"Loading {DATASET_FILE} from Kaggle dataset {DATASET_HANDLE}")
    return kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        DATASET_HANDLE,
        DATASET_FILE,
    )


def main() -> None:
    df = load_dataframe()
    print("First 5 records:")
    print(df.head())


if __name__ == "__main__":
    main()

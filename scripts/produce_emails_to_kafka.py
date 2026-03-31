#!/usr/bin/env python3
"""Stream normalized email records from a DataFrame into a Kafka topic."""

import argparse
import json
import time
from typing import Optional

from kafka import KafkaProducer

from email_record_utils import iter_records_from_rows
from kaggle import DATASET_FILE, DATASET_HANDLE, load_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the Enron emails DataFrame and publish normalized email records to Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default="emails_raw",
        help="Kafka topic that receives raw email records.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Pause between messages to simulate a live producer.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional cap for the number of emails to publish.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5000,
        help="Print progress every N published records.",
    )
    return parser.parse_args()


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )


def send_records(
    producer: KafkaProducer,
    rows,
    topic: str,
    max_records: Optional[int],
    sleep_seconds: float,
    progress_every: int,
) -> None:
    published = 0

    for record in iter_records_from_rows(rows):
        producer.send(topic, key=record["file_path"], value=record)
        published += 1

        if published % progress_every == 0:
            producer.flush()
            print(f"Published {published} records -> {topic}", flush=True)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        if max_records is not None and published >= max_records:
            break

    producer.flush()
    print(f"Finished publishing {published} records -> {topic}", flush=True)


def main() -> None:
    args = parse_args()
    dataframe = load_dataframe()
    producer = build_producer(args.bootstrap_servers)

    print(f"Reading source DataFrame from Kaggle dataset: {DATASET_HANDLE}")
    print(f"Dataset file: {DATASET_FILE}")
    print(f"Loaded rows: {len(dataframe)}")
    print(f"Publishing to Kafka topic: {args.topic}")
    print(f"Bootstrap servers: {args.bootstrap_servers}")

    try:
        send_records(
            producer,
            dataframe.to_dict("records"),
            args.topic,
            args.max_records,
            args.sleep_seconds,
            args.progress_every,
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()

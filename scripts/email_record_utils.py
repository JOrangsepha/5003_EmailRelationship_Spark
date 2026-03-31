#!/usr/bin/env python3
"""Shared helpers for converting raw email rows into normalized records."""

import argparse
import csv
import json
import re
import sys
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Optional, Tuple


def set_csv_field_size_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def split_headers_and_body(message: str) -> Tuple[str, str]:
    normalized = message.replace("\r\n", "\n")
    if "\n\n" not in normalized:
        return normalized, ""
    return normalized.split("\n\n", 1)


def parse_headers(header_text: str) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    current_name = None

    for raw_line in header_text.split("\n"):
        if not raw_line:
            continue
        if raw_line[0].isspace() and current_name:
            headers[current_name] = f"{headers[current_name]} {raw_line.strip()}"
            continue
        if ":" not in raw_line:
            continue
        name, value = raw_line.split(":", 1)
        current_name = name.strip()
        headers[current_name] = value.strip()

    return headers


def normalize_date(date_raw: str) -> Optional[str]:
    if not date_raw:
        return None

    cleaned = re.sub(r"\s+\([^)]+\)$", "", date_raw.strip())
    try:
        parsed = parsedate_to_datetime(cleaned)
    except (TypeError, ValueError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_cell(value: object) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    return str(value)


def sanitize_row(row: Mapping[str, object]) -> Dict[str, str]:
    return {str(key): normalize_cell(value) for key, value in row.items()}


def build_record(row: Mapping[str, object]) -> Dict[str, object]:
    normalized_row = sanitize_row(row)
    raw_message = normalized_row.get("message", "")
    header_text, body = split_headers_and_body(raw_message)
    headers = parse_headers(header_text)
    clean_body = body.strip()

    return {
        "file_path": normalized_row.get("file"),
        "message_id": headers.get("Message-ID"),
        "sent_at": normalize_date(headers.get("Date", "")),
        "sender": headers.get("From"),
        "to": headers.get("To"),
        "cc": headers.get("Cc") or headers.get("X-cc"),
        "bcc": headers.get("Bcc") or headers.get("X-bcc"),
        "subject": headers.get("Subject"),
        "x_folder": headers.get("X-Folder"),
        "x_origin": headers.get("X-Origin"),
        "x_filename": headers.get("X-FileName"),
        "body": clean_body,
        "body_length": len(clean_body),
    }


def iter_records(input_path: Path) -> Iterator[Dict[str, object]]:
    set_csv_field_size_limit()
    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield build_record(row)


def iter_records_from_rows(rows: Iterable[Mapping[str, object]]) -> Iterator[Dict[str, object]]:
    for row in rows:
        yield build_record(row)


def batch_records(
    records: Iterable[Dict[str, object]], batch_size: int, max_records: Optional[int]
) -> Iterator[list[Dict[str, object]]]:
    batch: list[Dict[str, object]] = []
    emitted = 0

    for record in records:
        batch.append(record)
        emitted += 1

        if len(batch) >= batch_size:
            yield batch
            batch = []

        if max_records is not None and emitted >= max_records:
            break

    if batch:
        yield batch


def ensure_empty_output_dir(output_dir: Path) -> None:
    existing = list(output_dir.glob("batch_*.json"))
    if existing:
        raise ValueError(
            f"Output directory already contains {len(existing)} batch files: {output_dir}. "
            "Please remove them or choose a new --output-dir before rerunning."
        )


def write_batch(output_dir: Path, batch_index: int, batch: list[Dict[str, object]]) -> Path:
    final_path = output_dir / f"batch_{batch_index:06d}.json"
    temp_path = output_dir / f".batch_{batch_index:06d}.json.tmp"

    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(batch, handle, ensure_ascii=False)

    temp_path.rename(final_path)
    return final_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the Enron Kaggle DataFrame, parse emails, and export JSON batches."
    )
    parser.add_argument(
        "--output-dir",
        default="parsed_emails",
        help="Directory where parsed JSON batch files will be written.",
    )
    parser.add_argument(
        "--records-per-file",
        type=int,
        default=5000,
        help="How many parsed emails to store in each JSON file.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional cap for the number of emails to parse.",
    )
    return parser.parse_args()


def main() -> None:
    from kaggle import load_dataframe

    args = parse_args()
    if args.records_per_file <= 0:
        raise ValueError("--records-per-file must be positive.")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ensure_empty_output_dir(output_dir)

    dataframe = load_dataframe()
    print(f"Loaded rows: {len(dataframe)}")
    print(f"Writing parsed batches to: {output_dir}")

    for batch_index, batch in enumerate(
        batch_records(
            iter_records_from_rows(dataframe.to_dict("records")),
            args.records_per_file,
            args.max_records,
        )
    ):
        batch_path = write_batch(output_dir, batch_index, batch)
        print(f"Wrote {len(batch)} parsed emails -> {batch_path.name}", flush=True)


if __name__ == "__main__":
    main()

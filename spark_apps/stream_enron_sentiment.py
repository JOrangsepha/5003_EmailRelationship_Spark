"""Read Enron emails from Kafka, enrich them with DistilBERT sentiment, and write to Elasticsearch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

import math

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame

from enron_common import (
    ES_INDEX_SENTIMENT,
    ES_INDEX_EDGES,
    KAFKA_TOPIC,
    KAFKA_EMAIL_JSON_SCHEMA,
    build_spark_session,
    es_index_exists,
    get_es_client,
)

from elasticsearch import helpers
# export ES_HOST="localhost"
# export ES_PORT="9200"
# export KAFKA_TOPIC="emails_raw"

DEFAULT_MODEL_NAME = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"

_MODEL = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Enron emails from Kafka, score sentiment with DistilBERT, and index documents in Elasticsearch."
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", KAFKA_TOPIC),
        help="须与 produce_emails_to_kafka.py 的 --topic 一致（默认 raw_emails_topic）。",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=os.getenv("CHECKPOINT_DIR", "./checkpoints/enron_kafka_es"),
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        help="Use earliest for a full replay, latest for only new messages.",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "200")),
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=int(os.getenv("TRIGGER_SECONDS", "15")),
    )
    parser.add_argument(
        "--model-name",
        default=os.getenv("HF_MODEL_NAME", DEFAULT_MODEL_NAME),
        help="Hugging Face model id or a local model directory.",
    )
    parser.add_argument(
        "--hf-batch-size",
        type=int,
        default=int(os.getenv("HF_BATCH_SIZE", "16")),
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ES_INDEX", ES_INDEX_SENTIMENT),
        help="须与看板 ES_INDEX_SENTIMENT 一致。",
    )
    parser.add_argument(
        "--reset-index",
        action="store_true",
        help="Delete and recreate the Elasticsearch index before streaming.",
    )
    return parser.parse_args()


def normalize_email_text(text: str) -> str:
    if not text:
        return ""

    cleaned = text.replace("\r\n", "\n")
    cleaned = re.sub(r"(?im)^>.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^-+\s*original message\s*-+.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^from:.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^sent:.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^to:.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^cc:.*$", " ", cleaned)
    cleaned = re.sub(r"(?im)^subject:.*$", " ", cleaned)
    cleaned = re.sub(r"(?is)\*{6,}.*?\*{6,}", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()[:4000]


def get_classifier(model_name: str):
    global _MODEL
    if _MODEL is None:
        from transformers import pipeline

        _MODEL = pipeline(
            task="sentiment-analysis",
            model=model_name,
            tokenizer=model_name,
            truncation=True,
        )
    return _MODEL


def score_email_bodies(bodies: list[Any], model_name: str, batch_size: int) -> list[dict[str, Any]]:
    """在 driver 上跑 DistilBERT，避免 streaming + pandas_udf 走 Arrow（JDK 上易反复报错）。"""
    classifier = get_classifier(model_name)
    cleaned_texts = [normalize_email_text(value or "") for value in bodies]

    predictions: list[dict[str, Any] | None] = [None] * len(cleaned_texts)
    non_empty = [(idx, text) for idx, text in enumerate(cleaned_texts) if text]

    if non_empty:
        index_map, texts = zip(*non_empty)
        outputs = classifier(list(texts), batch_size=batch_size, truncation=True)
        for idx, output in zip(index_map, outputs):
            predictions[idx] = output

    rows: list[dict[str, Any]] = []
    for clean_body, output in zip(cleaned_texts, predictions):
        if not clean_body or output is None:
            rows.append(
                {
                    "clean_body": clean_body,
                    "sentiment": None,
                    "sentiment_score": None,
                }
            )
            continue

        label = str(output["label"]).upper()
        if label not in {"POSITIVE", "NEGATIVE"}:
            label = "POSITIVE" if "POS" in label else "NEGATIVE"

        rows.append(
            {
                "clean_body": clean_body,
                "sentiment": label,
                "sentiment_score": float(output["score"]),
            }
        )
    return rows


# 把原来的 kafka_options_from_env 整个删掉或替换成：

def kafka_options_from_env() -> dict[str, str]:
    from enron_common import get_kafka_options
    return get_kafka_options()



def create_index_if_needed(index_name: str, reset_index: bool) -> None:
    client = get_es_client()
    mapping = {
        "mappings": {
            "properties": {
                "file_path": {"type": "keyword"},
                "message_id": {"type": "keyword"},
                "sent_at": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSSZ||strict_date_optional_time||epoch_millis",
                },
                "sender": {"type": "keyword"},
                "to": {"type": "keyword"},
                "cc": {"type": "keyword"},
                "bcc": {"type": "keyword"},
                "subject": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "x_folder": {"type": "keyword"},
                "x_origin": {"type": "keyword"},
                "x_filename": {"type": "keyword"},
                "body": {"type": "text"},
                "clean_body": {"type": "text"},
                "body_length": {"type": "integer"},
                "sentiment": {"type": "keyword"},
                "sentiment_score": {"type": "float"},
                "ingested_at": {"type": "date"},
            }
        }
    }

    if reset_index and es_index_exists(client, index_name):
        client.indices.delete(index=index_name)

    if not es_index_exists(client, index_name):
        client.indices.create(
            index=index_name,
            mappings=mapping["mappings"],   # 注意这里
        )



def _json_safe_record(d: dict[str, Any]) -> dict[str, Any]:
    """把 pandas/numpy 标量转成 ES 可序列化类型。"""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        elif isinstance(v, (np.floating, np.integer)):
            x = v.item()
            out[k] = None if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) else x
        elif isinstance(v, np.bool_):
            out[k] = bool(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


_SENTIMENT_ES_FIELDS = frozenset(
    {
        "file_path",
        "message_id",
        "sent_at",
        "sender",
        "to",
        "cc",
        "bcc",
        "subject",
        "x_folder",
        "x_origin",
        "x_filename",
        "body",
        "clean_body",
        "body_length",
        "sentiment",
        "sentiment_score",
        "ingested_at",
    }
)


def _split_emails(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    text = str(raw_value)
    if not text:
        return []
    parts = re.split(r"\s*[,;]\s*", text)
    out = []
    for p in parts:
        v = p.strip().lower()
        if "@" in v:
            out.append(v)
    return out


def _format_sent_at_for_es(value: Any) -> str | None:
    """与 2-spark-streaming.ipynb 对齐：yyyy-MM-dd'T'HH:mm:ss.SSSZ。"""
    if value is None:
        return None
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            ms = dt.strftime("%f")[:3]
            return dt.strftime("%Y-%m-%dT%H:%M:%S.") + ms + dt.strftime("%z")
        text = str(value).strip()
        if not text:
            return None
        # 兜底：原样返回
        return text
    except Exception:
        return str(value)


def _sentiment_es_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """只保留索引 mapping 中的字段；字段命名对齐 2-spark-streaming.ipynb。"""
    p = _json_safe_record(raw)
    p["to"] = str(p.get("to") or "")
    p["cc"] = str(p.get("cc") or "")
    p["bcc"] = str(p.get("bcc") or "")
    p["sent_at"] = _format_sent_at_for_es(p.get("sent_at"))
    p["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return {k: p[k] for k in p if k in _SENTIMENT_ES_FIELDS}


def write_batch_to_es(index_name: str, model_name: str, hf_batch_size: int):
    def _writer(df: DataFrame, epoch_id: int) -> None:
        if not df.take(1):
            return

        pdf = df.toPandas()

        # 同一批内去重，避免重复邮件重复做模型推理/写入 ES
        doc_id = pdf["message_id"].where(pdf["message_id"].notna() & (pdf["message_id"].astype(str) != ""), pdf["file_path"])
        pdf["_doc_id"] = doc_id
        pdf = pdf[pdf["_doc_id"].notna() & (pdf["_doc_id"].astype(str) != "")].drop_duplicates(subset=["_doc_id"], keep="last")
        if pdf.empty:
            print(f"Batch {epoch_id}: 0 documents after dedup", flush=True)
            return

        bodies = pdf["body"].tolist()
        scores = score_email_bodies(bodies, model_name, hf_batch_size)
        pdf["clean_body"] = [s["clean_body"] for s in scores]
        pdf["sentiment"] = [s["sentiment"] for s in scores]
        pdf["sentiment_score"] = [s["sentiment_score"] for s in scores]

        client = get_es_client()
        sentiment_actions = []
        edge_actions = []

        for raw in pdf.to_dict(orient="records"):
            payload = _sentiment_es_payload(raw)
            document_id = raw.get("_doc_id") or payload.get("message_id") or payload.get("file_path")
            if not document_id:
                continue

            sentiment_actions.append(
                {
                    "_index": index_name,
                    "_id": str(document_id),
                    "_source": payload,
                }
            )

            sender = str(payload.get("sender") or "").strip().lower()
            if "@" not in sender:
                continue

            recipients = (
                _split_emails(payload.get("to"))
                + _split_emails(payload.get("cc"))
                + _split_emails(payload.get("bcc"))
            )
            if not recipients:
                continue

            score_val = payload.get("sentiment_score")
            try:
                score_num = float(score_val) if score_val is not None else 0.0
            except Exception:
                score_num = 0.0
            signed_sentiment = score_num if str(payload.get("sentiment") or "").upper() == "POSITIVE" else -score_num

            sent_at_val = payload.get("sent_at")
            sent_at_str = str(sent_at_val) if sent_at_val else None
            subject = str(payload.get("subject") or "")

            for dst in set(recipients):
                if "@" not in dst or dst == sender:
                    continue
                edge_id = hashlib.md5(f"{sender}||{dst}".encode()).hexdigest()
                edge_actions.append(
                    {
                        "_op_type": "update",
                        "_index": ES_INDEX_EDGES,
                        "_id": edge_id,
                        "script": {
                            "source": """
                                if (ctx._source.weight == null) ctx._source.weight = 0;
                                if (ctx._source.sentiment_sum == null) ctx._source.sentiment_sum = 0.0;
                                ctx._source.weight += params.inc;
                                ctx._source.sentiment_sum += params.sent;
                                ctx._source.avg_sentiment = ctx._source.weight > 0 ? (ctx._source.sentiment_sum / ctx._source.weight) : 0.0;
                                if (params.time != null) ctx._source.last_contact_at = params.time;
                                if (params.subject != null && params.subject.length() > 0) ctx._source.sample_subject = params.subject;
                            """,
                            "params": {
                                "inc": 1,
                                "sent": signed_sentiment,
                                "time": sent_at_str,
                                "subject": subject,
                            },
                        },
                        "upsert": {
                            "src": sender,
                            "dst": dst,
                            "weight": 1,
                            "sentiment_sum": signed_sentiment,
                            "avg_sentiment": signed_sentiment,
                            "first_contact_at": sent_at_str,
                            "last_contact_at": sent_at_str,
                            "sample_subject": subject,
                        },
                    }
                )

        success, failures = helpers.bulk(
            client,
            sentiment_actions,
            stats_only=False,
            raise_on_error=False,
        )
        n_fail = len(failures) if isinstance(failures, list) else int(failures or 0)

        if edge_actions:
            helpers.bulk(client, edge_actions, raise_on_error=False)

        print(
            f"Batch {epoch_id}: sentiments={success}, sentiment_failures={n_fail}, edge_updates={len(edge_actions)}",
            flush=True,
        )
        if isinstance(failures, list) and failures:
            print(json.dumps(failures[0], ensure_ascii=False, default=str), flush=True)

    return _writer


def ensure_edges_index() -> None:
    client = get_es_client()
    if es_index_exists(client, ES_INDEX_EDGES):
        return
    client.indices.create(index=ES_INDEX_EDGES, body={
        "mappings": {"properties": {
            "src": {"type": "keyword"},
            "dst": {"type": "keyword"},
            "weight": {"type": "integer"},
            "first_contact_at": {"type": "date"},
            "last_contact_at": {"type": "date"},
            "sample_subject": {"type": "text"},
            "sentiment_sum": {"type": "float"},
            "avg_sentiment": {"type": "float"},
        }}
    })


def _ensure_ml_deps() -> None:
    """在拉起 Spark 之前检查，避免流跑起来后才因缺包失败。"""
    import sys

    try:
        import transformers  # noqa: F401
        import torch  # noqa: F401
    except ModuleNotFoundError as e:
        raise SystemExit(
            f"当前 Python 未安装情感分析依赖 ({e}).\n"
            f"  解释器: {sys.executable}\n"
            "  请安装:  python3 -m pip install -r requirements.txt\n"
            "  若依赖装在别的 Python 上，请用那个解释器启动，或设置环境变量 PYTHON_BIN 后运行 run.sh。"
        ) from e


def main() -> None:
    args = parse_args()
    _ensure_ml_deps()
    create_index_if_needed(args.index, args.reset_index)
    ensure_edges_index()

    spark = build_spark_session(
        app_name="EnronKafkaDistilBERTToES",
        include_kafka=True,
    )
    # foreachBatch 里用 toPandas()；关闭 Arrow 可避免部分 JDK 上 Arrow 内存 API 报错
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    reader = (
        spark.readStream.format("kafka")
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
    )

    for key, value in kafka_options_from_env().items():
        reader = reader.option(key, value)

    raw_stream = reader.load()

    from pyspark.sql import functions as F

    parsed_df = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), KAFKA_EMAIL_JSON_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("sent_at", F.regexp_replace(F.col("sent_at"), " UTC$", ""))
        .withColumn("sent_at", F.to_timestamp(F.col("sent_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("sent_at", F.date_format(F.col("sent_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    )

    query = (
        parsed_df.writeStream.foreachBatch(
            write_batch_to_es(args.index, args.model_name, args.hf_batch_size)
        )
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", args.checkpoint_dir)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
# python3 data_preparation/scripts/produce_emails_to_kafka.py \
#   --bootstrap-servers localhost:9092 \
#   --topic raw_emails_topic
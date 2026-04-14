from __future__ import annotations

import warnings

try:
    from urllib3.exceptions import NotOpenSSLWarning

    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except Exception:
    pass

from urllib.parse import urlparse, urlunparse

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

# ============ 邮件 Schema ============
EMAIL_SCHEMA = StructType([
    StructField("message_id", StringType(), True),
    StructField("sender", StringType(), True),
    StructField("recipients", ArrayType(StringType()), True),
    StructField("cc", ArrayType(StringType()), True),
    StructField("bcc", ArrayType(StringType()), True),
    StructField("subject", StringType(), True),
    StructField("body", StringType(), True),
    StructField("sent_at", TimestampType(), True),
    StructField("has_attachments", BooleanType(), True),
    StructField("attachment_names", ArrayType(StringType()), True),
    StructField("folder", StringType(), True),
    StructField("thread_id", StringType(), True),
])

# data_preparation/scripts/email_record_utils.build_record + produce_emails_to_kafka 的 value 格式
KAFKA_EMAIL_JSON_SCHEMA = StructType([
    StructField("file_path", StringType(), True),
    StructField("message_id", StringType(), True),
    StructField("sent_at", StringType(), True),
    StructField("sender", StringType(), True),
    StructField("to", StringType(), True),
    StructField("cc", StringType(), True),
    StructField("bcc", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("x_folder", StringType(), True),
    StructField("x_origin", StringType(), True),
    StructField("x_filename", StringType(), True),
    StructField("body", StringType(), True),
    # 与 2-spark-streaming.ipynb 保持一致：IntegerType
    StructField("body_length", IntegerType(), True),
])

# ============ 运行环境选择（local / external） ============
# local: 默认本地 Kafka + 本地 Elasticsearch
# external: 通过 EXTERNAL_* 环境变量切到外部 Kafka/ES（可对齐 2-spark-streaming.ipynb）
ENRON_PROFILE = os.getenv("ENRON_PROFILE", "local").strip().lower()


# ============ Kafka 配置（全部带默认值，不会炸） ============
# 统一默认 topic 为 raw_emails_topic
_LOCAL_KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_LOCAL_KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_emails_topic")
_LOCAL_KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")
_LOCAL_KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
_LOCAL_KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
_LOCAL_KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")

_EXTERNAL_KAFKA_BOOTSTRAP_SERVERS = os.getenv("EXTERNAL_KAFKA_BOOTSTRAP_SERVERS")
_EXTERNAL_KAFKA_TOPIC = os.getenv("EXTERNAL_KAFKA_TOPIC", _LOCAL_KAFKA_TOPIC)
_EXTERNAL_KAFKA_SECURITY_PROTOCOL = os.getenv("EXTERNAL_KAFKA_SECURITY_PROTOCOL")
_EXTERNAL_KAFKA_SASL_MECHANISM = os.getenv("EXTERNAL_KAFKA_SASL_MECHANISM")
_EXTERNAL_KAFKA_SASL_USERNAME = os.getenv("EXTERNAL_KAFKA_SASL_USERNAME")
_EXTERNAL_KAFKA_SASL_PASSWORD = os.getenv("EXTERNAL_KAFKA_SASL_PASSWORD")

if ENRON_PROFILE == "external" and _EXTERNAL_KAFKA_BOOTSTRAP_SERVERS:
    KAFKA_BOOTSTRAP_SERVERS = _EXTERNAL_KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC = _EXTERNAL_KAFKA_TOPIC
    KAFKA_SECURITY_PROTOCOL = _EXTERNAL_KAFKA_SECURITY_PROTOCOL
    KAFKA_SASL_MECHANISM = _EXTERNAL_KAFKA_SASL_MECHANISM
    KAFKA_SASL_USERNAME = _EXTERNAL_KAFKA_SASL_USERNAME
    KAFKA_SASL_PASSWORD = _EXTERNAL_KAFKA_SASL_PASSWORD
else:
    KAFKA_BOOTSTRAP_SERVERS = _LOCAL_KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC = _LOCAL_KAFKA_TOPIC
    KAFKA_SECURITY_PROTOCOL = _LOCAL_KAFKA_SECURITY_PROTOCOL
    KAFKA_SASL_MECHANISM = _LOCAL_KAFKA_SASL_MECHANISM
    KAFKA_SASL_USERNAME = _LOCAL_KAFKA_SASL_USERNAME
    KAFKA_SASL_PASSWORD = _LOCAL_KAFKA_SASL_PASSWORD


# ============ ES 配置（全部带默认值，不会炸） ============
_LOCAL_ES_HOST = os.getenv("ES_HOST", "localhost")
_LOCAL_ES_PORT = os.getenv("ES_PORT", "9200")
_LOCAL_ES_API_KEY = os.getenv("ES_API_KEY")
_LOCAL_ES_VERIFY_CERTS = os.getenv("ES_VERIFY_CERTS", "false")

_EXTERNAL_ES_HOST = os.getenv("EXTERNAL_ES_HOST")
_EXTERNAL_ES_PORT = os.getenv("EXTERNAL_ES_PORT", "443")
_EXTERNAL_ES_API_KEY = os.getenv("EXTERNAL_ES_API_KEY")
_EXTERNAL_ES_VERIFY_CERTS = os.getenv("EXTERNAL_ES_VERIFY_CERTS", "false")

if ENRON_PROFILE == "external" and _EXTERNAL_ES_HOST:
    ES_HOST = _EXTERNAL_ES_HOST
    ES_PORT = _EXTERNAL_ES_PORT
    ES_API_KEY = _EXTERNAL_ES_API_KEY
    ES_VERIFY_CERTS = _EXTERNAL_ES_VERIFY_CERTS
else:
    ES_HOST = _LOCAL_ES_HOST
    ES_PORT = _LOCAL_ES_PORT
    ES_API_KEY = _LOCAL_ES_API_KEY
    ES_VERIFY_CERTS = _LOCAL_ES_VERIFY_CERTS

# 所有 ES 索引名（情感与 stream_enron_sentiment 默认写入一致，便于看板）
ES_INDEX_SENTIMENT = os.getenv("ES_INDEX_SENTIMENT", "enron_emails")
ES_INDEX_EDGES = "enron_edges"
ES_INDEX_VERTEX_METRICS = "enron_vertex_metrics"
ES_INDEX_COMMUNITIES = "enron_communities"
ES_INDEX_STATS = "enron_stream_stats"

# JDK 9+ 模块系统会阻止 Arrow 为 pandas_udf 分配 direct buffer，触发
# UnsupportedOperationException: DirectByteBuffer.<init>(long, int) not available
_SPARK_ARROW_JAVA_OPTS = (
    "-Dio.netty.tryReflectionSetAccessible=true "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED "
)


# ============ Spark Session ============
def build_spark_session(
    app_name: str = "EnronEmailAnalysis",
    with_graphframes: bool = False,
    include_kafka: bool = False,
    driver_memory: str = "4g",
) -> SparkSession:
    """构建 Spark Session，按需加载 GraphFrames 和 Kafka"""
    packages = []
    if with_graphframes:
        packages.append("graphframes:graphframes:0.8.4-spark3.5-s_2.12")
    if include_kafka:
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", _SPARK_ARROW_JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", _SPARK_ARROW_JAVA_OPTS)
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    return builder.getOrCreate()


# ============ ES 工具函数 ============
def get_elasticsearch_url() -> str:
    """单节点连接 URL。若 ES_HOST 已是 http://host:9200，则不再追加 ES_PORT。"""
    raw = (ES_HOST or "localhost").strip()
    port = str(ES_PORT or "9200").strip()
    if "://" not in raw:
        return f"http://{raw}:{port}"
    parts = urlparse(raw)
    if parts.port is not None:
        return raw.rstrip("/")
    hostname = parts.hostname or "localhost"
    auth = ""
    if parts.username is not None:
        auth = (
            f"{parts.username}:{parts.password}@"
            if parts.password is not None
            else f"{parts.username}@"
        )
    netloc = f"{auth}{hostname}:{port}"
    return urlunparse(
        (
            parts.scheme or "http",
            netloc,
            parts.path or "",
            parts.params,
            parts.query,
            parts.fragment,
        )
    )


def get_es_client():
    """获取 ES 客户端（本地开发不需要 api_key 和证书）"""
    from elasticsearch import Elasticsearch

    url = get_elasticsearch_url()
    verify = ES_VERIFY_CERTS.lower() == "true"
    if ES_API_KEY:
        return Elasticsearch(url, api_key=ES_API_KEY, verify_certs=verify)
    return Elasticsearch(url, verify_certs=verify)


def es_index_exists(client, index_name: str) -> bool:
    """判断索引是否存在。部分环境下 indices.exists 会 400，故用 get + 404 判断。"""
    try:
        client.indices.get(index=index_name)
        return True
    except Exception as exc:  # noqa: BLE001 — 需兼容 elasticsearch / elastic_transport 多种类型
        meta = getattr(exc, "meta", None)
        status = getattr(meta, "status", None) if meta is not None else None
        if status is None:
            status = getattr(exc, "status_code", None)
        if status == 404:
            return False
        raise


def safe_count(index: str) -> int:
    """安全获取索引文档数"""
    try:
        es = get_es_client()
        return es.count(index=index)["count"]
    except Exception:
        return 0


def _split_address_string_col(col):
    """把逗号/分号分隔的邮箱串拆成数组（与 Spark SQL 列表达式配合）。"""
    from pyspark.sql import functions as F

    parts = F.split(F.regexp_replace(F.coalesce(col, F.lit("")), r"\s*[,;]\s*", ","), ",")
    trimmed = F.transform(parts, lambda x: F.trim(x))
    return F.filter(trimmed, lambda x: F.length(x) > 0)


def kafka_flat_to_email_df(flat_df):
    """
    将 data_preparation 产出的扁平 JSON 列转为 EMAIL_SCHEMA，并保留 file_path/body_length 等供 ES 索引。
    flat_df: from_json(..., KAFKA_EMAIL_JSON_SCHEMA) 展开后的列。
    """
    from pyspark.sql import functions as F

    to_s = F.col("to")
    cc_s = F.col("cc")
    bcc_s = F.col("bcc")
    sent_ts = F.to_timestamp(
        F.regexp_replace(F.col("sent_at"), " UTC$", ""),
        "yyyy-MM-dd HH:mm:ss",
    )
    empty_arr = F.lit(None).cast(ArrayType(StringType()))

    return (
        flat_df.select(
            F.col("message_id"),
            F.col("sender"),
            _split_address_string_col(to_s).alias("recipients"),
            _split_address_string_col(cc_s).alias("cc"),
            _split_address_string_col(bcc_s).alias("bcc"),
            F.col("subject"),
            F.col("body"),
            sent_ts.alias("sent_at"),
            F.lit(False).alias("has_attachments"),
            empty_arr.alias("attachment_names"),
            F.col("x_folder").alias("folder"),
            F.lit(None).cast(StringType()).alias("thread_id"),
            F.col("file_path"),
            F.col("body_length"),
            to_s.alias("to_es"),
            cc_s.alias("cc_es"),
            bcc_s.alias("bcc_es"),
            F.col("x_folder"),
            F.col("x_origin"),
            F.col("x_filename"),
        )
    )


def kafka_stream_to_email_df(kafka_df):
    """readStream Kafka load() → 与 EMAIL_SCHEMA 对齐的邮件列 + 原始元数据列。"""
    from pyspark.sql import functions as F

    j = kafka_df.selectExpr("CAST(value AS STRING) AS kafka_json")
    flat = j.select(F.from_json(F.col("kafka_json"), KAFKA_EMAIL_JSON_SCHEMA).alias("_k")).select("_k.*")
    return kafka_flat_to_email_df(flat)


# ============ Kafka 配置字典（给两个 Spark 脚本共用） ============
def get_kafka_options() -> dict:
    """构建 Kafka 连接选项，兼容 SSL/SASL"""
    options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    if KAFKA_SECURITY_PROTOCOL:
        options["kafka.security.protocol"] = KAFKA_SECURITY_PROTOCOL
    if KAFKA_SASL_MECHANISM:
        options["kafka.sasl.mechanism"] = KAFKA_SASL_MECHANISM
    if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
        options["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";'
        )
    return options
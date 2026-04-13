import argparse
import hashlib
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F

try:
    from graphframes import GraphFrame
except ImportError as e:
    raise SystemExit(
        "未安装 GraphFrames Python 包。官方 graphframes-py 需要 Python>=3.10，请用 3.10+ 执行:\n"
        "  python3 -m pip install 'graphframes-py==0.11.0'\n"
        "若你仍在 Python 3.9 上，情感分析可照常安装 requirements.txt；关系图请换 3.10+ 解释器或单独环境。"
    ) from e

from enron_common import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    ES_INDEX_EDGES,
    ES_INDEX_VERTEX_METRICS,
    ES_INDEX_COMMUNITIES,
    ES_INDEX_STATS,
    build_spark_session,
    es_index_exists,
    get_es_client,
    get_kafka_options,
    kafka_stream_to_email_df,
)

def edge_doc_id(src: str, dst: str) -> str:
    """用 src||dst 的 MD5 当文档 ID，保证同一条边只更新不重复"""
    return hashlib.md5(f"{src}||{dst}".encode()).hexdigest()


# ============ 边写入 ES ============
def ensure_edges_index(es):
    if not es_index_exists(es, ES_INDEX_EDGES):
        es.indices.create(index=ES_INDEX_EDGES, body={
            "mappings": {"properties": {
                "src": {"type": "keyword"},
                "dst": {"type": "keyword"},
                "weight": {"type": "integer"},
                "first_contact_at": {"type": "date"},
                "last_contact_at": {"type": "date"},
                "sample_subject": {"type": "text"},
            }}
        })


def upsert_edges_to_es(edges_df):
    """把一批边聚合后 upsert 到 ES（避免百万级明细直接 toPandas 卡死）。"""
    from elasticsearch.helpers import bulk

    es = get_es_client()
    ensure_edges_index(es)

    agg_df = (
        edges_df.groupBy("src", "dst")
        .agg(
            F.count("*").alias("inc"),
            F.max("sent_at").alias("last_contact_at"),
            F.first("subject", ignorenulls=True).alias("sample_subject"),
        )
    )

    chunk_size = 5000
    actions = []
    total_actions = 0

    for row in agg_df.toLocalIterator():
        src = row["src"]
        dst = row["dst"]
        inc = int(row["inc"] or 0)
        time_str = str(row["last_contact_at"]) if row["last_contact_at"] else None
        subject = row["sample_subject"] or ""

        actions.append(
            {
                "_op_type": "update",
                "_index": ES_INDEX_EDGES,
                "_id": edge_doc_id(src, dst),
                "script": {
                    "source": """
                        if (ctx._source.weight == null) ctx._source.weight = 0;
                        ctx._source.weight += params.inc;
                        if (params.time != null) ctx._source.last_contact_at = params.time;
                        if (params.subject != null && params.subject.length() > 0)
                            ctx._source.sample_subject = params.subject;
                    """,
                    "params": {"inc": inc, "time": time_str, "subject": subject},
                },
                "upsert": {
                    "src": src,
                    "dst": dst,
                    "weight": inc,
                    "first_contact_at": time_str,
                    "last_contact_at": time_str,
                    "sample_subject": subject,
                },
            }
        )

        if len(actions) >= chunk_size:
            bulk(es, actions, raise_on_error=False)
            total_actions += len(actions)
            actions = []

    if actions:
        bulk(es, actions, raise_on_error=False)
        total_actions += len(actions)

    return total_actions


# ============ 从 ES 读全部边 ============
def read_all_edges_from_es():
    es = get_es_client()
    if not es_index_exists(es, ES_INDEX_EDGES):
        return []

    edges = []
    resp = es.search(index=ES_INDEX_EDGES, query={"match_all": {}}, size=10000, scroll="2m")
    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]
    edges.extend([h["_source"] for h in hits])

    while hits:
        resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = resp["_scroll_id"]
        hits = resp["hits"]["hits"]
        edges.extend([h["_source"] for h in hits])

    try:
        es.clear_scroll(scroll_id=scroll_id)
    except Exception:
        pass
    return edges


# ============ 图计算核心 ============
def compute_and_save_graph_metrics(spark):
    """从 ES 读边 → 算 PageRank/度/社区 → 写回 ES"""
    from elasticsearch.helpers import bulk

    es = get_es_client()
    try:
        es.indices.refresh(index=ES_INDEX_EDGES)
    except Exception:
        pass

    edges_data = read_all_edges_from_es()

    if not edges_data:
        print("  [Graph] 没有边，跳过计算", flush=True)
        return

    rows = []
    for e in edges_data:
        src = e.get("src")
        dst = e.get("dst")
        w = e.get("weight", 0)
        if not src or not dst:
            continue
        try:
            weight = int(w or 0)
        except Exception:
            weight = 0
        if weight <= 0:
            continue
        rows.append((str(src), str(dst), weight))

    if not rows:
        print("  [Graph] 边数据为空或无有效权重，跳过计算", flush=True)
        return

    print(f"  [Graph] 基于 {len(rows)} 条唯一边计算图指标...", flush=True)

    # --- 构建 GraphFrame ---
    edges_df = spark.createDataFrame(rows, schema="src string, dst string, weight int")
    vertices_df = (
        edges_df.select(F.col("src").alias("id"))
        .union(edges_df.select(F.col("dst").alias("id")))
        .distinct()
    )
    gf_edges = edges_df.select(
        F.col("src").alias("src"),
        F.col("dst").alias("dst"),
        F.col("weight"),
    )
    g = GraphFrame(vertices_df, gf_edges)

    # --- PageRank ---
    print("  [Graph] PageRank...")
    pr_df = g.pageRank(resetProbability=0.15, maxIter=10).vertices

    # --- 度 ---
    print("  [Graph] 度计算...")
    deg_df = g.degrees
    in_df = g.inDegrees
    out_df = g.outDegrees

    metrics_df = (
        vertices_df
        .join(pr_df, "id", "left")
        .join(deg_df, "id", "left")
        .join(in_df, "id", "left")
        .join(out_df, "id", "left")
        .fillna(0)
        .withColumn("is_enron", F.col("id").contains("enron"))
    )

    # --- 连通分量（当社区用） ---
    print("  [Graph] 连通分量...")
    cc_df = g.connectedComponents()
    metrics_df = metrics_df.join(
        cc_df.select("id", F.col("component").alias("community_id")), "id", "left"
    )

    # --- 写顶点指标到 ES ---
    print("  [Graph] 写入顶点指标...")
    if not es_index_exists(es, ES_INDEX_VERTEX_METRICS):
        es.indices.create(index=ES_INDEX_VERTEX_METRICS, body={
            "mappings": {"properties": {
                "email": {"type": "keyword"},
                "in_degree": {"type": "integer"},
                "out_degree": {"type": "integer"},
                "total_degree": {"type": "integer"},
                "pagerank": {"type": "float"},
                "community_id": {"type": "long"},
                "is_enron": {"type": "boolean"},
            }}
        })

    metrics_pdf = metrics_df.toPandas()
    v_actions = []
    for _, row in metrics_pdf.iterrows():
        v_actions.append({
            "_op_type": "index",
            "_index": ES_INDEX_VERTEX_METRICS,
            "_id": str(row["id"]),
            "email": str(row["id"]),
            "in_degree": int(row.get("inDegree", 0)),
            "out_degree": int(row.get("outDegree", 0)),
            "total_degree": int(row["degree"]),
            "pagerank": float(row["pagerank"]),
            "community_id": int(row["community_id"])
            if pd.notna(row["community_id"])
            else -1,
            "is_enron": bool(row["is_enron"]),
        })
    if v_actions:
        bulk(es, v_actions, raise_on_error=False)

    # --- 社区摘要 ---
    print("  [Graph] 社区摘要...")
    comm_df = (
        metrics_df.groupBy("community_id")
        .agg(
            F.count("id").alias("member_count"),
            F.sum(F.col("is_enron").cast("int")).alias("enron_member_count"),
            F.max("pagerank").alias("max_pagerank"),
        )
        .withColumn("all_enron", F.col("member_count") == F.col("enron_member_count"))
    )

    if not es_index_exists(es, ES_INDEX_COMMUNITIES):
        es.indices.create(index=ES_INDEX_COMMUNITIES, body={
            "mappings": {"properties": {
                "community_id": {"type": "long"},
                "member_count": {"type": "integer"},
                "enron_member_count": {"type": "integer"},
                "max_pagerank": {"type": "float"},
                "all_enron": {"type": "boolean"},
            }}
        })

    es.delete_by_query(index=ES_INDEX_COMMUNITIES, body={"query": {"match_all": {}}}, refresh=True)
    comm_pdf = comm_df.toPandas()
    c_actions = []
    for _, row in comm_pdf.iterrows():
        c_actions.append({
            "_op_type": "index",
            "_index": ES_INDEX_COMMUNITIES,
            "community_id": int(row["community_id"]),
            "member_count": int(row["member_count"]),
            "enron_member_count": int(row["enron_member_count"]),
            "max_pagerank": float(row["max_pagerank"]),
            "all_enron": bool(row["all_enron"]),
        })
    if c_actions:
        bulk(es, c_actions, raise_on_error=False)

    # --- 更新统计 ---
    es.index(index=ES_INDEX_STATS, id="graph", body={
        "type": "graph",
        "total_edges": len(edges_data),
        "total_vertices": metrics_pdf.shape[0],
        "total_communities": comm_pdf.shape[0],
        "last_computed_at": datetime.now().isoformat(),
    })

    print(f"  [Graph] 完成! {metrics_pdf.shape[0]} 节点, {len(edges_data)} 边, {comm_pdf.shape[0]} 社区")


# ============ 主入口 ============
def main():
    parser = argparse.ArgumentParser(description="Kafka 流 → 关系图分析 → ES")
    parser.add_argument("--recompute-interval", type=int, default=10)
    parser.add_argument("--trigger-interval", type=str, default="10 seconds")
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka 起始 offset：首次回放建议 earliest，稳定运行建议 latest",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default="checkpoints/graph_stream",
        help="Structured Streaming checkpoint 目录",
    )
    args = parser.parse_args()

    spark = build_spark_session(
        app_name="EnronGraphStream",
        with_graphframes=True,
        include_kafka=True,
        driver_memory="4g",
    )
    spark.sparkContext.setLogLevel("WARN")

    # 从 Kafka 读流（与 stream_enron_sentiment 相同 SASL 选项）
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
    )
    for key, value in get_kafka_options().items():
        reader = reader.option(key, value)
    stream_df = reader.load()

    parsed_df = kafka_stream_to_email_df(stream_df)

    recompute_interval = max(1, args.recompute_interval)
    trigger_interval = args.trigger_interval
    checkpoint_dir = args.checkpoint_dir

    # --- foreachBatch 闭包，把需要的外部变量作为参数传进去 ---
    def process_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                print(f"[Batch {batch_id}] 空批次，等待新邮件...", flush=True)
                return

            count = batch_df.count()
            print(f"[Batch {batch_id}] 收到 {count} 封邮件", flush=True)

            dedup_batch_df = (
                batch_df
                .filter(F.col("message_id").isNotNull() & (F.length(F.trim(F.col("message_id"))) > 0))
                .dropDuplicates(["message_id"])
            )

            edges_df = (
                dedup_batch_df
                .select(
                    F.col("sender").alias("src"),
                    F.explode("recipients").alias("dst"),
                    F.col("subject"),
                    F.col("sent_at"),
                )
                .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
                .filter(F.col("src") != F.col("dst"))
                .filter(F.col("src").contains("@") & F.col("dst").contains("@"))
            )

            dedup_count = dedup_batch_df.count()
            edge_count = edges_df.count()
            print(f"  [Batch {batch_id}] 去重后邮件 {dedup_count} 封，可写入关系边 {edge_count} 条", flush=True)
            if edge_count > 0:
                upserted = upsert_edges_to_es(edges_df)
                print(f"  [Batch {batch_id}] 已 upsert {upserted} 条唯一边（原始关系 {edge_count}）", flush=True)

            # 含 batch_id==0：否则首批已写入的边在 ES 里已有，但顶点指标要等到第 N 个微批才算，看板会一直判「无图」
            if batch_id == 0 or batch_id % recompute_interval == 0:
                print(f"  [Batch {batch_id}] 触发图计算...", flush=True)
                compute_and_save_graph_metrics(batch_df.sparkSession)

        except Exception as e:
            print(f"  [Batch {batch_id}] 出错: {e}", flush=True)

    query = (
        parsed_df
        .writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("update")
        .start()
    )

    print("=" * 60, flush=True)
    print("🔗 关系图流处理已启动", flush=True)
    print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}", flush=True)
    print(f"   startingOffsets: {args.starting_offsets}", flush=True)
    print(f"   checkpoint: {checkpoint_dir}", flush=True)
    print(f"   图重算间隔: 每 {recompute_interval} 个微批", flush=True)
    print(f"   触发间隔: {trigger_interval}", flush=True)
    print("=" * 60, flush=True)

    query.awaitTermination()


if __name__ == "__main__":
    main()
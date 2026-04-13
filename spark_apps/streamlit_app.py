"""Enron 邮件实时分析看板 — 带系统监控 & 进度条"""

from __future__ import annotations

import socket
import time
from email.utils import parseaddr

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from enron_common import (
    ES_INDEX_COMMUNITIES,
    ES_INDEX_EDGES,
    ES_INDEX_SENTIMENT,
    ES_INDEX_STATS,
    ES_INDEX_VERTEX_METRICS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    get_es_client,
    safe_count,
)

es = get_es_client()


# ═══════════════════════════════════════════
#  工具函数：健康检查 / Kafka 探测 / 格式化
# ═══════════════════════════════════════════

def _kafka_host_port() -> tuple[str, int]:
    first = (KAFKA_BOOTSTRAP_SERVERS or "localhost:9092").split(",")[0].strip()
    if ":" in first:
        host, _, port_s = first.rpartition(":")
        return host.strip("[]"), int(port_s)
    return first, 9092


def _check_port(host: str, port: int, timeout: float = 2) -> bool:
    """TCP 端口可达性探测。"""
    try:
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True
    except (socket.timeout, socket.error, OSError):
        return False


def _get_kafka_topic_total(topic: str | None = None) -> int | None:
    """从 Kafka 获取 topic 消息总数（用于进度条分母）。"""
    try:
        from kafka import KafkaConsumer, TopicPartition

        t = topic or KAFKA_TOPIC
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            consumer_timeout_ms=3000,
        )
        parts = consumer.partitions_for_topic(t)
        if not parts:
            consumer.close()
            return None
        total = 0
        for p in parts:
            tp = TopicPartition(t, p)
            consumer.assign([tp])
            consumer.seek_to_end(tp)
            total += consumer.position(tp)
        consumer.close()
        return total
    except Exception:
        return None


def _status_badge(status) -> str:
    """把服务状态变成 emoji 标签。"""
    m = {
        True: "运行中",
        "running": "运行中",
        "idle": "等待数据",
        "done": "已完成",
        False: "未连接",
        "error": "异常",
    }
    return m.get(status, "未知")


def _format_eta(seconds: float) -> str:
    if seconds <= 0 or seconds != seconds:
        return "计算中…"
    s = int(seconds)
    if s < 60:
        return f"{s} 秒"
    if s < 3600:
        return f"{s // 60} 分 {s % 60} 秒"
    if s < 86400:
        return f"{s // 3600} 小时 {(s % 3600) // 60} 分"
    return f"{s // 86400} 天 {(s % 86400) // 3600} 小时"


def _get_services() -> dict[str, str]:
    """返回所有服务状态。"""
    svc: dict[str, str] = {}

    # Elasticsearch
    try:
        es.ping()
        svc["Elasticsearch"] = "running"
    except Exception:
        svc["Elasticsearch"] = False  # type: ignore[assignment]

    # Kafka
    kh, kp = _kafka_host_port()
    kafka_up = _check_port(kh, kp)
    svc["Kafka"] = "running" if kafka_up else False  # type: ignore[assignment]

    # Spark 情感分析 — 通过 ES 中是否有 sentiment 数据判断
    sent_n = safe_count(ES_INDEX_SENTIMENT)
    svc["情感分析"] = "running" if sent_n > 0 else ("idle" if kafka_up else False)

    # Spark 关系图谱
    edge_n = safe_count(ES_INDEX_EDGES)
    svc["关系图谱"] = "running" if edge_n > 0 else ("idle" if kafka_up else False)

    return svc


# ═══════════════════════════════════════════
#  数据读取（保持原有逻辑）
# ═══════════════════════════════════════════

def get_sentiment_dist():
    try:
        resp = es.search(index=ES_INDEX_SENTIMENT, size=0, aggs={
            "by_sentiment": {"terms": {"field": "sentiment", "size": 10}},
        })
        return {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_sentiment"]["buckets"]}
    except Exception:
        return {}


def get_recent_emails(n: int = 15):
    try:
        resp = es.search(
            index=ES_INDEX_SENTIMENT,
            query={"match_all": {}},
            size=n,
            sort=[
                {"ingested_at": {"order": "desc", "unmapped_type": "date"}},
                {"sent_at": {"order": "desc", "unmapped_type": "date"}},
            ],
            _source=["sender", "subject", "sentiment", "sentiment_score", "sent_at", "ingested_at"],
        )
        return [h["_source"] for h in resp["hits"]["hits"]]
    except Exception:
        return []


def _get_top_people_from_edges(n: int = 40):
    try:
        src_resp = es.search(
            index=ES_INDEX_EDGES,
            size=0,
            aggs={"top_src": {"terms": {"field": "src", "size": n * 3}}},
        )
        dst_resp = es.search(
            index=ES_INDEX_EDGES,
            size=0,
            aggs={"top_dst": {"terms": {"field": "dst", "size": n * 3}}},
        )

        people: dict[str, dict] = {}

        for b in src_resp.get("aggregations", {}).get("top_src", {}).get("buckets", []):
            email = b.get("key")
            cnt = int(b.get("doc_count", 0))
            if not email:
                continue
            people.setdefault(email, {"email": email, "in_degree": 0, "out_degree": 0, "total_degree": 0, "pagerank": 0.0, "is_enron": "enron" in email})
            people[email]["out_degree"] += cnt
            people[email]["total_degree"] += cnt

        for b in dst_resp.get("aggregations", {}).get("top_dst", {}).get("buckets", []):
            email = b.get("key")
            cnt = int(b.get("doc_count", 0))
            if not email:
                continue
            people.setdefault(email, {"email": email, "in_degree": 0, "out_degree": 0, "total_degree": 0, "pagerank": 0.0, "is_enron": "enron" in email})
            people[email]["in_degree"] += cnt
            people[email]["total_degree"] += cnt

        if not people:
            return []

        max_deg = max((v["total_degree"] for v in people.values()), default=1) or 1
        for v in people.values():
            v["pagerank"] = float(v["total_degree"]) / max_deg

        return sorted(people.values(), key=lambda x: x["total_degree"], reverse=True)[:n]
    except Exception:
        return []


def get_top_people(n: int = 40):
    try:
        resp = es.search(
            index=ES_INDEX_VERTEX_METRICS, query={"match_all": {}}, size=n,
            sort=[{"pagerank": {"order": "desc"}}],
        )
        rows = [h["_source"] for h in resp["hits"]["hits"]]
        if rows:
            return rows
    except Exception:
        pass

    return _get_top_people_from_edges(n)


def get_top_edges(n: int = 80):
    try:
        resp = es.search(
            index=ES_INDEX_EDGES, query={"match_all": {}}, size=n,
            sort=[{"weight": {"order": "desc"}}],
        )
        return [h["_source"] for h in resp["hits"]["hits"]]
    except Exception:
        return []


def get_communities(n: int = 15):
    try:
        resp = es.search(
            index=ES_INDEX_COMMUNITIES, query={"match_all": {}}, size=n,
            sort=[{"member_count": {"order": "desc"}}],
        )
        return [h["_source"] for h in resp["hits"]["hits"]]
    except Exception:
        return []


def get_graph_stats():
    try:
        return es.get(index=ES_INDEX_STATS, id="graph")["_source"]
    except Exception:
        return {}


def _split_addr_text(v) -> list[str]:
    if not v:
        return []
    text = str(v)
    out: list[str] = []
    for p in text.replace(";", ",").split(","):
        x = p.strip().lower()
        if "@" in x:
            out.append(x)
    return out


def get_pair_email_insights(person_a: str, person_b: str, n: int = 200):
    """查询 A/B 双方往来邮件明细，并计算平均情感与 topic。"""
    a = (person_a or "").strip().lower()
    b = (person_b or "").strip().lower()
    if not a or not b or a == b:
        return {"emails": [], "avg_sentiment": 0.0, "topic_counts": {}, "total": 0}

    try:
        resp = es.search(
            index=ES_INDEX_SENTIMENT,
            size=n,
            sort=[
                {"ingested_at": {"order": "desc", "unmapped_type": "date"}},
                {"sent_at": {"order": "desc", "unmapped_type": "date"}},
            ],
            query={
                "bool": {
                    "filter": [
                        {"terms": {"sender": [a, b]}},
                    ]
                }
            },
            _source=["sender", "to", "cc", "bcc", "subject", "sentiment", "sentiment_score", "sent_at", "ingested_at"],
        )
    except Exception:
        return {"emails": [], "avg_sentiment": 0.0, "topic_counts": {}, "total": 0}

    rows = []
    signed_scores = []
    topic_counts: dict[str, int] = {}

    for h in resp.get("hits", {}).get("hits", []):
        m = h.get("_source", {})
        sender = str(m.get("sender") or "").strip().lower()
        recips = set(
            _split_addr_text(m.get("to"))
            + _split_addr_text(m.get("cc"))
            + _split_addr_text(m.get("bcc"))
        )

        is_a_to_b = sender == a and b in recips
        is_b_to_a = sender == b and a in recips
        if not (is_a_to_b or is_b_to_a):
            continue

        sub = (m.get("subject") or "(无主题)").strip()
        topic_counts[sub] = topic_counts.get(sub, 0) + 1

        sent_label = str(m.get("sentiment") or "").upper()
        raw_score = m.get("sentiment_score")
        score = float(raw_score) if raw_score is not None else 0.0
        signed = score if sent_label == "POSITIVE" else (-score if sent_label == "NEGATIVE" else 0.0)
        signed_scores.append(signed)

        rows.append(
            {
                "direction": f"{sender} → {person_b if sender == a else person_a}",
                "sender": sender,
                "subject": sub,
                "sentiment": sent_label or "N/A",
                "sentiment_score": score,
                "signed_sentiment": signed,
                "sent_at": m.get("sent_at"),
                "ingested_at": m.get("ingested_at"),
            }
        )

    avg_sent = sum(signed_scores) / len(signed_scores) if signed_scores else 0.0
    return {
        "emails": rows,
        "avg_sentiment": avg_sent,
        "topic_counts": dict(sorted(topic_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]),
        "total": len(rows),
    }


# ═══════════════════════════════════════════
#  关系图可视化（保持原有逻辑）
# ═══════════════════════════════════════════

def _extract_name_email(raw: str) -> tuple[str, str]:
    name, addr = parseaddr(str(raw or "").strip())
    return (name or "").strip(), (addr or "").strip().lower()


def _person_name_from_email(email: str, display_name: str | None = None) -> str:
    if display_name and str(display_name).strip():
        return str(display_name).strip()
    local = (email or "").split("@")[0]
    local = local.replace(".", " ").replace("_", " ").replace("-", " ")
    local = " ".join(local.split())
    return local.title() if local else "Unknown"


def _name_from_x_folder(x_folder: str) -> str | None:
    """从 x_folder 中抽取类似 'Allen, Phillip K.' 的姓名并转为 'Phillip K. Allen'。"""
    if not x_folder:
        return None
    parts = [p.strip(" '") for p in str(x_folder).replace("\\", "/").split("/") if p.strip(" '")]
    candidate = None
    for p in parts:
        if "," in p and "mail" not in p.lower():
            candidate = p
    if not candidate:
        return None

    if "," in candidate:
        last, first = [x.strip() for x in candidate.split(",", 1)]
        name = f"{first} {last}".strip()
    else:
        name = candidate.strip()

    return " ".join(name.split()) or None


def _lookup_sender_names_from_x_folder(emails: list[str]) -> dict[str, str]:
    """按 sender 邮箱查样本邮件，提取 x_folder 里的姓名。"""
    email_list = sorted({str(e).strip().lower() for e in emails if e})
    if not email_list:
        return {}

    try:
        resp = es.search(
            index=ES_INDEX_SENTIMENT,
            size=min(500, len(email_list) * 3),
            query={"terms": {"sender": email_list}},
            sort=[{"ingested_at": {"order": "desc", "unmapped_type": "date"}}],
            _source=["sender", "x_folder"],
        )
    except Exception:
        return {}

    out: dict[str, str] = {}
    for h in resp.get("hits", {}).get("hits", []):
        src = h.get("_source", {})
        sender = str(src.get("sender") or "").strip().lower()
        if not sender or sender in out:
            continue
        name = _name_from_x_folder(str(src.get("x_folder") or ""))
        if name:
            out[sender] = name
    return out


def _build_email_name_map(top_people: list[dict], extra_emails: list[str] | None = None) -> dict[str, str]:
    """构建 email -> display_name 映射（优先真实姓名，含 x_folder 兜底）。"""
    people_email_map: dict[str, dict] = {}
    for p in top_people:
        raw_email = str(p.get("email") or "").strip()
        if not raw_email:
            continue
        _, email_from_header = _extract_name_email(raw_email)
        email = (email_from_header or raw_email).strip().lower()
        if email:
            people_email_map[email] = p

    extra_email_list = []
    for e in (extra_emails or []):
        _, parsed = _extract_name_email(str(e or ""))
        em = parsed.strip().lower() if parsed else str(e or "").strip().lower()
        if em:
            extra_email_list.append(em)

    all_emails = sorted(set(list(people_email_map.keys()) + extra_email_list))
    xfolder_name_map = _lookup_sender_names_from_x_folder(all_emails)

    out: dict[str, str] = {}
    for email in all_emails:
        p = people_email_map.get(email, {})
        name_from_email_header, _ = _extract_name_email(email)
        preferred_name = (
            p.get("name")
            or p.get("person_name")
            or p.get("display_name")
            or p.get("sender_name")
            or xfolder_name_map.get(email)
            or name_from_email_header
        )
        out[email] = _person_name_from_email(email, str(preferred_name) if preferred_name else None)

    return out


def _build_people_options(top_people: list[dict]) -> tuple[list[str], dict[str, str]]:
    """返回 label 列表与 label->email 映射，优先使用真实姓名显示。"""
    labels: list[str] = []
    label_to_email: dict[str, str] = {}
    email_name_map = _build_email_name_map(top_people)

    for email, display in sorted(email_name_map.items()):
        label = f"{display} <{email}>"
        if label in label_to_email:
            label = f"{label} ({len(labels)+1})"

        labels.append(label)
        label_to_email[label] = email

    labels = sorted(labels)
    return labels, label_to_email


def build_network_html(top_people, top_edges, max_nodes: int = 40, email_name_map: dict[str, str] | None = None):
    net = Network(height="600px", width="100%", bgcolor="#f7faff", font_color="#1f2937")

    people = {p["email"]: p for p in top_people[:max_nodes]}
    node_set = set(people.keys())
    for e in top_edges[:200]:
        node_set.add(e["src"])
        node_set.add(e["dst"])
    node_set = list(node_set)[:max_nodes]

    max_pr = max((people.get(e, {}).get("pagerank", 0.001) for e in node_set), default=0.001) or 0.001

    resolved_name_map = email_name_map or _build_email_name_map(top_people, extra_emails=node_set)

    for email in node_set:
        info = people.get(email, {})
        pr = info.get("pagerank", 0.001)
        size = 10 + (pr / max_pr) * 40
        color = "#00d4ff" if info.get("is_enron", True) else "#ff6b6b"
        person_name = resolved_name_map.get(str(email).lower(), _person_name_from_email(email))
        label = person_name
        title = f"{person_name}\n{email}\nPR: {pr:.4f} | 度: {info.get('total_degree', 0)}"
        net.add_node(email, label=label, size=size, color=color, title=title)

    max_w = max((e.get("weight", 1) for e in top_edges), default=1) or 1
    for e in top_edges:
        if e["src"] in node_set and e["dst"] in node_set:
            w = e.get("weight", 1)
            avg_sent = float(e.get("avg_sentiment", 0.0) or 0.0)
            net.add_edge(
                e["src"],
                e["dst"],
                value=w,
                width=1 + (w / max_w) * 8,
                title=f"邮件数: {w} | 平均情感: {avg_sent:.3f}",
            )

    net.set_options("""{
        "physics":{"forceAtlas2Based":{"gravitationalConstant":-80,"centralGravity":0.01,"springLength":150},"solver":"forceAtlas2Based"},
        "edges":{"color":{"opacity":0.4},"smooth":{"type":"continuous"}},
        "interaction":{"hover":true,"tooltipDelay":100}
    }""")
    return net.generate_html()


# ═══════════════════════════════════════════
#  主页面
# ═══════════════════════════════════════════

def main() -> None:
    st.set_page_config(page_title="Enron 实时分析", layout="wide")

    st.markdown("""<style>
        :root {
            --bg: #f6f8fc;
            --panel: #ffffff;
            --panel-2: #f9fbff;
            --text: #1f2937;
            --muted: #6b7280;
            --accent: #3b82f6;
            --accent-2: #60a5fa;
            --border: #dbe3ef;
        }

        .stApp {
            background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
            color: var(--text);
        }

        h1, h2, h3, p, label, span, div {
            color: var(--text);
        }

        section[data-testid="stSidebar"] {
            background: #f3f6fb;
            border-right: 1px solid var(--border);
        }

        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, var(--panel) 0%, var(--panel-2) 100%);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 12px 14px;
            box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
        }

        .stMetricLabel { color: var(--muted) !important; }
        .stMetricValue { color: var(--text) !important; }

        div[data-baseweb="select"] > div {
            background: var(--panel);
            border-color: var(--border);
            border-radius: 10px;
        }

        .stTabs [data-baseweb="tab-list"] { gap: 8px; }

        .stTabs [data-baseweb="tab"] {
            background: #f2f6fd;
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 8px 14px;
        }

        .stTabs [aria-selected="true"] {
            background: #eaf2ff;
            border-color: #bcd2f5;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }

        .stProgress > div > div > div {
            background: linear-gradient(90deg, var(--accent) 0%, var(--accent-2) 100%);
        }
    </style>""", unsafe_allow_html=True)

    st.title("Enron 邮件实时分析看板")

    refresh = st.sidebar.slider("自动刷新间隔（秒）", 5, 120, 10)

    # ── 计算处理速率（基于上一次刷新） ──
    now = time.time()
    if "prev_time" not in st.session_state:
        st.session_state["prev_time"] = now
        st.session_state["prev_sent_count"] = 0

    sent_count  = safe_count(ES_INDEX_SENTIMENT)
    edge_count  = safe_count(ES_INDEX_EDGES)
    vertex_count = safe_count(ES_INDEX_VERTEX_METRICS)

    if "prev_edge_count" not in st.session_state:
        st.session_state["prev_edge_count"] = edge_count

    dt = now - st.session_state["prev_time"]
    rate = (sent_count - st.session_state["prev_sent_count"]) / dt if dt > 0 else 0.0
    new_edges_in_refresh = max(0, edge_count - st.session_state["prev_edge_count"])

    st.session_state["prev_time"] = now
    st.session_state["prev_sent_count"] = sent_count
    st.session_state["prev_edge_count"] = edge_count

    # ── Kafka topic 总量（进度条分母） ──
    if "kafka_total" not in st.session_state or st.session_state["kafka_total"] is None:
        st.session_state["kafka_total"] = _get_kafka_topic_total()
    total_emails = st.session_state["kafka_total"] or sent_count or 518395

    # ── 系统状态灯 ──
    services = _get_services()
    st.markdown("#### 系统状态")
    svc_cols = st.columns(len(services))
    for col, (name, status) in zip(svc_cols, services.items()):
        with col:
            st.metric(name, _status_badge(status))

    st.divider()

    # ── 进度条 + 核心指标 ──
    progress = min(sent_count / total_emails, 1.0) if total_emails else 0

    if sent_count >= total_emails and sent_count > 0:
        eta_str = "已完成"
    elif rate > 0:
        eta_str = _format_eta((total_emails - sent_count) / rate)
    else:
        eta_str = "等待启动…"

    m1, m2, m3, m4 = st.columns(4)
    new_in_refresh = max(0, sent_count - st.session_state.get("prev_sent_count_display", sent_count))
    st.session_state["prev_sent_count_display"] = sent_count

    m1.metric(
        "已分析邮件",
        f"{sent_count:,}",
        f"本轮 +{new_in_refresh} 封 | {rate:.1f} 封/秒" if rate > 0 or new_in_refresh > 0 else "",
    )
    m2.metric(
        "关系边数",
        f"{edge_count:,}",
        f"本轮 +{new_edges_in_refresh} 条" if new_edges_in_refresh > 0 else "",
    )
    m3.metric("人物节点", f"{vertex_count:,}")
    m4.metric("预计剩余", eta_str)

    # 进度条
    st.progress(progress)
    st.caption(f"情感分析进度：{sent_count:,} / {total_emails:,}（{progress * 100:.1f}%）")

    # ── 图计算状态 ──
    gs = get_graph_stats()
    last_computed = gs.get("last_computed_at", "尚未计算")[:19]
    st.caption(f"关系图谱最后计算时间：{last_computed}")

    top_people_cache = get_top_people(100)
    top_edges_cache = get_top_edges(120)
    email_name_map = _build_email_name_map(
        top_people_cache,
        extra_emails=[e.get("src") for e in top_edges_cache] + [e.get("dst") for e in top_edges_cache],
    )

    st.divider()

    # ── Tab ──
    t1, t2, t3 = st.tabs(["情感分析", "关系图谱", "详细数据"])

    # == 情感 ==
    with t1:
        cl, cr = st.columns([1, 2])
        with cl:
            st.subheader("情感分布")
            sd = get_sentiment_dist()
            if sd:
                colors = {
                    "POSITIVE": "#86efac",
                    "NEUTRAL": "#fde68a",
                    "NEGATIVE": "#fca5a5",
                    "positive": "#86efac",
                    "neutral": "#fde68a",
                    "negative": "#fca5a5",
                }
                fig = go.Figure(data=[go.Pie(
                    labels=[k.capitalize() for k in sd],
                    values=list(sd.values()),
                    marker_colors=[colors.get(k.lower(), "#cbd5e1") for k in sd],
                    hole=0.45,
                    textinfo="label+percent",
                )])
                fig.update_layout(
                    paper_bgcolor="#f7faff",
                    plot_bgcolor="#f7faff",
                    font=dict(color="#334155"),
                    margin=dict(t=10, b=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据，等待情感分析写入…")
        with cr:
            st.subheader("最新邮件")
            for mail in get_recent_emails():
                sub = (mail.get("subject") or "(无主题)")[:50]
                sender_email = str(mail.get("sender") or "").strip().lower()
                who = email_name_map.get(sender_email, _person_name_from_email(sender_email))
                sc_raw = mail.get("sentiment_score")
                sc = float(sc_raw) if sc_raw is not None else 0.0
                sent_raw = mail.get("sent_at") or ""
                ingested_raw = mail.get("ingested_at") or ""
                sent_ts = str(sent_raw)[:16] if sent_raw else "N/A"
                ingested_ts = str(ingested_raw)[:19].replace("T", " ") if ingested_raw else "N/A"
                st.markdown(f"**{sub}** — {who}  `{sc:.2f}`  `[邮件时间:{sent_ts} | 入库时间:{ingested_ts}]`")

    # == 图谱 ==
    with t2:
        cl2, cr2 = st.columns([2, 1])
        with cl2:
            st.subheader("关系网络图")
            tp = top_people_cache[:40]
            te = top_edges_cache[:80]
            # 边来自流式 upsert；顶点指标来自周期性 GraphFrames。仅要求有边即可画图（无 PR 时用默认值）
            if te:
                components.html(build_network_html(tp, te, email_name_map=email_name_map), height=650)
                st.caption(f"当前可视化关系边：{len(te)}（按权重 Top）")
            else:
                st.info(
                    "暂无图数据：请确认已启动 build_email_relationship_graph.py。"
                    "首次建议用 --starting-offsets earliest，并使用新的 --checkpoint-dir 进行回放。"
                )
        with cr2:
            st.subheader("Top 关键人物")
            for p in top_people_cache[:15]:
                email = str(p.get("email", "")).strip().lower()
                name = email_name_map.get(email, _person_name_from_email(email))
                pr = float(p.get("pagerank", 0.0) or 0.0)
                degree = int(p.get("total_degree", 0) or 0)
                st.markdown(f"**{name}**  `PR:{pr:.4f}`  `度:{degree}`  `{email}`")
            st.subheader("社区概况")
            for c in get_communities(10):
                scope = "内部" if c.get("all_enron") else "混合"
                community_id = c.get("community_id", "N/A")
                member_count = int(c.get("member_count", 0) or 0)
                st.markdown(f"社区 {community_id}（{scope}）: {member_count} 人")

    # == 详情 ==
    with t3:
        st.subheader("Top 通信关系")
        te2 = top_edges_cache[:30]
        if te2:
            df = pd.DataFrame(te2)
            df["from"] = df["src"].apply(lambda x: email_name_map.get(str(x).strip().lower(), _person_name_from_email(str(x))))
            df["to"] = df["dst"].apply(lambda x: email_name_map.get(str(x).strip().lower(), _person_name_from_email(str(x))))
            if "avg_sentiment" not in df.columns:
                df["avg_sentiment"] = 0.0
            st.dataframe(df[["from", "to", "weight", "avg_sentiment", "sample_subject", "src", "dst"]],
                         use_container_width=True, hide_index=True)

            st.subheader("关系情感概览")
            avg_all = float(pd.to_numeric(df["avg_sentiment"], errors="coerce").fillna(0).mean()) if not df.empty else 0.0
            c1, c2 = st.columns(2)
            c1.metric("Top关系平均情感", f"{avg_all:.3f}")
            c2.metric("Top关系总邮件数", f"{int(pd.to_numeric(df['weight'], errors='coerce').fillna(0).sum()):,}")

        st.subheader("人物指标")
        ap = top_people_cache
        if ap:
            df2 = pd.DataFrame(ap)
            if "email" not in df2.columns:
                st.warning("人物指标缺少 email 字段，暂无法展示详情。")
            else:
                df2["name"] = df2["email"].apply(lambda x: email_name_map.get(str(x).strip().lower(), _person_name_from_email(str(x))))
                preferred_cols = [
                    "name", "email", "pagerank", "total_degree", "in_degree", "out_degree", "community_id"
                ]
                available_cols = [c for c in preferred_cols if c in df2.columns]
                st.dataframe(df2[available_cols], use_container_width=True, hide_index=True)

        st.subheader("A/B 双人关系分析")
        candidate_labels, label_to_email = _build_people_options(ap)
        if len(candidate_labels) < 2:
            st.info("候选人物不足（至少 2 人），请等待更多图谱数据。")
        else:
            c1, c2 = st.columns(2)
            label_a = c1.selectbox("选择人物 A", options=candidate_labels, index=0)
            b_default_idx = 1 if len(candidate_labels) > 1 else 0
            label_b = c2.selectbox("选择人物 B", options=candidate_labels, index=b_default_idx)

            person_a = label_to_email.get(label_a, "")
            person_b = label_to_email.get(label_b, "")

            if person_a == person_b:
                st.warning("A 和 B 不能是同一个人。")
            else:
                pair = get_pair_email_insights(person_a, person_b, n=300)
                total_pair = int(pair.get("total", 0) or 0)
                avg_pair_sent = float(pair.get("avg_sentiment", 0.0) or 0.0)

                m_a, m_b = st.columns(2)
                m_a.metric("A/B 往来邮件数", f"{total_pair:,}")
                m_b.metric("A/B 平均情感(有符号)", f"{avg_pair_sent:.3f}")

                topics = pair.get("topic_counts", {}) or {}
                if topics:
                    topic_df = pd.DataFrame(
                        [{"topic": k, "count": v} for k, v in topics.items()]
                    )
                    st.markdown("**往来 Topic（按出现次数 Top 10）**")
                    st.dataframe(topic_df, use_container_width=True, hide_index=True)

                emails = pair.get("emails", []) or []
                if emails:
                    emails_df = pd.DataFrame(emails)
                    if "sender" in emails_df.columns:
                        emails_df["sender_name"] = emails_df["sender"].apply(
                            lambda x: email_name_map.get(str(x).strip().lower(), _person_name_from_email(str(x)))
                        )
                    show_cols = [
                        c for c in ["direction", "sender_name", "subject", "sentiment", "sentiment_score", "signed_sentiment", "sent_at", "ingested_at"]
                        if c in emails_df.columns
                    ]
                    st.markdown("**往来邮件明细（最近 300 条内匹配）**")
                    st.dataframe(emails_df[show_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("当前查询范围内未找到 A/B 双向或单向往来邮件。")

    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()

# 5003 Email Relationship Spark

这是一个基于 **Kafka + Spark Structured Streaming + Elasticsearch + Streamlit** 的 Enron 邮件实时分析项目。

项目目标是把邮件数据从 Kafka 流式读入，完成情感分析、关系边构建、图指标计算，并在 Streamlit 看板中展示结果（目前来说）。

---

## 1. 你可以用它做什么

这个项目主要支持以下能力：

- 从 Kafka 实时消费 Enron 邮件数据
- 对邮件正文做情感分析（DistilBERT）
- 把邮件关系转换成图结构中的边 `src -> dst`
- 累积邮件关系权重、平均情感等指标
- 用 GraphFrames 计算顶点度数、PageRank、社区信息
- 用 Streamlit 看板展示：
  - 情感分布
  - 最新邮件
  - 关系图谱
  - Top 人物
  - 社区概况
  - A/B 双人关系分析

---

## 2. 项目结构总览

```text
.
├── data_preparation/
│   ├── README.md
│   └── scripts/
│       ├── produce_emails_to_kafka.py
│       ├── email_record_utils.py
│       ├── kaggle.py
│       └── ...
├── spark_apps/
│   ├── enron_common.py
│   ├── stream_enron_sentiment.py
│   ├── build_email_relationship_graph.py
│   ├── streamlit_app.py
│   ├── run.sh
│   ├── .env.external
│   ├── logs/
│   └── checkpoints/
├── 2-spark-streaming.ipynb
├── requirements.txt
└── README.md
```

### 2.1 项目workflow

```text
Enron 邮件数据
      │
      ▼
数据准备脚本（produce_emails_to_kafka.py）
      │
      ▼
Kafka topic：raw_emails_topic
      │
      ├─────────────────────────────────────────────────────────────┐
      │                                                             │
      ▼                                                             ▼
stream_enron_sentiment.py                                 build_email_relationship_graph.py
      │                                                             │
      │                                                             │
      ├── 情感分析（DistilBERT）                                    ├── 读取关系边
      ├── 写入 enron_emails                                         ├── GraphFrames 计算
      └── 聚合关系边到 enron_edges                                   └── 写回 vertex/community/stats
      │                                                             │
      └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                           streamlit_app.py
                                    │
                                    ▼
                            Streamlit 看板展示
```
你可以把项目理解成三层：

1. **数据层**：Kafka 里装原始邮件（data preparation负责的，根据周轩的readme进行）
2. **计算层**：Spark 做情感分析和图谱计算 （spark apps执行的，直接运行.sh就可以）
3. **展示层**：Streamlit 从 ES 读取数据并展示

---

## 3. `spark_apps/` 目录中各主要文件的功能

### 3.1 `spark_apps/enron_common.py`

这是 **公共配置与工具文件**，相当于整个 `spark_apps` 的“配置中心”。

它主要负责：

- 定义邮件 Schema
- 定义 Kafka 的默认连接参数
- 定义 Elasticsearch 的默认连接参数
- 提供本地 / 外部环境切换机制
- 提供 Spark Session 创建函数
- 提供 Kafka 读取时的解析函数
- 提供 ES 客户端与常用工具函数

#### 这个文件里最重要的内容

- `ENRON_PROFILE`
  - `local`：默认使用本地 Kafka / 本地 Elasticsearch
  - `external`：使用 `.env.external` 或你自己导出的外部 Kafka / ES 环境变量

- `KAFKA_TOPIC`
  - 默认已经统一为：`raw_emails_topic`

- `KAFKA_EMAIL_JSON_SCHEMA`
  - 定义了 Kafka 消息里的 JSON 结构
  - 这个结构必须和 `produce_emails_to_kafka.py` 写出去的数据一致

- `kafka_stream_to_email_df()`
  - 把 Kafka 的 `value` 转成结构化邮件 DataFrame
  - 这是 Spark 流作业读取邮件的标准入口

- `get_kafka_options()`
  - 根据环境变量生成 Kafka 连接参数
  - 兼容本地 Kafka 和外部 Kafka（SASL/SSL）

- `get_es_client()`
  - 根据环境变量生成 Elasticsearch 客户端

#### 你什么时候需要看这个文件

当你遇到以下问题时，先看这里：

- Kafka 连不上
- ES 连不上
- 想切换本地 / 外部环境
- 想确认 Kafka 消息字段格式
- 想知道 Spark 读取数据时到底怎么解析的

---

### 3.2 `spark_apps/stream_enron_sentiment.py`

这是 **邮件情感分析流作业**。

它的职责是：

1. 从 Kafka 读入邮件
2. 解析邮件 JSON
3. 对 `body` 做情感分析
4. 把结果写入 Elasticsearch
5. 同时把邮件关系边更新到 `enron_edges`

#### 它的处理流程

- Kafka `readStream`
- `from_json` 解析邮件结构
- 统一处理 `sent_at`
- 通过 DistilBERT 计算情感
- 写入情感索引 `enron_emails`
- 计算并 upsert 关系边到 `enron_edges`

#### 它写到 ES 里的数据

- 邮件正文情感结果：
  - `sentiment`
  - `sentiment_score`
  - `clean_body`
  - `ingested_at`
  - `sender`
  - `to`
  - `cc`
  - `bcc`
  - `subject`
  - `sent_at`

- 关系边：
  - `src`
  - `dst`
  - `weight`
  - `first_contact_at`
  - `last_contact_at`
  - `sample_subject`
  - `avg_sentiment`
  - `sentiment_sum`

#### 你什么时候运行它

当你只关心：

- 情感分析是否正常
- Elasticsearch 是否写入邮件结果
- 关系边是否在累积

---

### 3.3 `spark_apps/build_email_relationship_graph.py`

这是 **关系图谱计算流作业**。

它的职责是：

1. 从 ES 读取已经积累好的边数据
2. 构建 GraphFrame 图
3. 计算图指标
4. 把结果写回 ES

#### 它会计算什么

- PageRank
- 入度 `in_degree`
- 出度 `out_degree`
- 总度数 `total_degree`
- 连通分量 / 社区 `community_id`
- 顶点是否为 Enron 邮箱 `is_enron`

#### 它写到 ES 里的索引

- `enron_vertex_metrics`
- `enron_communities`
- `enron_stream_stats`

#### 你什么时候运行它

当你想在看板上看到：

- Top 关键人物
- 社区概况
- 图谱计算时间
- 更完整的网络分析

就需要运行它。

> 注意：如果你只跑情感分析，不跑这个脚本，那么看板里会显示“关系图谱最后计算时间：尚未计算”。

---

### 3.4 `spark_apps/streamlit_app.py`

这是 **Streamlit 前端看板**。

它负责把 ES 中已有的数据可视化展示出来。

#### 看板展示内容

- 系统状态
- Kafka / ES / Spark 运行状态
- 邮件分析进度条
- 情感分布饼图
- 最新邮件列表
- 关系网络图
- Top 关键人物
- 社区概况
- A/B 双人关系分析
- Top 通信关系详情
- 人物指标表

#### 它读取的数据来源

- `enron_emails`：情感分析结果
- `enron_edges`：关系边
- `enron_vertex_metrics`：人物图指标
- `enron_communities`：社区信息
- `enron_stream_stats`：图谱状态

#### 你什么时候运行它

只要你想“看结果”，就运行它。

它不负责计算，只负责展示。

---

### 3.5 `spark_apps/run.sh`

这是 **一键启动脚本**。

#### 它支持两种环境

- `local`
  - 本地 Kafka
  - 本地 Elasticsearch

- `external`
  - 使用 `.env.external`
  - 对齐 `2-spark-streaming.ipynb` 中的 Kafka / ES

#### 它支持的启动方式

```bash
./run.sh auto local
./run.sh auto external
./run.sh normal local
./run.sh bootstrap external
```

#### 参数怎么理解

- `auto`
  - 自动判断模式
  - 如果检测到边数据为空，会倾向于 bootstrap 回放

- `normal`
  - 只跑增量实时数据
  - 适合已有历史数据、只想继续往后消费

- `bootstrap`
  - 从头回放
  - 适合第一次跑、或者你刚清空了 checkpoint / ES 索引

#### 日志会写到哪里

- `spark_apps/logs/sentiment.log`
- `spark_apps/logs/graph.log`
- `spark_apps/logs/streamlit.log`

---

### 3.6 `spark_apps/.env.external`

这是 **外部环境变量模板文件**。

它主要用于：

- 保存 Kafka 的外部连接信息
- 保存 Elasticsearch 的外部连接信息
- 设置 `ENRON_PROFILE=external`
- 让你通过一条命令加载外部配置

#### 推荐启动方式

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

> 注意：里面的 key / secret 需要你自己替换成真实值。

---

## 4. 从零上手：最详细的运行指南

这一节是给第一次接触这个项目的人看的。如果你是新手，建议按顺序做，不要跳步。

### 4.1 先确认你本机环境

建议你先确认以下条件：

- Python 已安装
- 你有一个可用的 Spark/PySpark 环境
- Kafka 已准备好，或者你知道外部 Kafka 的连接信息
- Elasticsearch 已准备好，或者你知道外部 ES 的连接信息

如果你不确定当前 Python 版本，可以先运行：

```bash
python3 --version
```

这个项目推荐：

- Python 3.9(我的requirement里的具体版本是建立与3.9的基础上的)
- 但如果你已经有可用环境，也可以先试

### 4.2 安装依赖

在项目根目录执行：

```bash
python3 -m pip install -r requirements.txt
```

如果你的python版本和我的不一样，请查询更改txt中的具体包版本

### 4.3 确认 Kafka 和 Elasticsearch 可访问

#### 本地模式

如果你打算跑本地模式，默认会连接：

- Kafka：`localhost:9092`
- Elasticsearch：`http://localhost:9200`

#### 外部模式

如果你打算跑 notebook 对应的外部环境，要先准备好：

- Kafka bootstrap servers
- Kafka API key / secret
- Elasticsearch host
- Elasticsearch API key

然后通过 `.env.external` 一次性加载。

### 4.4 先确认 Kafka 里有数据

这个项目读取的是 Kafka topic：

- `raw_emails_topic`

如果这个 topic 里没有邮件数据，那么 Spark 作业会启动，但不会有结果可看。

### 4.5 最推荐的一键启动方式

#### 本地环境

```bash
cd spark_apps
./run.sh auto local
```

#### 外部环境（这个是连小包的数据库，用这个）

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

### 4.6 想从 0 重置并重新跑

如果你希望进度清零（去除所有的已经分析的内容），通常要做三件事：

1. 删除 ES 索引
2. 删除 checkpoint
3. 重新启动

例如：

```bash
curl -X DELETE "http://localhost:9200/enron_emails"
rm -rf spark_apps/checkpoints/enron_kafka_es
rm -rf spark_apps/checkpoints/enron_kafka_es_sentiment
rm -rf spark_apps/checkpoints/graph_stream
```

然后重新启动。

---

## 5. 数据流是怎么走的

整体链路如下：

```text
数据准备脚本
   ↓
Kafka topic: raw_emails_topic
   ↓
stream_enron_sentiment.py
   ├─ 情感分析 → enron_emails
   └─ 关系边聚合 → enron_edges
   ↓
build_email_relationship_graph.py
   ├─ 读取 enron_edges
   ├─ 计算 PageRank / 度 / 社区
   └─ 写回 ES
   ↓
streamlit_app.py
   └─ 读取 ES 并展示看板
```
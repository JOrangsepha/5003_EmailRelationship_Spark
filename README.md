# 5003 Email Relationship Spark 项目说明文档

本项目是一个基于 **Kafka + Spark Structured Streaming + Elasticsearch + GraphFrames + Streamlit / Kibana** 的 Enron 邮件实时分析系统。它的核心目标是：先从 Kaggle 的 Enron 邮件数据集中读取邮件，再把邮件转换成结构化 JSON 消息写入 Kafka；随后由 Spark 流式消费 Kafka 数据，完成邮件情感分析、通信关系边构建、图指标计算，并最终通过 Streamlit 看板或 Kibana 进行可视化展示。

简单来说，这个项目不是单纯地“读取邮件并展示”，而是完整模拟了一条实时数据分析链路：

```text
Kaggle Enron 数据集
      ↓
DataFrame 数据读取与邮件解析
      ↓
Kafka Producer 写入 raw_emails_topic
      ↓
Spark Structured Streaming 实时消费
      ↓
情感分析 + 通信关系边聚合
      ↓
Elasticsearch 存储分析结果
      ↓
GraphFrames 计算人物关系图指标
      ↓
Streamlit / Kibana 可视化展示
```

其中，**Streamlit** 更像是项目自带的综合前端看板，适合直接展示分析结果；**Kibana** 则是直接基于 Elasticsearch 索引做可视化分析，适合在 ES 数据已经写入后进一步探索数据。两者的数据来源相同，但展示方式和使用门槛不同。

---

## 1. 项目可以完成什么

本项目主要支持以下功能：

- 从 Kaggle 加载 Enron 邮件数据集；
- 将原始邮件解析为结构化 JSON 记录；
- 使用 Kafka 模拟实时邮件数据流；
- 使用 Spark Structured Streaming 实时消费 Kafka 邮件消息；
- 使用 DistilBERT 对邮件正文进行情感分析；
- 从邮件的 `sender`、`to`、`cc`、`bcc` 字段中构建通信关系边；
- 将邮件情感结果、通信关系边、人物图指标等写入 Elasticsearch；
- 使用 GraphFrames 计算 PageRank、入度、出度、社区等图指标；
- 使用 Streamlit 展示完整分析看板；
- 或者使用 Kibana 直接基于 Elasticsearch 索引做可视化探索。

你可以把它理解为一个完整的实时邮件关系分析系统：**Kafka 负责传输数据，Spark 负责实时计算，Elasticsearch 负责存储结果，Streamlit / Kibana 负责展示结果。**

---

## 2. 项目整体结构

项目主要分为两个部分：

1. `data_preparation/`：负责从 Kaggle 读取数据、解析邮件、写入 Kafka；
2. `spark_apps/`：负责从 Kafka 读取数据、执行 Spark 分析任务、写入 ES，并启动可视化看板。

推荐的目录结构如下：

```text
.
├── data_preparation/
│   ├── README.md
│   └── scripts/
│       ├── kaggle.py
│       ├── email_record_utils.py
│       ├── produce_emails_to_kafka.py
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

---

## 3. 整体 Workflow

完整数据流如下：

```text
Enron 邮件数据集
      │
      ▼
Kaggle DataFrame
      │
      ▼
email_record_utils.py 解析邮件字段
      │
      ▼
produce_emails_to_kafka.py
      │
      ▼
Kafka topic: raw_emails_topic
      │
      ├──────────────────────────────────────────────┐
      │                                              │
      ▼                                              ▼
stream_enron_sentiment.py                 build_email_relationship_graph.py
      │                                              │
      ├── 邮件正文情感分析                            ├── 读取 enron_edges
      ├── 写入 enron_emails                          ├── 构建 GraphFrame
      └── 聚合通信关系边到 enron_edges                └── 写回图指标与社区结果
      │                                              │
      └──────────────────────────────────────────────┘
                         │
                         ▼
                 Elasticsearch
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
   Streamlit 看板展示              Kibana 可视化分析
```

从工程角度看，项目可以分成三层：

1. **数据层**：从 Kaggle 读取 Enron 邮件数据，并写入 Kafka；
2. **计算层**：Spark 从 Kafka 实时读取数据，执行情感分析和图计算；
3. **展示层**：Streamlit 或 Kibana 从 Elasticsearch 读取结果并展示。

---

## 4. 数据准备部分：从 Kaggle 到 Kafka

这一部分负责把 Kaggle 上的 Enron 邮件数据转换成 Kafka 中的实时消息流。

正式流程是：

```text
Kaggle DataFrame -> 结构化邮件记录 -> Kafka topic: raw_emails_topic
```

也就是说，项目不会直接让 Spark 读取 Kaggle 文件，而是先通过 Producer 把邮件数据写进 Kafka。这样后面的 Spark 程序就可以按照“实时流数据”的方式消费邮件。

### 4.1 核心脚本

#### `scripts/kaggle.py`

该脚本负责使用 `kagglehub` 从 Kaggle 数据集 `wcukierski/enron-email-dataset` 读取 `emails.csv`，并返回一个 `pandas DataFrame`。

它的作用可以理解为：**把 Kaggle 数据集读进 Python。**

如果只是想检查 Kaggle 数据是否能正常读取，可以单独运行：

```bash
python3 scripts/kaggle.py
```

该命令通常会打印前 5 行数据，不会向 Kafka 写入任何消息。

#### `scripts/email_record_utils.py`

该脚本负责把 DataFrame 中的每一行邮件解析成结构化记录。

原始 Enron 邮件通常包含大量非结构化文本，例如发件人、收件人、主题、时间、正文等信息可能混在原始 message 字段里。这个脚本的作用就是把这些内容拆解成统一字段，例如：

- `file_path`
- `message_id`
- `sent_at`
- `sender`
- `to`
- `cc`
- `bcc`
- `subject`
- `x_folder`
- `x_origin`
- `x_filename`
- `body`
- `body_length`

其中，`sent_at` 会被统一转换为 UTC 字符串，例如：

```text
2001-05-14 23:39:00 UTC
```

#### `scripts/produce_emails_to_kafka.py`

这是 Kafka Producer 主脚本。它会调用 `load_dataframe()` 读取 Kaggle DataFrame，再调用邮件解析工具把每行数据转换成结构化 JSON，最后把这些 JSON 消息逐条发送到 Kafka topic。

它的作用可以理解为：**把已经解析好的邮件模拟成实时数据流。**

---

### 4.2 Kafka 消息格式

每条 Kafka 消息由 `key` 和 `value` 两部分组成：

- `key`：邮件文件路径，例如 `allen-p/_sent_mail/1.`；
- `value`：一条结构化 JSON 邮件记录。

`value` 中包含的主要字段如下：

```json
{
  "file_path": "allen-p/_sent_mail/1.",
  "message_id": "...",
  "sent_at": "2001-05-14 23:39:00 UTC",
  "sender": "...",
  "to": ["..."],
  "cc": ["..."],
  "bcc": ["..."],
  "subject": "...",
  "x_folder": "...",
  "x_origin": "...",
  "x_filename": "...",
  "body": "...",
  "body_length": 1234
}
```

后续 Spark 脚本中的 `KAFKA_EMAIL_JSON_SCHEMA` 必须和这里的 JSON 字段保持一致，否则 Spark 在解析 Kafka 消息时可能会出现字段为空或解析失败的问题。

---

### 4.3 启动 Kafka Producer

运行前需要先保证你已经有一个可连接的 Kafka broker，例如：

- 本地 Kafka：`localhost:9092`；
- 或远程 Kafka 集群地址。

建议统一写入以下 topic：

```text
raw_emails_topic
```

因为 `spark_apps/enron_common.py` 中默认读取的 Kafka topic 已经统一为 `raw_emails_topic`。如果 Producer 写入了其他 topic，而 Spark 仍然读取 `raw_emails_topic`，那么 Spark 作业虽然能启动，但不会读到任何邮件数据。

启动 Producer：

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic
```

如果只是想做小规模测试，可以限制发送数量：

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic \
  --max-records 1000 \
  --sleep-seconds 0.01
```

这里的参数含义是：

- `--bootstrap-servers`：Kafka broker 地址；
- `--topic`：要写入的 Kafka topic；
- `--max-records`：最多发送多少条邮件；
- `--sleep-seconds`：每条消息之间暂停多久，用来模拟流式输入。

---

### 4.4 无 Kafka broker 时的替代方案

如果暂时还没有 Kafka broker，可以先把邮件解析结果导出成 JSON 批文件，方便检查解析逻辑是否正确。

导出全部数据：

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails \
  --records-per-file 5000
```

只导出一小部分测试数据：

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails_test \
  --records-per-file 5000 \
  --max-records 10000
```

生成结果类似：

```text
batch_000000.json
batch_000001.json
batch_000002.json
```

每个文件中是一个 JSON 数组，数组里的每个元素就是一条解析后的邮件记录。

需要注意的是：这种方式只能用于检查数据解析结果，不能替代正式的 Kafka 流式流程。如果要运行 Spark Streaming 主流程，仍然需要 Kafka topic 中有数据。

---

## 5. Spark Apps 部分：从 Kafka 到 Elasticsearch

`spark_apps/` 是项目的计算和展示核心。它主要负责三件事：

1. 从 Kafka 读取结构化邮件；
2. 用 Spark 进行情感分析、通信关系构建和图指标计算；
3. 把计算结果写入 Elasticsearch，并通过 Streamlit 展示。

---

### 5.1 `spark_apps/enron_common.py`

这是 `spark_apps/` 的公共配置与工具文件，可以理解为整个 Spark 分析部分的“配置中心”。

它主要负责：

- 定义邮件 JSON Schema；
- 定义 Kafka 默认连接参数；
- 定义 Elasticsearch 默认连接参数；
- 提供本地 / 外部环境切换机制；
- 创建 Spark Session；
- 解析 Kafka 中的邮件 JSON；
- 创建 Elasticsearch 客户端。

其中比较重要的配置包括：

#### `ENRON_PROFILE`

用于区分运行环境：

- `local`：默认连接本地 Kafka 和本地 Elasticsearch；
- `external`：使用 `.env.external` 或外部环境变量中的 Kafka / ES 配置。

#### `KAFKA_TOPIC`

默认 topic 应统一为：

```text
raw_emails_topic
```

Producer 和 Spark Consumer 必须使用同一个 topic。

#### `KAFKA_EMAIL_JSON_SCHEMA`

它定义 Spark 从 Kafka 读取 JSON 后应该如何解析字段。这个 Schema 必须与 `produce_emails_to_kafka.py` 发送的 JSON 格式一致。

#### `kafka_stream_to_email_df()`

这是 Spark 流作业读取 Kafka 邮件消息的标准入口。它会把 Kafka 中的 `value` 字段从字符串 JSON 转成结构化 Spark DataFrame。

#### `get_kafka_options()`

根据当前环境生成 Kafka 连接参数，兼容本地 Kafka 和外部 Kafka。

#### `get_es_client()`

根据当前环境生成 Elasticsearch 客户端，用于检查 ES 状态或写入结果。

当你遇到 Kafka 连不上、ES 连不上、字段解析错误、环境变量不生效等问题时，通常应该先检查这个文件。

---

### 5.2 `spark_apps/stream_enron_sentiment.py`

这是邮件情感分析流作业，也是整个实时分析流程中最关键的脚本之一。

它的职责包括：

1. 从 Kafka topic `raw_emails_topic` 读取邮件；
2. 将 Kafka 中的 JSON 消息解析成结构化邮件记录；
3. 对邮件正文 `body` 进行清洗；
4. 使用 DistilBERT 进行情感分析；
5. 将邮件情感分析结果写入 Elasticsearch 的 `enron_emails` 索引；
6. 从邮件发件人和收件人字段中构建通信关系边；
7. 将关系边聚合结果 upsert 到 `enron_edges` 索引。

处理流程可以概括为：

```text
Kafka readStream
      ↓
from_json 解析邮件结构
      ↓
sent_at 时间字段标准化
      ↓
DistilBERT 情感分析
      ↓
写入 enron_emails
      ↓
抽取 sender -> recipients 关系
      ↓
聚合并 upsert 到 enron_edges
```

`enron_emails` 中通常保存以下字段：

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

`enron_edges` 中通常保存以下字段：

- `src`
- `dst`
- `weight`
- `first_contact_at`
- `last_contact_at`
- `sample_subject`
- `avg_sentiment`
- `sentiment_sum`

如果你只想确认邮件是否能被成功消费、情感分析是否正常、ES 是否能写入数据，优先检查这个脚本的运行结果。

---

### 5.3 `spark_apps/build_email_relationship_graph.py`

这是关系图谱计算流作业。它不是直接从原始 Kafka 邮件中计算所有图指标，而是基于已经写入 Elasticsearch 的 `enron_edges` 关系边构建图。

它的主要职责包括：

1. 从 Elasticsearch 读取已累积的通信边数据；
2. 构建 GraphFrame；
3. 计算图指标；
4. 将人物节点指标、社区信息和图谱运行状态写回 Elasticsearch。

它会计算的主要指标包括：

- PageRank；
- 入度 `in_degree`；
- 出度 `out_degree`；
- 总度数 `total_degree`；
- 连通分量 / 社区 `community_id`；
- 是否为 Enron 内部邮箱 `is_enron`。

它写入的 Elasticsearch 索引包括：

- `enron_vertex_metrics`：人物节点指标；
- `enron_communities`：社区统计信息；
- `enron_stream_stats`：图谱计算状态，例如最后一次计算时间。

如果只运行 `stream_enron_sentiment.py`，而不运行这个脚本，那么邮件情感和通信边可以正常写入，但 Streamlit 中与 PageRank、Top 人物、社区概况相关的部分可能没有完整结果。

---

### 5.4 `spark_apps/streamlit_app.py`

这是项目自带的 Streamlit 前端看板。它不负责计算，只负责从 Elasticsearch 读取已经计算好的结果，并把结果展示出来。

它会读取以下 ES 索引：

- `enron_emails`：邮件情感分析结果；
- `enron_edges`：通信关系边；
- `enron_vertex_metrics`：人物图指标；
- `enron_communities`：社区信息；
- `enron_stream_stats`：图谱计算状态。

看板中通常包括：

- 系统状态；
- Kafka / ES / Spark 运行状态；
- 邮件分析进度；
- 情感分布；
- 最新邮件列表；
- 关系网络图；
- Top 关键人物；
- 社区概况；
- A/B 双人关系分析；
- Top 通信关系详情；
- 人物指标表。

如果你的目标是“看最终效果”，那么这个脚本就是主要入口。

---

### 5.5 `spark_apps/run.sh`

这是项目的一键启动脚本，用于统一启动 Spark 作业和 Streamlit 看板。

它支持两种环境：

#### `local`

本地环境，默认连接：

- Kafka：`localhost:9092`
- Elasticsearch：`http://localhost:9200`

#### `external`

外部环境，通常通过 `.env.external` 读取 Kafka 和 ES 的连接信息。这个模式适合连接课程或团队提供的远程 Kafka / ES 服务。

常见启动方式：

```bash
./run.sh auto local
./run.sh auto external
./run.sh normal local
./run.sh bootstrap external
```

参数含义如下：

- `auto`：自动判断启动模式。如果检测到边数据为空，通常会倾向于 bootstrap 回放；
- `normal`：只处理新增实时数据，适合已有历史数据、只想继续增量消费；
- `bootstrap`：从头回放数据，适合第一次运行或清空 checkpoint / ES 后重新计算。

日志通常写入：

```text
spark_apps/logs/sentiment.log
spark_apps/logs/graph.log
spark_apps/logs/streamlit.log
```

如果程序没有按预期运行，建议优先查看这些日志。

---

### 5.6 `spark_apps/.env.external`

这是外部环境变量模板文件，用于保存远程 Kafka 和 Elasticsearch 的连接信息。

它通常包含：

- Kafka bootstrap servers；
- Kafka API key / secret；
- Elasticsearch host；
- Elasticsearch API key；
- `ENRON_PROFILE=external`。

推荐启动方式：

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

注意：`.env.external` 中的 key、secret、host 等信息需要替换成真实可用的配置。

---

## 6. 从零开始运行项目

这一节适合第一次运行项目时直接照着做。

---

### 6.1 检查基础环境

建议先确认以下环境已经准备好：

- Python 已安装；
- PySpark / Spark 环境可用；
- Kafka broker 可访问；
- Elasticsearch 可访问；
- 如果使用 Kibana，需要保证 Kibana 已连接到对应 Elasticsearch；
- 如果使用 Streamlit，需要安装项目所需 Python 依赖。

检查 Python 版本：

```bash
python3 --version
```

项目推荐使用 Python 3.9，因为 `requirements.txt` 中的依赖版本通常是基于 Python 3.9 环境整理的。如果你的 Python 版本不同，可能需要调整部分包版本。

---

### 6.2 安装依赖

在项目根目录执行：

```bash
python3 -m pip install -r requirements.txt
```

如果只运行数据准备部分，至少需要：

- `kafka-python`
- `pandas`
- `kagglehub`

如果运行 Spark 和 Streamlit 部分，还需要安装 `requirements.txt` 中与 Spark、Elasticsearch、Streamlit、Transformers 等相关的依赖。

---

### 6.3 启动或确认 Kafka / Elasticsearch

#### 本地模式

如果使用本地环境，默认配置通常是：

```text
Kafka: localhost:9092
Elasticsearch: http://localhost:9200
```

#### 外部模式

如果使用外部环境，需要准备：

- Kafka bootstrap servers；
- Kafka API key / secret；
- Elasticsearch host；
- Elasticsearch API key。

然后在 `spark_apps/.env.external` 中配置，并通过下面的方式加载：

```bash
cd spark_apps
source .env.external
```

---

### 6.4 向 Kafka 写入邮件数据

在运行 Spark 作业前，必须先确保 Kafka topic 中有数据。

推荐写入 topic：

```text
raw_emails_topic
```

启动 Producer：

```bash
cd data_preparation
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic
```

如果使用远程 Kafka，需要把 `--bootstrap-servers` 换成远程地址，并根据脚本实现补充认证参数或环境变量。

小规模测试：

```bash
cd data_preparation
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic \
  --max-records 1000 \
  --sleep-seconds 0.01
```

---

### 6.5 启动 Spark 分析和 Streamlit 看板

#### 本地环境

```bash
cd spark_apps
./run.sh auto local
```

#### 外部环境

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

如果是第一次运行，或者你已经清空了 ES / checkpoint，建议使用 `auto` 或 `bootstrap`。如果只是继续处理新增数据，可以使用 `normal`。

---

### 6.6 使用 Kibana 展示结果

如果你使用 Kibana，不一定需要运行 `streamlit_app.py`。Kibana 的逻辑是直接连接 Elasticsearch 中已经写入的索引，然后在 Kibana 里创建 Data View 和 Dashboard。

至少需要先运行以下两个 Spark 分析脚本，保证 ES 中已经有数据：

```text
stream_enron_sentiment.py
build_email_relationship_graph.py
```

常用索引包括：

```text
enron_emails
enron_edges
enron_vertex_metrics
enron_communities
enron_stream_stats
```

在 Kibana 中可以围绕这些索引创建可视化，例如：

- 基于 `enron_emails` 展示情感分布、邮件数量趋势、最新邮件；
- 基于 `enron_edges` 展示通信关系强度、Top 通信对；
- 基于 `enron_vertex_metrics` 展示 PageRank、入度、出度最高的人；
- 基于 `enron_communities` 展示社区规模和社区数量；
- 基于 `enron_stream_stats` 检查图谱最后更新时间。

需要注意的是，部分 Kibana 高级功能可能依赖 Elasticsearch / Kibana 的版本或订阅权限。如果只是做基础索引检索和图表展示，通常可以直接基于 ES 中的数据创建可视化。

---

## 7. 从 0 重置并重新运行

如果你想清空已有结果并重新跑一遍，通常需要同时清理两类内容：

1. Elasticsearch 中已有索引；
2. Spark Streaming 的 checkpoint。

只删除 ES 索引但不删除 checkpoint，Spark 可能认为旧数据已经处理过，从而不会重新消费；只删除 checkpoint 但不删除 ES 索引，则可能造成重复写入或旧数据残留。因此建议两者一起清理。

本地 ES 示例：

```bash
curl -X DELETE "http://localhost:9200/enron_emails"
curl -X DELETE "http://localhost:9200/enron_edges"
curl -X DELETE "http://localhost:9200/enron_vertex_metrics"
curl -X DELETE "http://localhost:9200/enron_communities"
curl -X DELETE "http://localhost:9200/enron_stream_stats"
```

删除 checkpoint：

```bash
rm -rf spark_apps/checkpoints/enron_kafka_es
rm -rf spark_apps/checkpoints/enron_kafka_es_sentiment
rm -rf spark_apps/checkpoints/graph_stream
```

然后重新启动：

```bash
cd spark_apps
./run.sh auto local
```

或者外部环境：

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

---

## 8. 如何检查运行是否成功

### 8.1 检查 Kafka 是否有数据

如果 Kafka topic 没有数据，Spark 作业可以启动，但不会产生任何分析结果。

需要确认 Producer 是否已经向以下 topic 写入数据：

```text
raw_emails_topic
```

---

### 8.2 检查 Elasticsearch 是否写入成功

可以查询最新邮件：

```bash
curl "http://localhost:9200/enron_emails/_search?size=5&sort=sent_at:desc"
```

也可以检查关系边：

```bash
curl "http://localhost:9200/enron_edges/_search?size=5"
```

如果 `enron_emails` 有数据，但 `enron_vertex_metrics` 或 `enron_communities` 没有数据，通常说明情感分析脚本已经运行，但图计算脚本还没有成功完成。

---

### 8.3 检查日志

主要日志位置：

```text
spark_apps/logs/sentiment.log
spark_apps/logs/graph.log
spark_apps/logs/streamlit.log
```

排查顺序建议：

1. 先看 `sentiment.log`，确认 Kafka 消费和 ES 写入是否正常；
2. 再看 `graph.log`，确认 GraphFrames 图计算是否正常；
3. 最后看 `streamlit.log`，确认前端看板是否成功启动。

---

## 9. 常见问题

### 9.1 Spark 启动了，但看不到数据

最常见原因是 Kafka topic 不一致。

请确认 Producer 写入的 topic 和 Spark 读取的 topic 都是：

```text
raw_emails_topic
```

如果 Producer 写入了 `emails_raw`，但 Spark 读取的是 `raw_emails_topic`，那么 Spark 不会读到任何数据。

---

### 9.2 Streamlit 中没有 PageRank 或社区结果

这通常说明 `build_email_relationship_graph.py` 没有成功运行，或者 `enron_edges` 中的数据还不够。

检查：

- `enron_edges` 是否有数据；
- `spark_apps/logs/graph.log` 是否报错；
- `enron_vertex_metrics` 是否写入成功；
- `enron_communities` 是否写入成功；
- `enron_stream_stats` 中是否有最后计算时间。

---

### 9.3 Kibana 和 Streamlit 有什么区别

Streamlit 是项目自带的前端展示程序，它读取 ES 数据后用 Python 代码组织成一个完整看板。

Kibana 是 Elasticsearch 生态中的通用可视化工具，它直接读取 ES 索引，由用户自己创建 Data View、图表和 Dashboard。

所以：

- 想快速看项目预设结果：用 Streamlit；
- 想自由探索 ES 数据：用 Kibana；
- 两者都依赖 Elasticsearch 中已经写入的分析结果。

---

### 9.4 只运行 Producer 可以看到结果吗

不可以。Producer 只负责把 Kaggle 邮件数据写入 Kafka，不负责情感分析、关系图谱计算，也不会写入最终的 Elasticsearch 分析索引。

完整结果至少需要：

```text
Producer -> Kafka -> stream_enron_sentiment.py -> Elasticsearch
```

如果还想看到人物 PageRank、社区信息等图谱指标，还需要运行：

```text
build_email_relationship_graph.py
```

---

### 9.5 只运行 Streamlit 可以看到结果吗

不一定。Streamlit 只负责展示，不负责计算。

如果 Elasticsearch 中还没有 `enron_emails`、`enron_edges`、`enron_vertex_metrics` 等索引，Streamlit 即使启动成功，也没有完整数据可以展示。

---

## 10. 推荐运行顺序

如果你是第一次从零运行，推荐严格按照下面顺序：

```text
1. 安装依赖
2. 启动 Kafka 和 Elasticsearch
3. 使用 Producer 把 Enron 邮件写入 raw_emails_topic
4. 运行 stream_enron_sentiment.py，生成 enron_emails 和 enron_edges
5. 运行 build_email_relationship_graph.py，生成人物指标和社区结果
6. 运行 Streamlit 看板，或在 Kibana 中创建可视化
```

如果使用 `run.sh`，推荐：

```bash
cd spark_apps
./run.sh auto local
```

外部环境：

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

---

## 11. 最重要的注意事项

1. **Kafka topic 必须统一。**  Producer 和 Spark Consumer 都应使用 `raw_emails_topic`。

2. **Streamlit 不负责计算。**  它只读取 Elasticsearch 中已有结果并展示。

3. **Kibana 也是读取 Elasticsearch。**  使用 Kibana 前，必须先保证 Spark 已经把数据写入 ES。

4. **图指标依赖关系边。**  如果 `enron_edges` 没有数据，PageRank、社区、Top 人物等指标不会正常生成。

5. **重跑时要同时清理 ES 索引和 checkpoint。**  只清理其中一个可能导致数据不更新、重复写入或结果残留。

6. **外部环境要先加载 `.env.external`。**  否则 Spark 可能仍然使用本地 Kafka / ES 默认配置。

---

## 12. 一句话总结

这个项目的核心是搭建一条完整的实时邮件关系分析流水线：**先用 Producer 把 Enron 邮件写入 Kafka，再用 Spark Streaming 做情感分析和关系图计算，最后把结果存入 Elasticsearch，并通过 Streamlit 或 Kibana 展示邮件情感、人物关系和社区结构。**

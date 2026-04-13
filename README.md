# 5003 Email Relationship Spark

基于 Kafka + Spark Structured Streaming + Elasticsearch + Streamlit 的 Enron 邮件实时分析项目。

项目支持：
- 实时邮件情感分析（DistilBERT）
- 邮件关系边增量聚合（src/dst/weight/avg_sentiment）
- 图指标计算（PageRank、度、社区）
- 可视化看板（情感、关系图谱、A/B 双人关系分析）

## 1. 项目结构

```text
.
├── data_preparation/
│   ├── README.md
│   └── scripts/
│       ├── produce_emails_to_kafka.py
│       ├── email_record_utils.py
│       └── ...
├── spark_apps/
│   ├── stream_enron_sentiment.py          # Kafka -> 情感分析 -> ES(sentiment + edges)
│   ├── build_email_relationship_graph.py  # Kafka -> 图计算 -> ES(vertex/community/stats)
│   ├── streamlit_app.py                   # 前端看板
│   ├── enron_common.py                    # 公共配置与工具
│   └── run.sh                             # 一键启动
├── requirements.txt
└── README.md
```

## 2. 技术栈

- Python 3.9+（推荐 3.10+）
- Apache Spark 3.5.x（PySpark）
- Kafka
- Elasticsearch 8.x
- Streamlit
- Transformers (DistilBERT) + PyTorch
- GraphFrames（图指标计算模块）

## 3. 环境准备

### 3.1 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

### 3.2 启动基础服务

确保以下服务可访问：
- Kafka（默认 `localhost:9092`）
- Elasticsearch（默认 `http://localhost:9200`）

如需修改连接参数，请设置环境变量（示例）：

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="emails_raw"
export ES_HOST="localhost"
export ES_PORT="9200"
```

## 4. 运行方式

### 4.0 接上data preparation的运行方式（目前来说kafka已经存在一些数据尚未消耗完）

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic emails_raw
```

### 4.1 一键启动

```bash
spark_apps/run.sh
```

可选模式(简单运行直接用命令就可以，这个是附加的)：
- `auto`：自动判断（首次无边数据时自动 bootstrap）
- `normal`：实时模式（`startingOffsets=latest`）
- `bootstrap`：全量回放模式（`startingOffsets=earliest` + 新 checkpoint）

启动后默认访问：
- Streamlit: `http://localhost:8501`

日志文件（可以定位错误这样子）：
- `spark_apps/logs/sentiment.log`
- `spark_apps/logs/graph.log`
- `spark_apps/logs/streamlit.log`

### 4.2 分模块启动（可选）

1) 数据写入 Kafka（参见 `data_preparation/README.md`）

2) 启动情感分析流：

```bash
python3 spark_apps/stream_enron_sentiment.py
```

3) 启动图关系计算流（可选）：

```bash
python3 spark_apps/build_email_relationship_graph.py
```

4) 启动前端：

```bash
streamlit run spark_apps/streamlit_app.py
```

## 5. 数据流与索引说明

### 5.1 主要 Elasticsearch 索引

- `enron_emails_sentiment`：邮件情感结果（发件人、收件人、subject、sentiment、score 等）
- `enron_edges`：关系边聚合（src、dst、weight、avg_sentiment 等）
- `enron_vertex_metrics`：顶点指标（pagerank、in/out/total degree、community_id）
- `enron_communities`：社区汇总
- `enron_stream_stats`：图计算状态统计

### 5.2 关系构建逻辑

- 边方向：`sender -> recipient`
- 权重：通信次数累计
- 边情感：从邮件情感分数累积并计算均值
- 图指标：基于 GraphFrames 周期重算

## 6. 前端功能

`streamlit_app.py` 主要包括：
- 系统状态与处理进度
- 情感分布和最新邮件
- 关系网络图（可视化 Top 边）
- Top 关键人物、社区概况
- A/B 双人关系分析：
  - 选择两个人
  - 查看往来邮件数量与平均情感
  - 查看往来 topic（subject Top）
  - 查看往来邮件明细

人物显示名优先级：
1. 指标索引中的显式姓名字段（如有）
2. 从 `x_folder` 中提取姓名（如 `Allen, Phillip K.`）
3. 邮件头中的姓名
4. 邮箱前缀兜底

## 7. 常见问题

### 7.1 `KeyError: ['community_id'] not in index`

原因：部分数据来源不包含 `community_id` 字段。

当前代码已做防御性处理：展示时会根据存在列动态选择，不会因缺字段崩溃。

### 7.2 GraphFrames 安装问题

`graphframes-py` 新版本通常要求 Python 3.10+。若 Python 3.9 无法安装：
- 情感流可单独运行
- 图计算模块建议切换到 Python 3.10+ 环境

### 7.3 checkpoint 导致“看不到新数据”

首次全量回放建议：
- 使用 `bootstrap` 模式
- 或手动指定新的 `--checkpoint-dir`

## 8. 开发建议

- 不要提交运行时产物（如 checkpoints、logs、`.pyc`、`.DS_Store`）
- 建议添加并完善 `.gitignore`
- 若长期运行，建议为 Spark/ES/Kafka 增加监控和资源上限配置
# DataFrame to Kafka Producer

这个项目当前的正式流程是：

`Kaggle DataFrame -> 结构化邮件记录 -> Kafka topic`

直接从 Kaggle 数据集加载 `pandas DataFrame`，再逐行转换成 Kafka 消息。

## 核心脚本

- [kaggle.py](/Users/maoabear/Desktop/msbd5003project/scripts/kaggle.py)
  使用 `kagglehub` 从 Kaggle 数据集 `wcukierski/enron-email-dataset` 读取 `emails.csv`，并返回 `pandas DataFrame`
- [email_record_utils.py](/Users/maoabear/Desktop/msbd5003project/scripts/email_record_utils.py)
  把 DataFrame 中的每一行邮件解析成结构化记录
- [produce_emails_to_kafka.py](/Users/maoabear/Desktop/msbd5003project/scripts/produce_emails_to_kafka.py)
  调用 `load_dataframe()` 读取 DataFrame，再把每条结构化记录发到 Kafka

## 运行环境

```bash
source .venv-kafka/bin/activate
```

当前运行这套代码需要这些 Python 包：

- `kafka-python`
- `pandas`
- `kagglehub`

## 数据读取方式

正式 Kafka Producer 的读取顺序是：

1. [produce_emails_to_kafka.py](/Users/maoabear/Desktop/msbd5003project/scripts/produce_emails_to_kafka.py) 调用 [kaggle.py](/Users/maoabear/Desktop/msbd5003project/scripts/kaggle.py)
2. [kaggle.py](/Users/maoabear/Desktop/msbd5003project/scripts/kaggle.py) 用 `kagglehub.dataset_load(...)` 把 Enron 数据集加载成 `pandas DataFrame`
3. [produce_emails_to_kafka.py](/Users/maoabear/Desktop/msbd5003project/scripts/produce_emails_to_kafka.py) 把 DataFrame 转成逐行 records
4. [email_record_utils.py](/Users/maoabear/Desktop/msbd5003project/scripts/email_record_utils.py) 解析每一行邮件内容
5. 解析后的 JSON 记录被发送到 Kafka topic

## 启动 Kafka Producer

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic emails_raw
```

如果你想先做小规模测试：

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic emails_raw \
  --max-records 1000 \
  --sleep-seconds 0.01
```

## 无 Kafka broker 时：导出解析后的 JSON 批文件

如果你现在还没有 Kafka broker，可以先把 DataFrame 中的邮件全部解析出来，并且每 `5000` 条写成一个 JSON 文件：

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails \
  --records-per-file 5000
```

如果你只想先测试一小部分：

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails_test \
  --records-per-file 5000 \
  --max-records 10000
```

这会生成类似下面的文件：

- `batch_000000.json`
- `batch_000001.json`
- `batch_000002.json`

每个文件里是一个 JSON 数组，数组中每个元素就是一条解析后的邮件记录。

## 可选：单独预览 DataFrame

如果你只是想确认 Kaggle 数据能正常读成 DataFrame，可以运行：

```bash
python3 scripts/kaggle.py
```

这个脚本会打印前 5 行数据，不会向 Kafka 发消息。

## Kafka 消息格式

每条 Kafka 消息：

- `key`：邮件文件路径，比如 `allen-p/_sent_mail/1.`
- `value`：一条 JSON 邮件记录

JSON 中当前包含这些字段：

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

其中 `sent_at` 会统一转换成 UTC 字符串，例如：

```text
2001-05-14 23:39:00 UTC
```

## 前提

运行前需要你已经有可连接的 Kafka broker，例如：

- `localhost:9092`
- 或远程 Kafka 集群地址

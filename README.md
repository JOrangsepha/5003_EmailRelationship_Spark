# 5003 Email Relationship Spark Project Documentation

This project is a real-time Enron email analysis system built with **Kafka + Spark Structured Streaming + Elasticsearch + GraphFrames + Streamlit / Kibana**. Its core goal is to first load emails from the Kaggle Enron email dataset, convert each email into a structured JSON message, and write the messages into Kafka. Spark then consumes the Kafka stream, performs email sentiment analysis, constructs communication relationship edges, computes graph metrics, and finally visualizes the results through either the built-in Streamlit dashboard or Kibana.

```text
Kaggle Enron Dataset
      ↓
DataFrame loading and email parsing
      ↓
Kafka Producer writes to raw_emails_topic
      ↓
Spark Structured Streaming consumes in real time
      ↓
Sentiment analysis + communication edge aggregation
      ↓
Elasticsearch stores analysis results
      ↓
GraphFrames computes relationship graph metrics
      ↓
Streamlit / Kibana visualization
```

In this project, **Streamlit** is the built-in integrated dashboard, suitable for directly presenting the analysis results. **Kibana**, on the other hand, performs visualization directly on top of Elasticsearch indices and requires access to the Elasticsearch API. Both tools read from the same data source, but they differ in their visualization workflow and ease of use.

---

## 1. What This Project Can Do

This project supports the following functions:

- Load the Enron email dataset from Kaggle;
- Parse raw emails into structured JSON records;
- Use Kafka to simulate a real-time email data stream;
- Use Spark Structured Streaming to consume Kafka email messages in real time;
- Use DistilBERT to perform sentiment analysis on email bodies;
- Build communication relationship edges from the `sender`, `to`, `cc`, and `bcc` fields;
- Write email sentiment results, communication edges, and graph metrics into Elasticsearch;
- Use GraphFrames to compute PageRank, in-degree, out-degree, communities, and other graph metrics;
- Use Streamlit to display a complete analysis dashboard;
- Or use Kibana to explore and visualize the Elasticsearch indices directly.

---

## 2. Overall Project Structure

The project is mainly divided into two parts:

1. `data_preparation/`: reads data from Kaggle, parses emails, and writes them into Kafka;
2. `spark_apps/`: reads data from Kafka, runs Spark analysis jobs, writes results into Elasticsearch, and starts the visualization dashboard.

The recommended directory structure is as follows:

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

## 3. Overall Workflow

The complete data flow is shown below:

```text
Enron email dataset
      │
      ▼
Kaggle DataFrame
      │
      ▼
email_record_utils.py parses email fields
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
      ├── Email body sentiment analysis              ├── Read enron_edges
      ├── Write to enron_emails                      ├── Build GraphFrame
      └── Aggregate communication edges to enron_edges└── Write graph metrics and community results
      │                                              │
      └──────────────────────────────────────────────┘
                         │
                         ▼
                 Elasticsearch
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
   Streamlit dashboard              Kibana visualization
```

From an engineering perspective, the project can be divided into three layers:

1. **Data layer**: loads Enron emails from Kaggle and writes them into Kafka;
2. **Computation layer**: Spark reads data from Kafka in real time and performs sentiment analysis and graph computation;
3. **Visualization layer**: Streamlit or Kibana reads results from Elasticsearch and displays them.

---

## 4. Data Preparation: From Kaggle to Kafka

This part converts the Enron email dataset from Kaggle into a real-time Kafka message stream.

The formal process is:

```text
Kaggle DataFrame -> structured email records -> Kafka topic: raw_emails_topic
```

In other words, the project does not let Spark read Kaggle files directly. Instead, the Producer first writes emails into Kafka. This allows the later Spark programs to consume emails as real-time streaming data.

### 4.1 Core Scripts

#### `data_preparation/scripts/kaggle.py`

This script uses `kagglehub` to read `emails.csv` from the Kaggle dataset `wcukierski/enron-email-dataset` and returns a `pandas DataFrame`.

Its role can be understood as: **loading the Kaggle dataset into Python**.

If you only want to check whether the Kaggle data can be loaded correctly, run:

```bash
python3 scripts/kaggle.py
```

This command usually prints the first five rows and does not write any messages to Kafka.

#### `data_preparation/scripts/email_record_utils.py`

This script parses each row in the DataFrame into a structured email record.

Raw Enron emails usually contain a large amount of unstructured text. Fields such as sender, recipients, subject, timestamp, and body may all be embedded in the raw `message` field. This script extracts those values into a unified schema, including:

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

The `sent_at` field is normalized into a UTC string, for example:

```text
2001-05-14 23:39:00 UTC
```

#### `data_preparation/scripts/produce_emails_to_kafka.py`

This is the main Kafka Producer script. It calls `load_dataframe()` to load the Kaggle DataFrame, uses the email parsing utilities to convert each row into structured JSON, and then sends those JSON messages one by one to a Kafka topic.

Its role can be understood as: **simulating parsed emails as a real-time data stream**.

---

### 4.2 Kafka Message Format

Each Kafka message contains a `key` and a `value`:

- `key`: the email file path, for example `allen-p/_sent_mail/1.`;
- `value`: one structured JSON email record.

The `value` usually contains the following fields:

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

The `KAFKA_EMAIL_JSON_SCHEMA` used later in Spark must be consistent with these JSON fields. Otherwise, Spark may parse fields as null or fail to parse the Kafka messages correctly.

---

### 4.3 Starting the Kafka Producer

Before running the Producer, make sure you have access to a Kafka broker, such as:

- Local Kafka: `localhost:9092`;
- Or a remote Kafka cluster address.

The recommended topic name is:

```text
raw_emails_topic
```

This is because the default Kafka topic in `spark_apps/enron_common.py` is also `raw_emails_topic`. If the Producer writes to a different topic while Spark still reads from `raw_emails_topic`, the Spark jobs may start successfully but will not consume any email data.

Start the Producer:

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic
```

For a small-scale test, limit the number of records:

```bash
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic \
  --max-records 1000 \
  --sleep-seconds 0.01
```

Parameter meanings:

- `--bootstrap-servers`: Kafka broker address;
- `--topic`: the Kafka topic to write to;
- `--max-records`: the maximum number of emails to send;
- `--sleep-seconds`: the pause between messages, used to simulate streaming input.

---

### 4.4 Alternative When No Kafka Broker Is Available

If you do not currently have a Kafka broker, you can first export the parsed email results into JSON batch files. This is useful for checking whether the parsing logic is correct.

Export all data:

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails \
  --records-per-file 5000
```

Export only a small test subset:

```bash
python3 scripts/email_record_utils.py \
  --output-dir parsed_emails_test \
  --records-per-file 5000 \
  --max-records 10000
```

The generated files look like this:

```text
batch_000000.json
batch_000001.json
batch_000002.json
```

Each file contains a JSON array, and each element in the array is one parsed email record.

Note that this approach is only for checking the data parsing result. It cannot replace the formal Kafka streaming workflow. To run the main Spark Streaming process, the Kafka topic still needs to contain data.

---

## 5. Spark Apps: From Kafka to Elasticsearch

`spark_apps/` is the computation and visualization core of the project. It mainly handles three tasks:

1. Read structured emails from Kafka;
2. Use Spark to perform sentiment analysis, communication relationship construction, and graph metric computation;
3. Write results into Elasticsearch and display them through Streamlit.

---

### 5.1 `spark_apps/enron_common.py`

This is the shared configuration and utility file for `spark_apps/`. It can be regarded as the configuration center of the Spark analysis components.

It is responsible for:

- Defining the email JSON Schema;
- Defining default Kafka connection parameters;
- Defining default Elasticsearch connection parameters;
- Providing local / external environment switching;
- Creating Spark Sessions;
- Parsing email JSON messages from Kafka;
- Creating Elasticsearch clients.

Important configurations include:

#### `ENRON_PROFILE`

Used to distinguish the runtime environment:

- `local`: connects to local Kafka and local Elasticsearch by default;
- `external`: uses Kafka / Elasticsearch configurations from `.env.external` or external environment variables.

#### `KAFKA_TOPIC`

The default topic should be unified as:

```text
raw_emails_topic
```

The Producer and Spark Consumer must use the same topic.

#### `KAFKA_EMAIL_JSON_SCHEMA`

This defines how Spark should parse the JSON fields read from Kafka. The schema must be consistent with the JSON format sent by `produce_emails_to_kafka.py`.

#### `kafka_stream_to_email_df()`

This is the standard entry point for Spark streaming jobs to read email messages from Kafka. It converts Kafka's `value` field from a JSON string into a structured Spark DataFrame.

#### `get_kafka_options()`

Generates Kafka connection parameters according to the current environment. It supports both local Kafka and external Kafka.

#### `get_es_client()`

Generates an Elasticsearch client according to the current environment. It is used to check ES status or write results.

When Kafka cannot connect, Elasticsearch cannot connect, fields fail to parse, or environment variables do not take effect, this file is usually the first place to check.

---

### 5.2 `spark_apps/stream_enron_sentiment.py`

This is the email sentiment analysis streaming job and one of the most important scripts in the real-time analysis workflow.

Its responsibilities include:

1. Read emails from Kafka topic `raw_emails_topic`;
2. Parse Kafka JSON messages into structured email records;
3. Clean the email body field `body`;
4. Use DistilBERT for sentiment analysis;
5. Write email sentiment analysis results into the Elasticsearch index `enron_emails`;
6. Build communication relationship edges from sender and recipient fields;
7. Upsert aggregated relationship edges into the Elasticsearch index `enron_edges`.

The processing flow can be summarized as:

```text
Kafka readStream
      ↓
from_json parses email structure
      ↓
sent_at timestamp normalization
      ↓
DistilBERT sentiment analysis
      ↓
Write to enron_emails
      ↓
Extract sender -> recipients relationships
      ↓
Aggregate and upsert to enron_edges
```

The `enron_emails` index usually stores the following fields:

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

The `enron_edges` index usually stores the following fields:

- `src`
- `dst`
- `weight`
- `first_contact_at`
- `last_contact_at`
- `sample_subject`
- `avg_sentiment`
- `sentiment_sum`

If you only want to check whether emails can be consumed successfully, whether sentiment analysis works, and whether Elasticsearch receives data, you should inspect this script first.

---

### 5.3 `spark_apps/build_email_relationship_graph.py`

This is the relationship graph computation streaming job. It does not compute all graph metrics directly from raw Kafka emails. Instead, it builds a graph based on the accumulated `enron_edges` relationship data already written into Elasticsearch.

Its main responsibilities include:

1. Read accumulated communication edges from Elasticsearch;
2. Build a GraphFrame;
3. Compute graph metrics;
4. Write vertex metrics, community information, and graph runtime status back into Elasticsearch.

The main metrics include:

- PageRank;
- In-degree `in_degree`;
- Out-degree `out_degree`;
- Total degree `total_degree`;
- Connected component / community `community_id`;
- Whether the email address belongs to Enron, `is_enron`.

It writes to the following Elasticsearch indices:

- `enron_vertex_metrics`: person-level vertex metrics;
- `enron_communities`: community statistics;
- `enron_stream_stats`: graph computation status, such as the latest computation time.

If you only run `stream_enron_sentiment.py` but do not run this script, email sentiment and communication edges can still be written successfully. However, the Streamlit sections related to PageRank, Top people, and community overview may not have complete results.

---

### 5.4 `spark_apps/streamlit_app.py`

If you want to use Kibana for visualization, go directly to Section 6.

This is the built-in Streamlit frontend dashboard. It does not perform computation. It only reads already-computed results from Elasticsearch and displays them.

It reads the following ES indices:

- `enron_emails`: email sentiment analysis results;
- `enron_edges`: communication relationship edges;
- `enron_vertex_metrics`: person-level graph metrics;
- `enron_communities`: community information;
- `enron_stream_stats`: graph computation status.

The dashboard usually includes:

- System status;
- Kafka / ES / Spark runtime status;
- Email analysis progress;
- Sentiment distribution;
- Latest email list;
- Relationship network graph;
- Top key people;
- Community overview;
- A/B two-person relationship analysis;
- Top communication relationship details;
- Person metric table.

If your goal is to view the final results, this script is the main entry point.

---

### 5.5 `spark_apps/run.sh`

This is the one-click startup script used to start Spark jobs and the Streamlit dashboard in a unified way.

It supports two environments:

#### `local`

Local environment. By default, it connects to:

- Kafka: `localhost:9092`
- Elasticsearch: `http://localhost:9200`

#### `external`

External environment. It usually reads Kafka and ES connection information from `.env.external`. This mode is suitable for connecting to remote Kafka / ES services provided by a course or team.

Common startup commands:

```bash
./run.sh auto local
./run.sh auto external
./run.sh normal local
./run.sh bootstrap external
```

Parameter meanings:

- `auto`: automatically decides the startup mode. If edge data is empty, it usually tends to perform a bootstrap replay;
- `normal`: only processes newly added real-time data. This is suitable when historical data already exists and you only want incremental consumption;
- `bootstrap`: replays data from the beginning. This is suitable for the first run or for recomputation after clearing checkpoints / ES indices.

Logs are usually written to:

```text
spark_apps/logs/sentiment.log
spark_apps/logs/graph.log
spark_apps/logs/streamlit.log
```

If the program does not behave as expected, check these logs first.

---

### 5.6 `spark_apps/.env.external`

This is the external environment variable template file. It stores connection information for remote Kafka and Elasticsearch.

It usually contains:

- Kafka bootstrap servers;
- Kafka API key / secret;
- Elasticsearch host;
- Elasticsearch API key;
- `ENRON_PROFILE=external`.

Recommended startup method:

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

Note: the key, secret, host, and other values in `.env.external` must be replaced with real valid configurations.

---

## 6. Running the Project from Scratch

This section is intended for first-time project execution.

---

### 6.1 Check the Basic Environment

Make sure the following components are ready:

- Python is installed;
- PySpark / Spark environment is available;
- Kafka broker is accessible;
- Elasticsearch is accessible;
- If using Kibana, make sure Kibana is connected to the corresponding Elasticsearch instance;
- If using Streamlit, install all required Python dependencies.

Check the Python version:

```bash
python3 --version
```

Python 3.9 is recommended because the dependency versions in `requirements.txt` are usually organized based on a Python 3.9 environment. If your Python version is different, you may need to adjust some package versions.

---

### 6.2 Install Dependencies

Run the following command in the project root directory:

```bash
python3 -m pip install -r requirements.txt
```

If you only run the data preparation part, the minimum required packages are:

- `kafka-python`
- `pandas`
- `kagglehub`

If you run the Spark and Streamlit components, you also need the dependencies related to Spark, Elasticsearch, Streamlit, Transformers, and other packages listed in `requirements.txt`.

---

### 6.3 Start or Confirm Kafka / Elasticsearch

#### Local mode

If using a local environment, the default configuration is usually:

```text
Kafka: localhost:9092
Elasticsearch: http://localhost:9200
```

#### External mode

If using an external environment, prepare the following:

- Kafka bootstrap servers;
- Kafka API key / secret;
- Elasticsearch host;
- Elasticsearch API key.

Then configure them in `spark_apps/.env.external` and load them with:

```bash
cd spark_apps
source .env.external
```

---

### 6.4 Write Email Data into Kafka

Before running Spark jobs, make sure the Kafka topic contains data.

Recommended topic:

```text
raw_emails_topic
```

Start the Producer:

```bash
cd data_preparation
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic
```

If using remote Kafka, replace `--bootstrap-servers` with the remote address, and add authentication parameters or environment variables according to your script implementation.

Small-scale test:

```bash
cd data_preparation
python3 scripts/produce_emails_to_kafka.py \
  --bootstrap-servers localhost:9092 \
  --topic raw_emails_topic \
  --max-records 1000 \
  --sleep-seconds 0.01
```

---

### 6.5 Start Spark Analysis and the Streamlit Dashboard

#### Local environment

```bash
cd spark_apps
./run.sh auto local
```

#### External environment

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

For the first run, or after clearing ES / checkpoints, use `auto` or `bootstrap`. If you only want to continue processing newly added data, use `normal`.

---

### 6.6 Visualizing Results with Kibana

If you use Kibana, you do not necessarily need to run `streamlit_app.py`. Kibana connects directly to the indices written into Elasticsearch, and you can create Data Views and Dashboards in Kibana.

At minimum, run the following two Spark analysis scripts first so that Elasticsearch contains data:

```text
stream_enron_sentiment.py
build_email_relationship_graph.py
```

Common indices include:

```text
enron_emails
enron_edges
enron_vertex_metrics
enron_communities
enron_stream_stats
```

In Kibana, you can create visualizations around these indices, for example:

- Use `enron_emails` to show sentiment distribution, email count trends, and latest emails;
- Use `enron_edges` to show communication relationship strength and top communication pairs;
- Use `enron_vertex_metrics` to show people with the highest PageRank, in-degree, and out-degree;
- Use `enron_communities` to show community size and number of communities;
- Use `enron_stream_stats` to check the latest graph computation time.

Some advanced Kibana features may depend on the Elasticsearch / Kibana version or subscription. For basic index search and chart visualization, you can usually create visualizations directly from ES data.

---

## 7. Resetting and Rerunning from Zero

If you want to clear existing results and rerun everything, you usually need to clean two types of content:

1. Existing indices in Elasticsearch;
2. Spark Streaming checkpoints.

If you delete ES indices but keep checkpoints, Spark may believe old data has already been processed and may not consume it again. If you delete checkpoints but keep ES indices, duplicate writes or stale data may remain. Therefore, it is recommended to clean both at the same time.

Local ES example:

```bash
curl -X DELETE "http://localhost:9200/enron_emails"
curl -X DELETE "http://localhost:9200/enron_edges"
curl -X DELETE "http://localhost:9200/enron_vertex_metrics"
curl -X DELETE "http://localhost:9200/enron_communities"
curl -X DELETE "http://localhost:9200/enron_stream_stats"
```

Delete checkpoints:

```bash
rm -rf spark_apps/checkpoints/enron_kafka_es
rm -rf spark_apps/checkpoints/enron_kafka_es_sentiment
rm -rf spark_apps/checkpoints/graph_stream
```

Then restart:

```bash
cd spark_apps
./run.sh auto local
```

Or for the external environment:

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

---

## 8. How to Check Whether the Project Is Running Successfully

### 8.1 Check Whether Kafka Has Data

If the Kafka topic has no data, the Spark jobs may start successfully but will not produce any analysis results.

Confirm that the Producer has written data to:

```text
raw_emails_topic
```

---

### 8.2 Check Whether Elasticsearch Receives Data

Query the latest emails:

```bash
curl "http://localhost:9200/enron_emails/_search?size=5&sort=sent_at:desc"
```

Check relationship edges:

```bash
curl "http://localhost:9200/enron_edges/_search?size=5"
```

If `enron_emails` has data but `enron_vertex_metrics` or `enron_communities` has no data, it usually means the sentiment analysis script has run, but the graph computation script has not completed successfully.

---

### 8.3 Check Logs

Main log locations:

```text
spark_apps/logs/sentiment.log
spark_apps/logs/graph.log
spark_apps/logs/streamlit.log
```

Recommended debugging order:

1. Check `sentiment.log` first to confirm Kafka consumption and ES writes;
2. Check `graph.log` to confirm GraphFrames graph computation;
3. Check `streamlit.log` to confirm whether the frontend dashboard started successfully.

---

## 9. Common Issues

### 9.1 Spark Starts, but No Data Appears

The most common reason is a Kafka topic mismatch.

Make sure both the Producer and Spark Consumer use:

```text
raw_emails_topic
```

If the Producer writes to `emails_raw` while Spark reads from `raw_emails_topic`, Spark will not consume any data.

---

### 9.2 Streamlit Has No PageRank or Community Results

This usually means `build_email_relationship_graph.py` did not run successfully, or there is not enough data in `enron_edges`.

Check:

- Whether `enron_edges` has data;
- Whether `spark_apps/logs/graph.log` contains errors;
- Whether `enron_vertex_metrics` was written successfully;
- Whether `enron_communities` was written successfully;
- Whether `enron_stream_stats` contains a latest computation time.

---

### 9.3 What Is the Difference Between Kibana and Streamlit?

Streamlit is the built-in frontend application of this project. It reads ES data and organizes it into a complete dashboard using Python code.

Kibana is a general-purpose visualization tool in the Elasticsearch ecosystem. It directly reads ES indices, and users create their own Data Views, charts, and Dashboards.

Therefore:

- Use Streamlit if you want to quickly view the project’s predefined results;
- Use Kibana if you want to freely explore ES data;
- Both depend on analysis results already written into Elasticsearch.

---

### 9.4 Can I See Results by Running Only the Producer?

No. The Producer only writes Kaggle email data into Kafka. It does not perform sentiment analysis, relationship graph computation, or write final analysis indices into Elasticsearch.

A minimal complete result requires:

```text
Producer -> Kafka -> stream_enron_sentiment.py -> Elasticsearch
```

If you also want PageRank, community information, and other graph metrics, run:

```text
build_email_relationship_graph.py
```

---

### 9.5 Can I See Results by Running Only Streamlit?

Not necessarily. Streamlit only displays results. It does not perform computation.

If Elasticsearch does not yet contain indices such as `enron_emails`, `enron_edges`, and `enron_vertex_metrics`, Streamlit may start successfully but will not have complete data to display.

---

## 10. Recommended Running Order

For a first run from scratch, follow this order strictly:

```text
1. Install dependencies
2. Start Kafka and Elasticsearch
3. Use the Producer to write Enron emails into raw_emails_topic
4. Run stream_enron_sentiment.py to generate enron_emails and enron_edges
5. Run build_email_relationship_graph.py to generate person metrics and community results
6. Run the Streamlit dashboard, or create visualizations in Kibana
```

If using `run.sh`, the recommended command is:

```bash
cd spark_apps
./run.sh auto local
```

For the external environment:

```bash
cd spark_apps
source .env.external && ./run.sh auto external
```

---

## 11. Most Important Notes

1. **The Kafka topic must be consistent.** The Producer and Spark Consumer should both use `raw_emails_topic`.

2. **Streamlit does not perform computation.** It only reads existing results from Elasticsearch and displays them.

3. **Kibana also reads from Elasticsearch.** Before using Kibana, make sure Spark has already written data into ES.

4. **Graph metrics depend on communication edges.** If `enron_edges` has no data, PageRank, communities, Top people, and other graph metrics cannot be generated correctly.

5. **When rerunning from scratch, clean both ES indices and checkpoints.** Cleaning only one of them may cause data not to update, duplicate writes, or stale results.

6. **Load `.env.external` before using the external environment.** Otherwise, Spark may still use the default local Kafka / ES configuration.

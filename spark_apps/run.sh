#!/bin/bash
set -euo pipefail
# 用法：
#   ./run.sh                 # auto：自动判断（首次无图数据会走 bootstrap）
#   ./run.sh auto
#   ./run.sh normal          # 实时模式（latest）
#   ./run.sh bootstrap       # 首次回放模式（earliest + 新 checkpoint）
#
# 可选环境变量：
#   PYTHON_BIN=python3.10
#   GRAPH_TRIGGER_INTERVAL="10 seconds"
#   GRAPH_RECOMPUTE_INTERVAL=1
#   GRAPH_CHECKPOINT_DIR="checkpoints/graph_stream_custom"
#   RUN_GRAPH_METRICS=0                # 0: 仅实时边/平均情感（推荐）；1: 启动 GraphFrames 指标计算
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-auto}"
if [[ "$MODE" != "auto" && "$MODE" != "normal" && "$MODE" != "bootstrap" ]]; then
  echo "错误: 模式必须是 auto / normal / bootstrap"
  echo "示例: ./run.sh auto"
  exit 1
fi

# 与情感脚本、PySpark 使用同一解释器（可覆盖：export PYTHON_BIN=/path/to/python3）
PYTHON_BIN="${PYTHON_BIN:-python3}"

# 本脚本用 python3 直接跑 Structured Streaming，JVM 必须和 PySpark 的 Python 包同版本。
# 不要沿用 Homebrew 的 SPARK_HOME（例如 4.1.x），否则会出现 SparkSession 构造失败。
export SPARK_HOME="$($PYTHON_BIN -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" \
  || { echo "错误: $PYTHON_BIN 未安装 pyspark，请先: $PYTHON_BIN -m pip install pyspark"; exit 1; }

export PYSPARK_PYTHON="$(command -v "$PYTHON_BIN" 2>/dev/null || echo "$PYTHON_BIN")"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

# 把当前解释器的用户 site-packages 放前面（不写死用户名/版本号）
USER_SITE="$($PYTHON_BIN -c 'import site; print(site.getusersitepackages())' 2>/dev/null || true)"
if [[ -n "$USER_SITE" ]]; then
  export PYTHONPATH="${USER_SITE}${PYTHONPATH:+:$PYTHONPATH}"
fi

cd "$SCRIPT_DIR"
REQ="$PROJECT_ROOT/requirements.txt"
if ! "$PYTHON_BIN" -c "import transformers, torch" 2>/dev/null; then
  echo "错误: $PYTHON_BIN 未安装 transformers / torch（情感分析需要）。"
  echo "  请执行:  $PYTHON_BIN -m pip install -r \"$REQ\""
  echo "  或指定已安装依赖的解释器:  export PYTHON_BIN=/你的/python3  再运行本脚本。"
  exit 1
fi

mkdir -p logs

# ========== 关系图参数（按模式自动设置） ==========
GRAPH_TRIGGER_INTERVAL="${GRAPH_TRIGGER_INTERVAL:-10 seconds}"

if [[ "$MODE" == "auto" ]]; then
  EDGE_COUNT="$($PYTHON_BIN - <<'PY'
from enron_common import safe_count, ES_INDEX_EDGES
print(safe_count(ES_INDEX_EDGES))
PY
)"
  EDGE_COUNT="${EDGE_COUNT//$'\n'/}"
  if [[ "${EDGE_COUNT:-0}" =~ ^[0-9]+$ ]] && [[ "$EDGE_COUNT" -eq 0 ]]; then
    MODE="bootstrap"
    echo "自动模式: 检测到当前关系边为 0，切换为 bootstrap 首次回放模式。"
  else
    MODE="normal"
    echo "自动模式: 已存在关系边数据，切换为 normal 实时模式。"
  fi
fi

if [[ "$MODE" == "bootstrap" ]]; then
  GRAPH_STARTING_OFFSETS="earliest"
  GRAPH_RECOMPUTE_INTERVAL="${GRAPH_RECOMPUTE_INTERVAL:-1}"
  GRAPH_CHECKPOINT_DIR="${GRAPH_CHECKPOINT_DIR:-checkpoints/graph_stream_bootstrap_$(date +%Y%m%d_%H%M%S)}"
else
  GRAPH_STARTING_OFFSETS="latest"
  GRAPH_RECOMPUTE_INTERVAL="${GRAPH_RECOMPUTE_INTERVAL:-10}"
  GRAPH_CHECKPOINT_DIR="${GRAPH_CHECKPOINT_DIR:-checkpoints/graph_stream}"
fi

mkdir -p "$GRAPH_CHECKPOINT_DIR"

"$PYTHON_BIN" -u stream_enron_sentiment.py 2>&1 | tee logs/sentiment.log &
PID1=$!

RUN_GRAPH_METRICS="${RUN_GRAPH_METRICS:-0}"
if [[ "$RUN_GRAPH_METRICS" == "1" ]]; then
  "$PYTHON_BIN" -u build_email_relationship_graph.py \
    --starting-offsets "$GRAPH_STARTING_OFFSETS" \
    --checkpoint-dir "$GRAPH_CHECKPOINT_DIR" \
    --recompute-interval "$GRAPH_RECOMPUTE_INTERVAL" \
    --trigger-interval "$GRAPH_TRIGGER_INTERVAL" \
    2>&1 | tee logs/graph.log &
  PID2=$!
else
  echo "RUN_GRAPH_METRICS=0：跳过 GraphFrames 指标流，仅保留实时关系边与平均情感。" | tee logs/graph.log
  PID2=""
fi

"$PYTHON_BIN" -m streamlit run streamlit_app.py 2>&1 | tee logs/streamlit.log &
PID3=$!

echo "============================================"
echo "  模式 MODE: $MODE"
echo "  SPARK_HOME (与 PySpark 一致): $SPARK_HOME"
echo "  情感分析  PID: $PID1"
echo "  关系图    PID: ${PID2:-未启动（RUN_GRAPH_METRICS=0）}"
echo "  Streamlit PID: $PID3"
echo "  关系图参数: startingOffsets=$GRAPH_STARTING_OFFSETS"
echo "  关系图参数: checkpoint=$GRAPH_CHECKPOINT_DIR"
echo "  关系图参数: recomputeInterval=$GRAPH_RECOMPUTE_INTERVAL"
echo "  关系图参数: triggerInterval=$GRAPH_TRIGGER_INTERVAL"
echo "  浏览器打开 http://localhost:8501"
echo "  按 Ctrl+C 停止所有服务"
echo "  说明: 三个进程独立后台运行；某一个崩溃后，另外两个仍会继续，请查看对应 logs/*.log"
echo "============================================"

cleanup() {
  kill "$PID1" "$PID3" 2>/dev/null || true
  if [[ -n "${PID2:-}" ]]; then
    kill "$PID2" 2>/dev/null || true
  fi
}

trap cleanup SIGINT SIGTERM
wait

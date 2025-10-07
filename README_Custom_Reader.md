# ⚡ Ethereum Streaming Source for Apache Spark

A custom **PySpark Structured Streaming DataSource** that ingests live **Ethereum blockchain data** directly from an RPC endpoint (e.g., Infura or Alchemy) into Spark or Databricks.  
Designed for scalable, fault-tolerant blockchain ingestion, transformation, and analytics.

---

## 🚀 Features

- 🧩 Custom `DataSource` + `DataSourceStreamReader` for Ethereum blocks  
- 🔁 Supports **continuous ingestion** using offset tracking  
- ⚙️ Configurable batch size, rate limiting, and output directory  
- 🧠 Parallel block fetching via Spark partitions  
- 🪵 Detailed logging and checkpointing  
- 💾 Outputs structured block metadata:
  - `block_number`
  - `block_hash`
  - `timestamp`
  - `tx_count`

---

## 🏗️ Architecture Overview

+-----------------------------+
| EthereumDataSource |
| └── Defines schema & reader|
+-------------+---------------+
|
v
+-----------------------------+
| EthereumStreamReader |
| ├── initialOffset() |
| ├── latestOffset() |
| ├── planPartitions() |
| ├── read(partition) |
| └── commit() |
+-------------+---------------+
|
v
+-----------------------------+
| BlockRangePartition |
| → start_block..end_block |
+-----------------------------+


Each micro-batch:
1. Reads the **latest committed block**  
2. Fetches the **latest block number from the chain**  
3. Divides that range into partitions of size `batch_size`  
4. Each partition executes as a **Spark task**  
5. Streams structured block data downstream

---

## ⚙️ Configuration Options

| Option | Description | Default |
|:-------|:-------------|:---------|
| `provider_uri` | Ethereum RPC endpoint (Infura, Alchemy, etc.) | **Required** |
| `start_block` | Starting block number | `1` |
| `batch_size` | Number of blocks per Spark partition | `10` |
| `output_dir` | Directory for saving fetched JSON files | `dbfs:/Volumes/web3_data/ethereum_mainnet/ethereum_blocks/raw` |
| `max_calls_per_second` | Rate limit for Ethereum RPC calls | `0.8` |

---

## 🧩 Schema

| Column | Type | Description |
|:--------|:------|:-------------|
| `block_number` | Long | Block height |
| `block_hash` | String | Block hash (hex) |
| `timestamp` | Long | Unix timestamp |
| `tx_count` | Long | Number of transactions |

---

## 📦 Installation

1. Copy the Python file into your Databricks workspace or local repo:  
2. Make sure dependencies are available:
```bash
pip install web3 pyspark
from pyspark.sql import SparkSession
from ethereum_stream_source import EthereumDataSource

spark = SparkSession.builder.getOrCreate()

options = {
    "provider_uri": "https://mainnet.infura.io/v3/<YOUR_INFURA_KEY>",
    "start_block": "23500000",
    "batch_size": "10",
    "output_dir": "dbfs:/Volumes/web3_data/ethereum_mainnet/ethereum_blocks/raw",
    "max_calls_per_second": "0.8"
}

df = (
    spark.readStream
        .format("ethereum")
        .options(**options)
        .load()
)

(
    df.writeStream
      .format("parquet")
      .option("path", "dbfs:/Volumes/web3_data/ethereum_mainnet/ethereum_output")
      .option("checkpointLocation", "dbfs:/Volumes/web3_data/ethereum_mainnet/checkpoints")
      .start()
)


Logging

Each run logs:

Connection status to Ethereum provider

Offset and partition details

Rate-limiting sleeps

Block fetch success/fail counts

Checkpoint commits

Logs are visible both in the Databricks console and Spark driver logs.

dbfs:/Volumes/web3_data/ethereum_mainnet/
│
├── ethereum_blocks/raw/
│   ├── block_23500001.json
│   ├── block_23500002.json
│   └── ...
│
├── ethereum_output/
│   ├── part-00000-*.snappy.parquet
│   └── ...
│
└── checkpoints/

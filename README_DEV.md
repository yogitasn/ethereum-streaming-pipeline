# Ethereum Streaming – Developer Guide

This document provides **developer-focused instructions** for extending the Ethereum streaming project with a **custom Spark Structured Streaming source**.

It is intended for contributors who want to experiment with direct streaming ingestion instead of file-based JSON producers.

---

## 📂 Project Layout

```
ethereum_streaming/
│── blocks_producer.py        # JSON producer (default mode)
│── ethereum_custom.py        # Custom Spark reader (developer mode)
│── README.md                 # User overview
│── README_DEV.md             # Developer guide (this file)
```

---

## 🔧 Custom Reader Design

The **custom reader** implements a `DataSourceStreamReader` that polls Ethereum for new blocks, converts them into rows, and streams them directly into Spark.

This avoids intermediate JSON files but requires Databricks runtime support for custom sources.

### Core Components

* `EthereumStreamReader` → polls Ethereum every *N* seconds
* `schema` → defines the DataFrame structure
* `options` → provider URI + poll interval

---

## ⚙️ Prerequisites

* Python 3.9+
* Spark 3.5+ (local or Databricks cluster)
* Infura API key → [Get API key](https://infura.io/)
* Dependencies:

  ```bash
  pip install web3 pyspark
  ```

---

## 🧩 Example Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("number", LongType()),
    StructField("hash", StringType()),
    StructField("miner", StringType()),
    StructField("timestamp", LongType()),
    StructField("transactions", StringType())
])
```

---

## 🚀 Usage: Local Test

Run the custom reader locally before moving to Databricks.

```python
df = (spark.readStream
    .format("ethereum_custom")
    .option("provider_uri", "https://mainnet.infura.io/v3/YOUR_INFURA_KEY")
    .option("poll_interval", "10")
    .schema(schema)
    .load())

query = (df.writeStream
    .format("console")     # Print rows to console for testing
    .outputMode("append")
    .start())

query.awaitTermination()
```

Expected console output (every 10s):

```
+----------+------------------------------------+-----------------+----------+-------------------+
| number   | hash                               | miner           | timestamp| transactions      |
+----------+------------------------------------+-----------------+----------+-------------------+
| 18345678 | 0xabc123...                        | 0xdef456...     | 16940000 | [{"hash":"..."}] |
+----------+------------------------------------+-----------------+----------+-------------------+
```

---

## 🚀 Usage: Databricks

On Databricks, instead of writing to console, write to **Delta + Volumes**:

```python
(df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/Volumes/ethereum_catalog/checkpoints/blocks/")
   .start("/Volumes/ethereum_catalog/bronze/blocks_delta/"))
```

⚠️ **Note:** Custom sources (`ethereum_custom`) are not supported on Databricks Community Edition or shared clusters. Use the **file-based producer** (`blocks_producer.py`) if you hit this limitation.

---

## 📜 Development Notes

* The custom reader polls Infura every `poll_interval` seconds (default = 10).
* Blocks are read sequentially to avoid gaps.
* Transactions are stored as serialized JSON strings for simplicity (can be normalized later).
* Future enhancements:

  * Add **schema for transactions** (instead of dumping JSON)
  * Add **error handling** for Infura rate limits
  * Support **parallel block fetches** for higher throughput



Would you like me to also generate a **sample `ethereum_custom.py` file** (a working class you can drop into your repo to test the custom reader), so this second README points to actual code?

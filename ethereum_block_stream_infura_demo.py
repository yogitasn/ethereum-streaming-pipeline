# Databricks notebook source
# MAGIC %md
# MAGIC # Ethereum Blockchain Streaming with Chainstack
# MAGIC
# MAGIC ## 🔗 Overview
# MAGIC
# MAGIC This notebook streams Ethereum blockchain data using **Chainstack** as the RPC provider and writes to **Delta Lake** tables in **Databricks Unity Catalog**.
# MAGIC
# MAGIC ## 📋 Prerequisites
# MAGIC
# MAGIC 1. **Chainstack Account**: Get your API endpoint from [Chainstack Console](https://console.chainstack.com/)
# MAGIC 2. **API Key**: Stored in `.env` file as `CHAINSTACK_API_KEY`
# MAGIC 3. **Databricks Cluster**: Running with Spark 3.x or later
# MAGIC
# MAGIC ## 🚀 Quick Start
# MAGIC
# MAGIC ### 1. Configure Chainstack Endpoint
# MAGIC
# MAGIC In **Cell 3**, update the `CHAINSTACK_ENDPOINT` variable with your actual Chainstack node endpoint:
# MAGIC
# MAGIC ```python
# MAGIC # Get this from your Chainstack dashboard
# MAGIC CHAINSTACK_ENDPOINT = "https://nd-XXX-XXX-XXX.p2pify.com"  # ← Your endpoint here
# MAGIC ```
# MAGIC
# MAGIC ### 2. Set API Key
# MAGIC
# MAGIC The API key is loaded from environment variables:
# MAGIC - **Local development**: Store in `.env` file as `CHAINSTACK_API_KEY`
# MAGIC - **Databricks**: Set as environment variable or use Databricks Secrets
# MAGIC
# MAGIC ### 3. Run Cells Sequentially
# MAGIC
# MAGIC Execute cells in order:
# MAGIC 1. **Cell 1**: Install web3 library
# MAGIC 2. **Cell 2**: Configure Unity Catalog (catalog, schema, volumes)
# MAGIC 3. **Cell 3**: Define custom Ethereum data source
# MAGIC 4. **Cell 4**: Start streaming pipeline
# MAGIC 5. **Cell 5**: Query results
# MAGIC
# MAGIC ## 📊 What This Does
# MAGIC
# MAGIC - ✅ Connects to Ethereum mainnet via Chainstack
# MAGIC - ✅ Streams blocks in micro-batches (configurable)
# MAGIC - ✅ Writes to Unity Catalog Delta table: `{catalog}.{schema}.ethereum_blocks`
# MAGIC - ✅ Handles retries and failures gracefully
# MAGIC - ✅ Checkpoint-based recovery for fault tolerance
# MAGIC
# MAGIC ## 🔧 Configuration Options
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |-----------|---------|-------------|
# MAGIC | `start_block` | Latest | Starting Ethereum block number |
# MAGIC | `shuffle_partitions` | 16 | Parallel tasks |
# MAGIC | `max_retries` | 3 | Retry attempts per block |
# MAGIC
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC - [Chainstack Docs](https://docs.chainstack.com/)
# MAGIC - [Databricks Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC

# COMMAND ----------

# MAGIC %pip install web3 python-dotenv

# COMMAND ----------

from web3 import Web3



# === SIMPLE PARAMETERIZATION (VARIABLES FOR REUSABILITY) ===
dbutils.widgets.text("catalog_name", "eth_blockchain_demo", "Catalog Name")
dbutils.widgets.text("schema_name", "infura", "Schema Name")
dbutils.widgets.text("max_calls_per_second","20","Max calls/sec")
dbutils.widgets.text("s3_managed_bucket","ethereum-streaming-bucket","S3 Managed Bucket")

# === CONFIGURATION ===
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
S3_MANAGED_BUCKET = dbutils.widgets.get('s3_managed_bucket')
MAX_CALLS_PER_SECOND = int(dbutils.widgets.get("max_calls_per_second"))


# Unity Catalog paths
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/ethereum_checkpoints"
OUTPUT_TABLE = f"{CATALOG}.{SCHEMA}.ethereum_blocks"

print(f"🔧 Using Catalog: {CATALOG}, Schema: {SCHEMA}")
print(f"⚡ Rate Limit: {MAX_CALLS_PER_SECOND} req/sec (shuffle_partitions)")
print(f"📁 Checkpoints: {CHECKPOINT_VOLUME}")
print(f"📊 Output Table: {OUTPUT_TABLE}")
print(f"")
print(f"💡 Python API Note:")
print(f"   - Microbatch size is uncontrolled (first batch may be large)")
print(f"   - Real bottleneck: RPC rate limit (Spark handles data volume)")
print(f"   - For strict batch control: use Scala implementation")

# === UNITY CATALOG SETUP ===
stmts = [
    f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION 's3://{S3_MANAGED_BUCKET}/'",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_checkpoints",
]

for i, s in enumerate(stmts, 1):
    print(f"[{i}/{len(stmts)}] {s}")
    try:
        spark.sql(s)
        print("  ✅ Success")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# COMMAND ----------

"""
=============================================================================
ETHEREUM BLOCKCHAIN STREAMING DATA SOURCE FOR APACHE SPARK
=============================================================================

⚠️ PYTHON API LIMITATION - Microbatch Size Control:
====================================================
This Python implementation cannot control microbatch size due to PySpark's
simplified DataSource API.

PRACTICAL IMPACT:
-----------------
✓ **If starting from chain tip**: Works perfectly fine
✓ **If catching up from old block**: First batch processes ALL outstanding blocks
  - Example: 1 million blocks behind → first batch processes 1M blocks
  - Subsequent batches: Near real-time (only new blocks since last batch)
  
✓ **Real bottleneck**: Your RPC provider's rate limit, not Spark
  - Spark easily handles millions of rows
  - API rate limiting (via shuffle_partitions) is the actual constraint

✗ **Not suitable for**: Strict incremental processing requirements
  - If you MUST process exactly N blocks per batch
  - Use Scala implementation for this use case

PYTHON vs SCALA API:
--------------------
Python:  latestOffset(self) -> dict
         ❌ No parameters to control batch size

Scala:   latestOffset(startOffset: Offset, limit: ReadLimit): Offset
         ✅ Receives current position and max rows limit
         https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/streaming/SupportsAdmissionControl.html

FOR PRODUCTION WITH STRICT BATCH CONTROL:
------------------------------------------
Use the Scala implementation:
  📄 src/main/scala/com/ethereum/spark/sql/EthereumMicroBatchStream.scala

DOCUMENTATION:
--------------
  📖 useful_ref/PYTHON_VS_SCALA_DATASOURCE_API_LIMITATION.md - Technical details
  📖 useful_ref/HOW_TO_FIX_PYTHON_API_IN_SPARK.md - How to contribute fix
  📖 useful_ref/PYSPARK_DATASOURCE_API_REFERENCE.md - Python API reference

This Python version is suitable for:
  ✓ Development/testing
  ✓ Production if starting near chain tip
  ✓ Catch-up scenarios (first batch will be large, then normal)
  ✗ Strict incremental processing (exactly N blocks per batch)
"""

import time, logging
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import Row
from web3 import Web3
from dataclasses import dataclass

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

@dataclass
class BlockRangePartition(InputPartition):
    """Partition descriptor with [start, end) half-open interval."""
    start: int
    end: int

class EthereumStreamReader(DataSourceStreamReader):
    """
    Ethereum Blockchain Streaming Reader - Spark DataSource V2 API Implementation
    
    SPARK'S STREAMING PROTOCOL (what Spark provides and expects):
    ==============================================================
    
    Spark Structured Streaming manages the streaming lifecycle and calls these methods
    in a specific order for each microbatch:
    
    LIFECYCLE FOR NEW QUERY (no checkpoint):
    1. Spark creates EthereumStreamReader instance
    2. Spark calls: initialOffset() → YOU provide starting offset
    3. Spark calls: latestOffset() → YOU provide current available data
    4. Spark calls: partitions(start_offset, end_offset) → YOU create partitions
       - start_offset: from initialOffset() or last commit
       - end_offset: from latestOffset()
    5. Spark distributes partitions to executors
    6. Executors call: read(partition) → YOU return data rows
    7. Spark calls: commit(end_offset) → YOU can do cleanup (optional)
    8. Spark saves end_offset to checkpoint (managed by Spark, not you)
    
    LIFECYCLE FOR RESTARTED QUERY (has checkpoint):
    1. Spark creates EthereumStreamReader instance
    2. Spark reads last committed offset from checkpoint
    3. Spark calls: latestOffset() → YOU provide current available data
    4. Spark calls: partitions(checkpoint_offset, latest_offset) → YOU create partitions
       - checkpoint_offset: from Spark's saved checkpoint
       - latest_offset: from your latestOffset()
    5-8. Same as above
    
    WHAT SPARK PROVIDES TO YOU:
    ===========================
    - Checkpoint management: Spark saves/loads offsets automatically
    - Fault tolerance: If job fails, Spark restarts from last checkpoint
    - Exactly-once semantics: Spark ensures each offset range processed once
    - Distributed execution: Spark distributes your partitions to executors
    - Trigger control: Spark manages microbatch timing (availableNow, processingTime, etc.)
    
    WHAT YOU MUST PROVIDE TO SPARK:
    ================================
    - initialOffset(): Starting point for new queries
    - latestOffset(): Current data availability (chain tip in our case)
    - partitions(): How to split work (offset ranges in our case)
    - read(): Actual data fetching logic (RPC calls in our case)
    - commit(): Optional cleanup (we use pass)
    
    KEY SPARK BEHAVIORS:
    ====================
    - Spark treats offsets as opaque dictionaries (you define structure)
    - Spark uses exclusive upper bound: [start, end) means process start, not end
    - Spark calls latestOffset() frequently (even without new data)
    - Spark may create multiple reader instances (don't rely on instance state)
    - Spark manages parallelism via shuffle_partitions config
    """
    def __init__(self, schema, options):
        self.options = options
        
        # Required parameters
        if "provider_uri" not in options:
            raise ValueError("provider_uri is required")
        self.provider_uri = options.get("provider_uri")
        
        if "start_block" not in options:
            raise ValueError("start_block is required")
        # Parse configuration
        self.start_block = int(options.get("start_block"))
        if self.start_block < 0:
            raise ValueError(f"start_block must be >= 0, got {self.start_block}")
        self.shuffle_partitions = int(options.get("shuffle_partitions", 16))
        if self.shuffle_partitions < 1:
            raise ValueError(f"shuffle_partitions must be >= 1, got {self.shuffle_partitions}")
        
        self.max_retries = int(options.get("max_retries", 3))
        if self.max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {self.max_retries}")
        
        self.retry_base_delay = float(options.get("retry_base_delay", 1.0))
        if self.retry_base_delay < 0:
            raise ValueError(f"retry_base_delay must be >= 0, got {self.retry_base_delay}")
        logger.info(f"🚀 Initialized EthereumStreamReader:")
        logger.info(f"   - start_block={self.start_block}")
        logger.info(f"   - ⚠️  Microbatch size: UNCONTROLLED (Python API limitation)")
        logger.info(f"   - For production with rate limiting: use Scala implementation")
        logger.info(f"   - shuffle_partitions={self.shuffle_partitions}")

    def initialOffset(self):
        """
        Return starting offset for NEW streaming queries (no checkpoint exists).
        
        WHEN SPARK CALLS THIS:
        ----------------------
        - Only for brand new queries (first time running)
        - NOT called if checkpoint directory already exists
        - Called ONCE per query initialization
        
        WHAT SPARK PROVIDES:
        --------------------
        - Nothing (no parameters)
        - You decide the starting point
        
        WHAT SPARK EXPECTS:
        -------------------
        - Return: dict with your offset structure
        - Must be JSON-serializable
        - Should represent "where to start reading"
        
        OUR IMPLEMENTATION:
        -------------------
        - Returns start_block from configuration
        - This becomes the first checkpoint if query succeeds
        
        Example:
          User starts query with start_block=1000
          → Spark calls initialOffset()
          → We return {"offset": 1000}
          → Spark processes [1000, latestOffset)
        """
        logger.info(f"📍 initialOffset() -> {self.start_block}")
        return {"offset": self.start_block}

    def latestOffset(self):
        """
        Query Ethereum and return the latest available block from the blockchain.
        
        WHEN SPARK CALLS THIS:
        ----------------------
        - Called FREQUENTLY (every microbatch trigger)
        - Called BEFORE partitions() for each batch
        - Called on DRIVER node (not executors)
        - May be called even when no new data exists
        
        WHAT SPARK PROVIDES:
        --------------------
        - Nothing (no parameters)
        - You decide what "latest" means for your data source
        
        WHAT SPARK EXPECTS:
        -------------------
        - Return: dict (your offset structure)
        - Must be JSON-serializable
        - Should be monotonically increasing (or stay same)
        - Represents "current data availability"
        
        WHAT SPARK DOES WITH YOUR RETURN VALUE:
        ========================================
        - Uses it as end_offset parameter in partitions(start_offset, end_offset)
        - Saves it to checkpoint after successful batch processing
        - On restart, this becomes the start_offset for the next batch
        
        ⚠️ PYTHON API LIMITATION:
        =========================
        This method returns the ACTUAL CHAIN TIP (highest block number available).
        There is NO way to control batch size in Python's DataSource API.
        
        PRACTICAL IMPACT:
        -----------------
        - First batch: Processes ALL blocks from checkpoint to chain tip
        - Subsequent batches: Only new blocks since last batch
        - Real bottleneck: RPC API rate limit (Spark handles data volume easily)
        
        Example scenario (catching up from old block):
          Batch 0: checkpoint=0, chain=1,000,000 → processes 1M blocks (slow)
          Batch 1: checkpoint=1,000,000, chain=1,000,010 → processes 10 blocks (fast)
          Batch 2: checkpoint=1,000,010, chain=1,000,015 → processes 5 blocks (fast)
          ...continuing near real-time
        
        Example scenario (starting from chain tip):
          Batch 0: checkpoint=1,000,000, chain=1,000,005 → processes 5 blocks ✓
          Batch 1: checkpoint=1,000,005, chain=1,000,012 → processes 7 blocks ✓
          ...all batches are small
        
        THE MISSING API IN PYTHON:
        ---------------------------
        Python:  latestOffset(self) -> dict  
                 ❌ NO PARAMETERS - can't see current position or limit batch size!
        
        Scala:   latestOffset(startOffset: Offset, limit: ReadLimit): Offset
                 ✅ Receives current position and max rows limit
        
        If Python had these parameters, we could return:
          min(startOffset + limit, chain_latest)
        
        🔗 Reference:
        https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/streaming/SupportsAdmissionControl.html
        
        FOR STRICT BATCH CONTROL:
        --------------------------
        Use Scala if you need exactly N blocks per batch:
          - src/main/scala/com/ethereum/spark/sql/EthereumMicroBatchStream.scala
          - useful_ref/PYTHON_VS_SCALA_DATASOURCE_API_LIMITATION.md
        """
        w3 = Web3(Web3.HTTPProvider(self.provider_uri))
        chain_latest = w3.eth.block_number
        
        logger.info(f"📊 latestOffset():")
        logger.info(f"   - Returning chain tip: {chain_latest:,}")
        
        return {"offset": chain_latest}

    def partitions(self, start_offset, end_offset):
        """
        Create partitions for parallel block processing.
        
        WHEN SPARK CALLS THIS:
        ----------------------
        - AFTER latestOffset() returns
        - Called on DRIVER node (not executors)
        - Called once per microbatch
        - Determines parallelism for this batch
        
        WHAT SPARK PROVIDES:
        --------------------
        - start_offset: dict
          * For NEW query: from initialOffset()
          * For RESTARTED query: from checkpoint file
          * Represents: "where we left off"
        - end_offset: dict  
          * From latestOffset() (just called)
          * Represents: "current data availability"
        
        WHAT SPARK EXPECTS:
        -------------------
        - Return: List[InputPartition]
        - Each partition is a serializable object
        - Partitions will be distributed to executors
        - Each partition processed by calling read(partition)
        - Empty list is valid (no data to process)
        
        OFFSET SEMANTICS (IMPORTANT):
        -----------------------------
        - start_offset["offset"]: INCLUSIVE (first block to process)
        - end_offset["offset"]: EXCLUSIVE (NOT processed in this batch)
        - Range notation: [start, end)
        
        Example: start=100, end=110
          → Process blocks: 100, 101, 102, 103, 104, 105, 106, 107, 108, 109
          → Do NOT process block 110 (it's the start of the next batch)
        
        PARALLELISM CONTROL:
        --------------------
        - Number of partitions = number of parallel tasks
        - More partitions = more parallelism (but more overhead)
        - shuffle_partitions config controls this
        - Each partition will call read() on an executor
        
        BATCH SIZE IN PYTHON API:
        -------------------------
        Batch size = end - start (uncontrolled in Python API)
        
        - Large first batch? Normal if catching up from old checkpoint
        - Small batches after? Normal for near real-time operation
        - Bottleneck: Your RPC provider's rate limit (not Spark)
        
        For strict batch size control (exactly N blocks per batch):
          → Use Scala implementation at src/main/scala/.../EthereumMicroBatchStream.scala
        
        OUR IMPLEMENTATION:
        -------------------
        - Calculate total blocks to process: end - start
        - Divide into shuffle_partitions equal chunks
        - Create BlockRangePartition objects with [start, end) ranges
        - Return list for Spark to distribute
        
        Example execution:
          start=1000, end=1100, shuffle_partitions=4
          → Partition 1: [1000, 1025)  → Executor A
          → Partition 2: [1025, 1050)  → Executor B  
          → Partition 3: [1050, 1075)  → Executor C
          → Partition 4: [1075, 1100)  → Executor D
        """
        start = start_offset["offset"]
        end = end_offset["offset"]  # EXCLUSIVE
        
        total_blocks = end - start
        
        logger.info(f"🔧 partitions() called:")
        logger.info(f"   - Range: [{start}, {end})")
        logger.info(f"   - Total blocks: {total_blocks:,}")
        
        if total_blocks == 0:
            logger.warning("⚠️  No blocks to process (caught up with chain tip)")
            return []
        
        if total_blocks > 1000:
            logger.info(f"📊 Large batch detected: {total_blocks:,} blocks")
            logger.info(f"   This is normal for initial catch-up")
            logger.info(f"   Subsequent batches will be smaller (near real-time)")
        
        blocks_per_partition = max(1, total_blocks // self.shuffle_partitions)
        
        logger.info(f"   - Shuffle partitions: {self.shuffle_partitions}")
        logger.info(f"   - Blocks per partition: ~{blocks_per_partition:,}")
        
        # Create partitions with half-open intervals [start, end)
        partitions = []
        for i in range(start, end, blocks_per_partition):
            partition_end = min(i + blocks_per_partition, end)
            partitions.append(BlockRangePartition(i, partition_end))
            logger.debug(f"   - Partition: [{i}, {partition_end})")
        
        logger.info(f"   Created {len(partitions)} partitions")
        
        return partitions

    def read(self, partition):
        """
        Fetch actual blockchain data for a given partition.
        
        WHEN SPARK CALLS THIS:
        ----------------------
        - AFTER partitions() returns partition objects
        - Called IN PARALLEL on executor nodes (not driver)
        - Each executor processes one or more partitions
        - May be called MULTIPLE TIMES for same partition (retry on failure)
        
        WHAT SPARK PROVIDES:
        --------------------
        - partition: InputPartition object YOU created in partitions()
        - In our case: BlockRangePartition(start_block, end_block)
        - Spark has already distributed this partition to an executor
        
        WHAT SPARK EXPECTS:
        -------------------
        - Return: Iterator[Row] (use yield or return iter([...]))
        - Rows must match the schema YOU defined in EthereumDataSource
        - Must be deterministic (same partition → same data)
        - EXCLUSIVE upper bound: process [start, end) not including end
        
        SPARK'S EXECUTION MODEL:
        ------------------------
        - Runs in executor JVM/process (not driver)
        - Has access to executor resources (CPU, memory)
        - Multiple partitions may run on same executor
        - Spark handles task scheduling and retry on failure
        
        OUR IMPLEMENTATION:
        -------------------
        - Extracts [start_block, end_block) from partition
        - Makes RPC call for each block in range
        - Implements rate limiting (1 req/sec per executor)
        - Returns iterator of Row objects
        
        Example Spark execution:
          Partition 1: BlockRangePartition(100, 110) → Executor A
          Partition 2: BlockRangePartition(110, 120) → Executor B
          Partition 3: BlockRangePartition(120, 130) → Executor A
          
          Executor A processes partitions 1 & 3 sequentially
          Executor B processes partition 2 in parallel
        """
        
        w3 = Web3(Web3.HTTPProvider(self.provider_uri))
        rows = []
        target_delay = 1.0  # 1 request per second per partition
        
        # Half-open interval: [start, end) - end is EXCLUSIVE
        for blk_num in range(partition.start, partition.end):
            start_time = time.time()
            
            for attempt in range(self.max_retries):
                try:
                    block = w3.eth.get_block(blk_num, full_transactions=False)
                    
                    rows.append(Row(
                        block_number=block.number,
                        block_hash=block.hash.hex(),
                        timestamp=block.timestamp,
                        tx_count=len(block.transactions)
                    ))
                    
                    break  # Success
                    
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        delay = self.retry_base_delay * (2 ** attempt)
                        time.sleep(delay)
                    else:
                        raise RuntimeError(
                            f"Cannot fetch block {blk_num} after {self.max_retries} retries. "
                            f"Error: {e}"
                        )
            
            # Rate limiting
            elapsed = time.time() - start_time
            if elapsed < target_delay:
                time.sleep(target_delay - elapsed)
        
        total = partition.end - partition.start
        return iter(rows)

    def commit(self, end_offset):
        """
        Called by Spark AFTER successfully processing a microbatch.
        
        WHEN SPARK CALLS THIS:
        ----------------------
        - AFTER all partitions successfully processed
        - Used to signal successful batch completion
        - Called on DRIVER node (not executors)
        - NOT called if microbatch fails
        
        WHAT SPARK PROVIDES:
        --------------------
        - end_offset: dict that was returned by latestOffset()
        - Same offset that Spark will save to checkpoint
        - Guaranteed to be called only once per successful batch
        
        WHAT SPARK EXPECTS:
        -------------------
        - Optional method (can be empty/pass)
        - Use for cleanup, metrics, external state tracking
        - Should be fast (blocks next batch)
        - Don't throw exceptions (causes query failure)

        
        YOU SHOULD NOT:
        ---------------
        - Manually save offsets (Spark does this)
        - Track state in instance variables (lost on restart)
        - Perform heavy computation (blocks streaming)
        
        TYPICAL USE CASES:
        ------------------
        - Update external progress tracker
        - Emit metrics/monitoring data
        - Clean up temporary resources
        - Update database with progress
        
        OUR IMPLEMENTATION:
        -------------------
        - Just pass (Spark's checkpoint is sufficient)
        - We don't need external state tracking
        
        Example flow:
          1. latestOffset() returns {"offset": 1000}
          2. Spark processes [900, 1000)
          3. Spark calls commit({"offset": 1000})  ← YOU ARE HERE
          4. Spark saves {"offset": 1000} to checkpoint
          5. Next batch starts from 1000
        """
        pass  # Spark's checkpoint management is sufficient)

# === DATA SOURCE ENTRY POINT ===
class EthereumDataSource(DataSource):
    """
    Main entry point for Spark to access Ethereum blockchain data.
    Spark calls this to get the stream reader.
    """
    @classmethod
    def name(cls):
        return "ethereum"
    
    def schema(self):
        return "block_number LONG, block_hash STRING, timestamp LONG, tx_count LONG"
    
    def streamReader(self, schema):
        return EthereumStreamReader(schema, self.options)

# COMMAND ----------


import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from web3 import Web3
# ============================================================================
# CHAINSTACK CONFIGURATION (Benchmarked: 18-19 req/s sustained)
# ============================================================================
# Chainstack endpoint with API key (same structure as original Infura setup)
ETH_PROVIDER_URI = "https://mainnet.infura.io/v3/6504e6a7883c4b49ac1cf17099e2ea3a"

# Connect and test
w3 = Web3(Web3.HTTPProvider(ETH_PROVIDER_URI))
latest_block = w3.eth.block_number
# Lets start 200 offsets behind
start_block = latest_block - 200

print(f"✅ Connected to Infura!")
print(f"✅ Latest block on chain: {latest_block:,}")
print(f"✅ Starting from block: {start_block:,}")
print(f"📊 Benchmarked throughput: 18-19 req/s (parallel)")

# --- Spark session ---
spark = SparkSession.builder.appName("EthereumStream").getOrCreate()

eth_schema = StructType([
    StructField("block_number", LongType(), True),
    StructField("block_hash", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("tx_count", LongType(), True)
])


# Deterministic checkpoint path based on table name
# This ensures the query resumes from last processed block after restart
checkpoint_path = f"{CHECKPOINT_VOLUME}/ethereum_blocks_checkpoint/"
start_block = f"{start_block}"

print("✅ Using checkpoint:", checkpoint_path)
print("✅ Using output table:", OUTPUT_TABLE)
print("✅ Using start_block:", start_block)


spark.dataSource.register(EthereumDataSource)

# --- Read from custom Ethereum source ---
# shuffle_partitions matches the rate limit (max_calls_per_second)
# Each partition makes 1 req/sec, so total = shuffle_partitions × 1 req/sec
df = spark.readStream \
    .format("ethereum") \
    .schema(eth_schema) \
    .option("provider_uri", ETH_PROVIDER_URI) \
    .option("start_block", start_block) \
    .option("shuffle_partitions", str(MAX_CALLS_PER_SECOND)) \
    .option("max_retries", "3") \
    .load()

query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(OUTPUT_TABLE)

logger.info(f"Streaming query started, writing to table: {OUTPUT_TABLE}")


# COMMAND ----------

df = spark.read.table(OUTPUT_TABLE)

display(df)

# COMMAND ----------

# MAGIC %sql SELECT min(block_number), max(block_number) FROM eth_blockchain_demo.infura.ethereum_blocks
# MAGIC
# MAGIC -- min: 23665564, max: 23665764 -- 200

# COMMAND ----------

dbutils.fs.head('/Volumes/eth_blockchain_demo/infura/ethereum_checkpoints/ethereum_blocks_checkpoint/offsets/1')

# 23713806 - 23713782

# COMMAND ----------


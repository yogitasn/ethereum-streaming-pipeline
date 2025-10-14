# Databricks notebook source
# MAGIC %pip install web3

# COMMAND ----------

from web3 import Web3



# === SIMPLE PARAMETERIZATION (VARIABLES FOR REUSABILITY) ===
dbutils.widgets.text("catalog_name", "blockchain", "Catalog Name")
dbutils.widgets.text("schema_name", "ethereum", "Schema Name")
dbutils.widgets.text("poll_interval", "15", "Polling Interval (seconds)")
dbutils.widgets.text("microbatch_size", "10", "Blocks per Microbatch")
dbutils.widgets.text("s3_managed_bucket","ethereum-streaming-bucket","S3 Managed Bucket")
dbutils.widgets.text("max_calls_per_second","0.8","Max calls/sec")
dbutils.widgets.text("max_offset_per_trigger","100","Max Offset per Trigger")

dbutils.widgets.text("output_catalog_name", "blockchain_ethereum", "Output Catalog Name")
dbutils.widgets.text("output_schema_name", "bronze", "Output Schema Name")

# === CONFIGURATION ===
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
S3_MANAGED_BUCKET = dbutils.widgets.get('s3_managed_bucket')
POLL_INTERVAL = int(dbutils.widgets.get("poll_interval"))
MICROBATCH_SIZE = int(dbutils.widgets.get("microbatch_size"))
MAX_CALLS_PER_SECOND = dbutils.widgets.get("max_calls_per_second")
MAX_OFFSET_PER_TRIGGER = int(dbutils.widgets.get("max_offset_per_trigger"))

OUTPUT_CATALOG = dbutils.widgets.get("output_catalog_name")
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema_name")


# Unity Catalog volume paths
DATA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/blocks"
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
SCHEMA_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/schemas"
OUTPUT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/output"

OUTPUT_DATA_VOLUME = f"/Volumes/{OUTPUT_CATALOG}/{OUTPUT_SCHEMA}/ethereum_blocks"


print(f"🔧 Using Catalog: {CATALOG}, Schema: {SCHEMA}")
print(f"⏱ Poll Interval: {POLL_INTERVAL}s")
print(f"📦 Microbatch Size: {MICROBATCH_SIZE} blocks per batch")
print(f"📦 Max Offset : {MAX_OFFSET_PER_TRIGGER} per trigger")
print(f"📁 Data: {DATA_VOLUME}")
print(f"📁 Checkpoints: {CHECKPOINT_VOLUME}")
print(f"📁 Schemas: {SCHEMA_VOLUME}")
print(f"🔧 Using Output Catalog: {OUTPUT_CATALOG}, Output Schema: {OUTPUT_SCHEMA}")


# === UNITY CATALOG SETUP ===
stmts = [
    f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION 's3://{S3_MANAGED_BUCKET}/'",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_blocks",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_checkpoints",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_schemas",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ethereum_output",
    f"CREATE CATALOG IF NOT EXISTS {OUTPUT_CATALOG} MANAGED LOCATION 's3://{S3_MANAGED_BUCKET}/'",
    f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}",
  
]

for i, s in enumerate(stmts, 1):
    print(f"[{i}/{len(stmts)}] {s}")
    try:
        spark.sql(s)
        print("  ✅ Success")
    except Exception as e:
        print(f"  ❌ Error: {e}")


# COMMAND ----------


import os, json, time, asyncio, aiohttp, logging
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import Row
from web3 import AsyncWeb3, AsyncHTTPProvider
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp,col
from delta.tables import DeltaTable
from datetime import datetime

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)  # 👈 Send logs to notebook output
    ]
)
logger = logging.getLogger("EthereumStream")

# -----------------------------
# Partition class
# -----------------------------
class BlockRangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end
        logger.debug(f"Created partition: blocks {start} to {end}")

# -----------------------------
# Async Block Fetcher
# -----------------------------
async def fetch_block(aweb3, blk_num):
    """Fetch a single block asynchronously."""
    block = await aweb3.eth.get_block(blk_num, full_transactions=False)
    return {
        "block_number": block.number,
        "block_hash": block.hash.hex(),
        "timestamp": block.timestamp,
        "tx_count": len(block.transactions)
    }

async def fetch_blocks_concurrent(provider_uri, start, end, max_conc):
    """Fetch blocks in parallel with bounded concurrency."""
    async with aiohttp.ClientSession() as session:
        aweb3 = AsyncWeb3(AsyncHTTPProvider(provider_uri))  # remove session param
        sem = asyncio.Semaphore(max_conc)

        async def guarded(n):
            async with sem:
                return await fetch_block(aweb3, n)

        return await asyncio.gather(*(guarded(n) for n in range(start, end + 1)))

# -----------------------------
# StreamReader
# -----------------------------
class EthereumStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        logger.info("=" * 60)
        logger.info("Initializing EthereumStreamReader")
        logger.info("=" * 60)

        self.schema = schema
        self.options = options
        self.provider_uri = options.get("provider_uri")
        self.start_block = int(options.get("start_block", 1))
        self.output_dir = options.get("output_dir", "dbfs:/Volumes/blockchain/ethereum/ethereum_blocks/raw")
        self.enable_local_write = options.get("enable_local_write", "false").lower() == "true"
        self.max_concurrency = int(options.get("max_concurrency", 8))
        self.current_block = self.start_block
        self.last_committed_offset = None
    

        logger.info("Configuration:")
        logger.info(f"  - Provider URI: {self.provider_uri}")
        logger.info(f"  - Start block: {self.start_block}")
        logger.info(f"  - Output directory: {self.output_dir}")
        logger.info(f"  - Enable local write: {self.enable_local_write}")
        logger.info(f"  - Max concurrency: {self.max_concurrency}")
        logger.info("=" * 60)

    # -------------------------
    # Offsets
    # -------------------------
    def initialOffset(self):
        offset = {"offset": self.start_block}
        logger.info(f"initialOffset() -> {offset}")
        return offset


    def latestOffset(self):
        try:
            # Connect to Ethereum node
            w3 = Web3(Web3.HTTPProvider(self.provider_uri))
            if not w3.is_connected():
                raise RuntimeError("Failed to connect to Ethereum provider")

            chain_tip = w3.eth.block_number

            # Use maxOffsetPerTrigger
            max_offset = int(self.options.get("maxOffsetPerTrigger", 100))

            # Delta table for state management
            state_table = self.options.get(
                "offset_state_table",
                "blockchain_ethereum.bronze.eth_stream_state"
            )

            # Default starting offset
            last_committed = self.start_block

            # Read last committed offset from Delta table
            try:
                spark = SparkSession.getActiveSession()
                
                if spark is not None and spark.catalog.tableExists(state_table):
                    state_df = spark.table(state_table)
                    if state_df.count() > 0:
                        # Get max offset from state table
                        max_row = state_df.agg({"offset": "max"}).first()
                        if max_row and max_row[0] is not None:
                            last_committed = max_row[0]
                            logger.info(f"Retrieved last committed offset: {last_committed}")
            except Exception as e:
                logger.warning(f"Could not read state table: {e}")

            # Compute next offset range
            next_end = min(chain_tip, last_committed + max_offset)
            offset = {"offset": next_end}

            logger.info(
                f"latestOffset() -> chain_tip={chain_tip}, "
                f"last_committed={last_committed}, next_end={next_end}"
            )

            return offset

        except Exception as e:
            logger.error(f"Error in latestOffset(): {e}", exc_info=True)
            raise

    # -------------------------
    # Partition Planning
    # -------------------------
    def partitions(self, start_offset, end_offset):
        """Legacy method name - calls planPartitions internally"""
        return self.planPartitions(start_offset, end_offset)
    
    def planPartitions(self, start_offset, end_offset):
        start = start_offset.get("offset", self.start_block) if start_offset else self.start_block
        end = end_offset.get("offset", start + 1000) if end_offset else start + 1000
        step = int(self.options.get("batch_size", 10))

        partitions = [
            BlockRangePartition(i, min(i + step - 1, end))
            for i in range(start, end + 1, step)
        ]

        logger.info(f"Planned {len(partitions)} partitions: {start}-{end}, batch={step}")
        return partitions

    # -------------------------
    # Read Partition (Async)
    # -------------------------
    def read(self, partition):
        logger.info(f"Reading partition: {partition.start}-{partition.end}")
        blocks = asyncio.run(fetch_blocks_concurrent(
            self.provider_uri, partition.start, partition.end, self.max_concurrency
        ))

        for blk in blocks:
            if self.enable_local_write:
                # Atomic write: temp → rename
                tmp_path = os.path.join(self.output_dir, f"block_{blk['block_number']}.json.tmp")
                final_path = os.path.join(self.output_dir, f"block_{blk['block_number']}.json")
                with open(tmp_path, "w") as f:
                    json.dump(blk, f, indent=2)
                os.replace(tmp_path, final_path)
                logger.debug(f"Wrote {final_path}")

            yield Row(**blk)
        logger.info(f"Completed reading partition {partition.start}-{partition.end}")

    # -------------------------
    # Commit Checkpoint
    # -------------------------

    def commit(self, end_offset):
        """
        Commit is now handled in foreachBatch, so this is just for logging.
        """
        self.last_committed_offset = end_offset.get("offset")
        logger.info(f"commit() called with offset: {self.last_committed_offset}")
        logger.info("(State management is handled in foreachBatch)")
        # No actual work needed here since foreachBatch handles persistence

# -----------------------------
# DataSource Wrapper
# -----------------------------
class EthereumDataSource(DataSource):
    @classmethod
    def name(cls):
        return "ethereum"

    def schema(self):
        return StructType([
            StructField("block_number", LongType()),
            StructField("block_hash", StringType()),
            StructField("timestamp", LongType()),
            StructField("tx_count", LongType())
        ])

    def streamReader(self, schema):
        return EthereumStreamReader(schema, self.options)


# COMMAND ----------

import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import col, max as spark_max, lit
from web3 import Web3
from datetime import datetime

# Setup logger
logger = logging.getLogger("EthereumStream")

# Hard-coded constants
ETH_PROVIDER_URI = "https://mainnet.infura.io/v3/6504e6a7883c4b49ac1cf17099e2ea3a"

# Connect to Ethereum
w3 = Web3(Web3.HTTPProvider(ETH_PROVIDER_URI))
current_block = w3.eth.block_number
print(f"Latest Ethereum block: {current_block}")

# Get active Spark session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("EthereumStream").getOrCreate()

# Config
checkpoint_path = f"{CHECKPOINT_VOLUME}/"
eth_provider_uri = ETH_PROVIDER_URI
poll_interval = POLL_INTERVAL
state_table = "blockchain_ethereum.bronze.eth_stream_state"

print(f"✅ Checkpoint: {checkpoint_path}")
print(f"✅ State table: {state_table}")

# Create state table if it doesn't exist
print(f"\n📋 Initializing state table: {state_table}")
try:
    if not spark.catalog.tableExists(state_table):
        print(f"  Creating new state table: {state_table}")
        
        # Create schema
        state_schema = StructType([
            StructField("offset", LongType(), False),
            StructField("timestamp", StringType(), False),
            StructField("source_id", StringType(), False)
        ])
        
        # Create empty DataFrame and save as table
        empty_df = spark.createDataFrame([], state_schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(state_table)
        
        print(f"  ✅ State table created: {state_table}")
    else:
        print(f"  ✅ State table already exists: {state_table}")
except Exception as e:
    print(f"  ⚠️ Error creating state table: {e}")
    raise

# Determine start block from state table or use default
print(f"\n📍 Determining start block...")
try:
    if spark.catalog.tableExists(state_table):
        last_offset_row = spark.table(state_table) \
            .orderBy(col("timestamp").desc()) \
            .first()
        if last_offset_row:
            start_block = last_offset_row["offset"] + 1  # Start from next block
            print(f"✅ Resuming from block: {start_block}")
        else:
            start_block = current_block - 100  # Default: last 100 blocks
            print(f"✅ Starting from block: {start_block} (no previous state)")
    else:
        start_block = current_block - 100
        print(f"✅ State table not found. Starting from block: {start_block}")
except Exception as e:
    print(f"⚠️ Could not read state table: {e}")
    start_block = current_block - 100
    print(f"✅ Starting from block: {start_block}")

# Register datasource (assumes EthereumDataSource is already defined in the notebook)
print(f"\n🔌 Registering Ethereum data source...")
try:
    spark.dataSource.register(EthereumDataSource)
    print("✅ EthereumDataSource registered successfully")
except Exception as e:
    print(f"⚠️ Error registering data source: {e}")
    raise

# Read stream
print(f"\n📖 Creating read stream from block {start_block}...")
df = spark.readStream \
    .format("ethereum") \
    .option("provider_uri", eth_provider_uri) \
    .option("start_block", start_block) \
    .option("batch_size", 10) \
    .option("offset_state_table", state_table) \
    .option("maxOffsetPerTrigger", MAX_OFFSET_PER_TRIGGER) \
    .option("max_concurrency", 8) \
    .option("source_id", "ethereum_mainnet") \
    .load()

print("✅ Read stream created")

# Start streaming query
print("\n🚀 Starting streaming query...")
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable("blockchain_ethereum.bronze.blocks_json")

print("✅ Streaming query started. Awaiting termination...")

# Wait for completion with timeout
try:
    query.awaitTermination(timeout=300)  # 5 minute timeout
    print("✅ Query completed successfully")
except Exception as e:
    print(f"⚠️ Query terminated with exception: {e}")
    raise

# Show results
print("\n" + "="*60)
print("📊 RESULTS")
print("="*60)

# After streaming completes, update state table with max offset
print("\n📝 Updating state table...")
try:
    # Get the max block_number from the blocks table
    max_block_df = spark.table('blockchain_ethereum.bronze.blocks_json') \
        .select(spark_max("block_number").alias("offset")) \
        .withColumn("timestamp", lit(datetime.utcnow().isoformat())) \
        .withColumn("source_id", lit("ethereum_mainnet"))
    
    # Write to state table
    max_block_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(state_table)
    
    max_offset = max_block_df.select("offset").first()[0]
    print(f"✅ State table updated - committed offset: {max_offset}")
    
except Exception as e:
    print(f"⚠️ Error updating state table: {e}")

# Show blocks written
try:
    blocks_count = spark.table('blockchain_ethereum.bronze.blocks_json').count()
    print(f"\n✅ Total blocks written: {blocks_count}")
    
    # Show sample of blocks
    print("\n📦 Sample blocks (latest 5):")
    spark.table('blockchain_ethereum.bronze.blocks_json') \
        .orderBy(col("block_number").desc()) \
        .limit(5) \
        .show(truncate=False)
except Exception as e:
    print(f"⚠️ Error reading blocks table: {e}")

# Show processing state
print("\n📍 Processing state:")
try:
    if spark.catalog.tableExists(state_table):
        state_df = spark.sql(f"SELECT offset, timestamp, source_id FROM {state_table} ORDER BY timestamp DESC LIMIT 10")
        state_df.show(truncate=False)
        
        # Show summary
        latest_state = state_df.first()
        if latest_state:
            print(f"\n✅ Latest committed offset: {latest_state['offset']}")
            print(f"✅ Last update: {latest_state['timestamp']}")
    else:
        print("⚠️ State table not yet created")
except Exception as e:
    print(f"⚠️ Error reading state table: {e}")

print("\n" + "="*60)
print("✅ Streaming job completed!")
print("="*60)

# COMMAND ----------

#%sql DELETE FROM blockchain_ethereum.bronze.blocks_json

# COMMAND ----------

# MAGIC %sql SELECT MIN(block_number), MAX(block_number) FROM blockchain_ethereum.bronze.blocks_json limit 10
# MAGIC
# MAGIC -- max block: 23572103
# MAGIC -- next max block:23572134
# MAGIC -- next max: 23572235 23,571,286

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM blockchain_ethereum.bronze.blocks_json

# COMMAND ----------

# MAGIC %md
# MAGIC FOr Each Batch must be executed in production

# COMMAND ----------

def process_batch(batch_df, batch_id):
    """Production-grade batch processor with atomic state updates"""
    
    if batch_df.isEmpty():
        return
    
    # Write blocks data
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("blockchain_ethereum.bronze.blocks_json")
    
    # Update state atomically (same transaction context)
    state_df = batch_df.select(
        spark_max("block_number").alias("offset")
    ).withColumn("timestamp", lit(datetime.utcnow().isoformat())) \
     .withColumn("source_id", lit("ethereum_mainnet")) \
     .withColumn("batch_id", lit(batch_id))
    
    state_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(state_table)

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="30 seconds")  # Or availableNow for batch
    .start()

# COMMAND ----------

# MAGIC %sql SELECT offset, timestamp FROM blockchain_ethereum.bronze.eth_stream_state ORDER BY offset DESC
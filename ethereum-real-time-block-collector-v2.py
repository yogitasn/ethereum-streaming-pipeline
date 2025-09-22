# Databricks notebook source
pip install web3

# COMMAND ----------

# MAGIC %md
# MAGIC **## Batch processing of Ethereum data from AWS S3**

# COMMAND ----------

# Databricks notebook source
"""
Download Ethereum blockchain data from AWS S3 to Unity Catalog Volume
Target: /Volumes/ethereum-catalog/ethereum/ethereum-batch-volume
"""

import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os

# ============================================
# CONFIGURATION
# ============================================
UC_VOLUME_PATH = "/Volumes/ethereum-catalog/ethereum/ethereum-batch-volume"  # Your Unity Catalog volume
NUM_FILES_TO_DOWNLOAD = 10  # Download 10 files as test

print("Ethereum Blockchain Data Downloader")
print("="*60)
print(f"Target: {UC_VOLUME_PATH}")
print(f"Files to download: {NUM_FILES_TO_DOWNLOAD}")
print("="*60)

# COMMAND ----------

# ============================================
# STEP 1: Connect to S3 (no credentials needed)
# ============================================
print("\nStep 1: Connecting to AWS S3 public blockchain data...")

s3 = boto3.client(
    's3',
    region_name='us-east-2',
    config=Config(signature_version=UNSIGNED)  # Anonymous access
)

print("✓ Connected to S3")

# COMMAND ----------

# ============================================
# STEP 2: List available files
# ============================================
print("\nStep 2: Listing available Ethereum block files...")

try:
    response = s3.list_objects_v2(
        Bucket='aws-public-blockchain',
        Prefix='v1.0/eth/blocks/',
        MaxKeys=50  # Get 50 files to have more options
    )
    
    if 'Contents' not in response:
        print("✗ No files found!")
        exit(1)
    
    # Filter to get only .parquet files
    parquet_files = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.parquet'):
            parquet_files.append({
                's3_key': obj['Key'],
                'size_mb': obj['Size'] / (1024 * 1024),
                'filename': obj['Key'].split('/')[-1]
            })
    
    print(f"✓ Found {len(parquet_files)} Parquet files")
    
    # Show first 5
    print("\nFirst 5 available files:")
    for i, f in enumerate(parquet_files[:5], 1):
        print(f"  {i}. {f['filename']} ({f['size_mb']:.2f} MB)")
    
except Exception as e:
    print(f"✗ ERROR listing files: {e}")
    exit(1)

# COMMAND ----------

# ============================================
# STEP 3: Download files to UC Volume
# ============================================
print(f"\nStep 3: Downloading {NUM_FILES_TO_DOWNLOAD} files to {UC_VOLUME_PATH}...")
print("-"*60)

# Select files to download
files_to_download = parquet_files[:NUM_FILES_TO_DOWNLOAD]

success_count = 0
fail_count = 0
total_size_mb = 0

for i, file_info in enumerate(files_to_download, 1):
    s3_key = file_info['s3_key']
    
    # Create local path preserving the S3 structure
    # Example: v1.0/eth/blocks/date=2024-09-20/file.parquet
    # Becomes: /Volumes/soni/default/ethereum/blocks/date=2024-09-20/file.parquet
    
    relative_path = s3_key.replace('v1.0/eth/', '')  # Remove prefix
    local_path = os.path.join(UC_VOLUME_PATH, relative_path)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Download the file
    print(f"[{i}/{NUM_FILES_TO_DOWNLOAD}] Downloading: {file_info['filename']}... ", end="", flush=True)
    
    try:
        s3.download_file(
            Bucket='aws-public-blockchain',
            Key=s3_key,
            Filename=local_path
        )
        print(f"✓ ({file_info['size_mb']:.2f} MB)")
        success_count += 1
        total_size_mb += file_info['size_mb']
        
    except Exception as e:
        print(f"✗ Error: {e}")
        fail_count += 1


# COMMAND ----------

# ============================================
# STEP 4: Summary
# ============================================
print("\n" + "="*60)
print("DOWNLOAD COMPLETE!")
print("="*60)
print(f"Successful: {success_count}/{NUM_FILES_TO_DOWNLOAD}")
print(f"Failed: {fail_count}/{NUM_FILES_TO_DOWNLOAD}")
print(f"Total downloaded: {total_size_mb:.2f} MB")
print(f"Location: {UC_VOLUME_PATH}")

# ============================================
# STEP 5: Verify downloaded files
# ============================================
print(f"\nStep 5: Verifying files in {UC_VOLUME_PATH}...")
print("-"*60)

try:
    # Walk through the directory tree
    for root, dirs, files in os.walk(UC_VOLUME_PATH):
        level = root.replace(UC_VOLUME_PATH, '').count(os.sep)
        indent = '  ' * level
        print(f'{indent}{os.path.basename(root)}/')
        
        sub_indent = '  ' * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f'{sub_indent}{file} ({size_mb:.2f} MB)')
            
    print(f"\n✓ All files saved to {UC_VOLUME_PATH}")
    
except Exception as e:
    print(f"⚠ Could not list directory: {e}")

# COMMAND ----------



# COMMAND ----------

df_all = spark.read.option("mergeSchema", "true").parquet(
    "/Volumes/ethereum-catalog/ethereum/ethereum-batch-volume/blocks"
)

# Check schema (note: Spark will include 'date' as a partition column)
df_all.printSchema()

# Show some rows
df_all.show(5, truncate=False)

# COMMAND ----------

display(df_all)

# COMMAND ----------

import os
import json
import time
import logging
from typing import Any, Dict, Union
from web3 import Web3
from web3.datastructures import AttributeDict
from hexbytes import HexBytes
from requests.exceptions import HTTPError, ConnectionError, Timeout
from web3.exceptions import Web3Exception

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RateLimiter:
    """Simple rate limiter to control API call frequency"""
    def __init__(self, max_calls_per_second: float = 5.0):
        self.max_calls_per_second = max_calls_per_second
        self.min_interval = 1.0 / max_calls_per_second
        self.last_call_time = 0
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.min_interval:
            sleep_time = self.min_interval - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()

def check_api_response_status(w3: Web3) -> bool:
    """Check if the API endpoint is responding correctly using Web3"""
    try:
        # Make a simple test request using Web3
        block_number = w3.eth.block_number
        
        if block_number > 0:
            logger.info(f"✅ API endpoint responding correctly (current block: {block_number})")
            return True
        else:
            logger.error(f"❌ API endpoint returned invalid block number: {block_number}")
            return False
            
    except Web3Exception as e:
        logger.error(f"❌ Web3 error connecting to API endpoint: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to connect to API endpoint: {e}")
        return False

def to_serializable(obj: Any) -> Any:
    """Convert Web3 objects to JSON-serializable format"""
    if isinstance(obj, (AttributeDict, dict)):
        return {key: to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(item) for item in obj]
    elif isinstance(obj, HexBytes):
        return obj.hex()
    elif isinstance(obj, bytes):
        return obj.hex()
    elif hasattr(obj, '__dict__'):
        return to_serializable(obj.__dict__)
    else:
        return obj

def fetch_block_with_retry(w3: Web3, block_number: int, rate_limiter: RateLimiter, 
                          max_retries: int = 3, base_delay: float = 1.0) -> Dict[str, Any]:
    """Fetch a block with rate limiting and retry logic"""
    for attempt in range(max_retries):
        try:
            rate_limiter.wait_if_needed()
            block = w3.eth.get_block(block_number, full_transactions=True)
            return to_serializable(block)
            
        except (HTTPError, ConnectionError, Timeout) as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to fetch block {block_number} after {max_retries} attempts: {e}")
                raise
            
            # Exponential backoff
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed for block {block_number}, retrying in {delay}s: {e}")
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Unexpected error fetching block {block_number}: {e}")
            raise

def main():
    # Configuration
    provider_uri = "https://mainnet.infura.io/v3/31966bfed8674afaab1a9d9ba201665a"
    
    # Rate limiting: Infura free tier allows ~100k requests/day (~1.15 requests/second)
    # Setting to 0.8 requests/second to be safe
    rate_limiter = RateLimiter(max_calls_per_second=0.8)
    
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(provider_uri))
    
    # Check connection
    if not w3.is_connected():
        logger.error("Failed to connect to Ethereum node")
        return
    
    logger.info("Connected to Ethereum mainnet")
    
    # Get starting block
    rate_limiter.wait_if_needed()
    last_block = w3.eth.block_number
    logger.info(f"Starting from block {last_block}")
    
    # Setup output directory
    output_dir = "/Volumes/ethereum-catalog/ethereum/ethereum-volume/raw/blocks/"
    os.makedirs(output_dir, exist_ok=True)
    
    # Main loop
    while True:
        try:
            # Get latest block number
            rate_limiter.wait_if_needed()
            latest_block = w3.eth.block_number
            
            # Process new blocks
            blocks_to_process = list(range(last_block + 1, latest_block + 1))
            
            if blocks_to_process:
                logger.info(f"Processing blocks {last_block + 1} to {latest_block}")
                
                # Debug: log the block numbers we're about to process
                logger.debug(f"Block numbers to process: {blocks_to_process}")
            
            for block_number in blocks_to_process:
                try:
                    # Ensure block_number is an integer
                    if not isinstance(block_number, int):
                        logger.error(f"❌ Invalid block number type: {type(block_number)} - {block_number}")
                        continue
                        
                    logger.debug(f"Processing block {block_number} (type: {type(block_number)})")
                    
                    # Fetch block with rate limiting and retry logic
                    block_dict = fetch_block_with_retry(w3, block_number, rate_limiter)
                    
                    # Save to file
                    file_path = os.path.join(output_dir, f"block_{block_number}.json")
                    with open(file_path, "w") as f:
                        json.dump(block_dict, f, indent=2)
                    
                    tx_count = len(block_dict.get('transactions', []))
                    logger.info(f"✅ Saved block {block_number} with {tx_count} transactions")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process block {block_number}: {e}")
                    logger.debug(f"Block number type: {type(block_number)}, value: {repr(block_number)}")
                    # Check if we should do a health check
                    if "web3" in str(e).lower() or "connection" in str(e).lower():
                        logger.info("Performing API health check due to connection error...")
                        check_api_response_status(w3)
                    # Continue with next block instead of crashing
                    continue
            
            last_block = latest_block
            
            # Wait before checking for new blocks
            logger.info("Waiting for new blocks...")
            time.sleep(15)  # Check every 15 seconds (Ethereum block time is ~12-13 seconds)
            
        except KeyboardInterrupt:
            logger.info("Stopping block fetcher...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.info("Waiting 30 seconds before retrying...")
            time.sleep(30)

if __name__ == "__main__":
    main()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Transaction schema
tx_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("from", StringType(), True),
    StructField("to", StringType(), True),
    StructField("value", StringType(), True),   # value is big number, keep as string
    StructField("nonce", LongType(), True),
    StructField("gas", LongType(), True)
])

# Block schema
block_schema = StructType([
    StructField("number", LongType(), True),
    StructField("hash", StringType(), True),
    StructField("miner", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("transactions", ArrayType(tx_schema), True)
])


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check your current catalog
# MAGIC SELECT current_catalog();
# MAGIC
# MAGIC -- List schemas in your catalog
# MAGIC SHOW SCHEMAS;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ethereum;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import explode, col

# ✅ Read JSON block files incrementally from your Volume
raw_blocks = (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .schema(block_schema)  # provide schema for better performance
      .load("/Volumes/ethereum-catalog/ethereum/ethereum-volume/raw/blocks/")
)

# Extract block-level fields
blocks_df = raw_blocks.select("number", "hash", "miner", "timestamp")

# Write to Delta using Structured Streaming
blocks_query = (
    blocks_df.writeStream
        .format("delta")  # Delta Lake sink
        .outputMode("append")  # Append new blocks
        .option("checkpointLocation", "/Volumes/ethereum-catalog/ethereum/ethereum-volume/checkpoints/blocks/")  # Required for streaming
        .trigger(availableNow=True)  # Process all available data immediately
        .table("ethereum.blocks")  # Save directly as Delta table in Unity Catalog
)

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Flatten nested transactions
txs_df = raw_blocks.select(
    col("number").alias("block_number"),
    explode("transactions").alias("tx")
).select(
    col("block_number"),
    col("tx.hash").alias("tx_hash"),
    col("tx.from").alias("from_address"),
    col("tx.to").alias("to_address"),
    col("tx.value"),
    col("tx.nonce"),
    col("tx.gas")
)

# Write to Delta using Structured Streaming
txs_query = (
    txs_df.writeStream
        .format("delta")  # Delta Lake sink
        .outputMode("append")  # Append new transactions
        .option("checkpointLocation", "/Volumes/ethereum-catalog/ethereum/ethereum-volume/checkpoints/transactions/")  # Required for streaming
        .trigger(availableNow=True)  # Process all available data immediately
        .table("ethereum.transactions")  # Save directly as Delta table in Unity Catalog
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total blocks ingested
# MAGIC SELECT COUNT(*) 
# MAGIC FROM ethereum.blocks;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Total transactions ingested
# MAGIC SELECT COUNT(*) 
# MAGIC FROM ethereum.transactions;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top miners by number of blocks mined
# MAGIC SELECT miner, COUNT(*) AS blocks_mined
# MAGIC FROM ethereum.blocks
# MAGIC GROUP BY miner
# MAGIC ORDER BY blocks_mined DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- High-value transfers (greater than 1 ETH)
# MAGIC SELECT *
# MAGIC FROM ethereum.transactions
# MAGIC WHERE CAST(value AS DECIMAL(38,0)) > 1000000000000000000;

# COMMAND ----------


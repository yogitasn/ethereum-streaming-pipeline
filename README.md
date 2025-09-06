# ethereum-streaming-pipeline
Streaming pipeline for Ethereum data
Ethereum Streaming Producer

# Ethereum Streaming Producer

This project streams **Ethereum blocks** in near real time using [Web3.py](https://web3py.readthedocs.io/) and writes them as **raw JSON files** into a **Databricks Unity Catalog Volume**.

The script polls for new blocks every **10 seconds**, fetches full block + transaction data, and saves them into a specified **volume path** for downstream processing in Databricks.

---

## 📌 Features

* ⛓️ Connects to Ethereum Mainnet via **Infura**.
* 📦 Retrieves **full block data** including transactions.
* 🗂️ Writes JSON files into **Unity Catalog Volumes**.
* ⏱️ Polls for new blocks every **10 seconds**.
* ✅ Designed for **bronze (raw) layer ingestion** in a medallion architecture.

---

## ⚙️ Prerequisites

* Python 3.9+
* Infura API key (free tier works) → [Get API key](https://infura.io/)
* Databricks workspace with Unity Catalog enabled
* Python libraries:

  ```bash
  pip install web3
  ```

---

## 🚀 Usage

1. **Set Infura provider**
   Replace `YOUR_INFURA_KEY` with your Infura API key:

   ```python
   provider_uri = "https://mainnet.infura.io/v3/YOUR_INFURA_KEY"
   ```

2. **Set output directory**
   Update the Unity Catalog volume path:

   ```python
   output_dir = "/Volumes/ethereum_catalog/raw/blocks/"
   ```

3. **Run the script**

   ```bash
   python blocks_producer.py
   ```

   The script will:

   * Start from the latest block
   * Continuously fetch new blocks
   * Write them as JSON files in the volume:

     ```
     /Volumes/ethereum-catalog/ethereum/ethereum-volume/raw/blocks/block_12345678.json
     ```

---

## 📂 Example Output

A saved block file (`block_12345678.json`):

```json
{
  "number": 12345678,
  "hash": "0xabc123...",
  "miner": "0xdef456...",
  "timestamp": 1694000000,
  "transactions": [
    {
      "hash": "0x789abc...",
      "from": "0x111...",
      "to": "0x222...",
      "value": 1234567890000000000
    }
  ]
}
```

---

## 🔄 Suggested Volume Paths

To align with medallion architecture:

* **Raw blocks (bronze):**
  `/Volumes/ethereum-catalog/ethereum/ethereum-volume/raw/blocks/`

* **Checkpoints (for streaming jobs):**
  `/Volumes/ethereum-catalog/ethereum/ethereum-volume/checkpoints/blocks/`

* **Silver / Gold (optional for later):**
  `/Volumes/ethereum-catalog/ethereum/ethereum-volume/silver/...`
  `/Volumes/ethereum-catalog/ethereum/ethereum-volume/gold/...`

---

## 🛠️ Next Steps

* Ingest raw JSON into **Delta tables** with Spark Structured Streaming.
* Build **Silver** tables with normalized block & transaction schema.
* Add **Gold** aggregations (block stats, miner stats, tx volumes).

---

## 📜 License

MIT License. Free to use and extend.

---

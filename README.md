# Financial_Transactions_Real_Time_Fraud_Detection_Pipeline

An end-to-end real-time financial transaction monitoring and fraud detection platform built using Apache Kafka, Confluent Cloud, PySpark and Databricks Lakeflow Spark Declarative Pipelines.

FinGuard ingests financial transactions and fraud watchlist data, processes them through a Bronze-Silver-Gold architecture, identifies suspicious activities and generates automated email alerts.

## Project Architecture

![Gemini_Generated_Image_3arou93arou93aro.png](./Gemini_Generated_Image_3arou93arou93aro.png "Gemini_Generated_Image_3arou93arou93aro.png")

### Architecture Overview

1. **Transaction Generation:** Python-based producer generates simulated credit card transactions and publishes events to Confluent Kafka.
2. **Streaming Ingestion:** Databricks reads transaction events from Kafka using Spark Structured Streaming.
3. **Fraud Watchlist Ingestion:** Databricks Auto Loader continuously ingests JSON files containing fraud watchlist information.
4. **Customer Data Ingestion:** Customer information is sourced from PostgreSQL and ingested using Lakeflow Connect.
5. **Data Processing:** Lakeflow Spark Declarative Pipelines processes data using the Medallion Architecture.
6. **Fraud Detection:** Streaming joins, customer enrichment and fraud detection rules identify suspicious transactions.
7. **Alerting:** Custom foreachBatch sinks send automated email notifications using Gmail SMTP.
8. **Analytics:** Gold-layer tables support transaction aggregations and Databricks dashboards.

## Technology Stack

| Component | Technology |
|---|---|
| Programming | Python, PySpark |
| Streaming Platform | Apache Kafka, Confluent Cloud |
| Stream Processing | Spark Structured Streaming |
| Data Engineering | Databricks Lakeflow Spark Declarative Pipelines |
| File Ingestion | Databricks Auto Loader |
| Storage | Delta Lake |
| Medallion Architecture | Bronze, Silver, Gold |
| Relational Database | PostgreSQL |
| Data Governance | Unity Catalog |
| Alerting | Gmail SMTP, foreachBatch |
| Analytics | Databricks AI/BI Dashboards, Genie |

## Data Pipeline Implementation

### 1. Transaction Generation and Kafka Ingestion

- Developed a Python Kafka producer using the Confluent Kafka client.
- Generated simulated financial transaction events.
- Published transaction records to the `credit_card_transactions` topic.
- Configured Kafka authentication using SASL_SSL and SASL/PLAIN.

### 2. Bronze Layer – Raw Ingestion

- Ingested transaction events from Confluent Kafka.
- Used Databricks Auto Loader to ingest fraud watchlist JSON files.
- Captured source metadata and ingestion timestamps.

### 3. Silver Layer – Data Transformation

- Parsed incoming JSON transaction records using PySpark.
- Applied schema validation and data quality expectations.
- Normalized fraud watchlist data.
- Processed customer information for downstream enrichment.

### 4. Gold Layer – Fraud Detection and Analytics

- Implemented stream-stream joins between transactions and fraud watchlist records.
- Used watermarking to handle late-arriving events.
- Enriched transactions with customer information.
- Identified high-value transactions based on customer transaction limits.
- Implemented tumbling and sliding-window aggregations for transaction analytics.

### 5. Automated Fraud Alerts

- Developed custom `foreachBatch` sinks.
- Integrated Gmail SMTP for email notifications.
- Generated alerts for suspicious transactions matching fraud watchlist criteria.

### 6. Analytics and Dashboard

- Created Gold-layer analytics tables.
- Built transaction monitoring dashboards using Databricks AI/BI and Genie.

## Key Concepts Implemented

- Kafka producers and consumers
- Spark Structured Streaming
- Auto Loader and incremental file ingestion
- Medallion Architecture
- Delta Lake
- Stream-stream joins and stream-static joins
- Event-time processing and watermarking
- Tumbling and sliding-window aggregations
- Data quality expectations
- foreachBatch streaming sinks
- Automated email notifications

## Project Setup

### Prerequisites

- Python 3.x
- Confluent Cloud account
- Databricks workspace
- PostgreSQL database
- Gmail account with an app password for SMTP notifications

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/finguard-real-time-fraud-detection.git

cd finguard-real-time-fraud-detection
```

### 2. Create a Python environment

```bash
python -m venv .venv
```

Activate it:

Windows:

```bash
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure credentials

Configure Kafka connection details and Gmail credentials using Databricks Secret Scopes.

Do not hardcode credentials in source files or commit secrets to GitHub.

### 5. Run the Kafka producer

Configure the required Kafka connection settings and run:

```bash
python producer_normal.py
```

### 6. Deploy and execute Databricks pipelines

Import or connect the Databricks source files to your workspace, configure the required Unity Catalog permissions and source paths, and execute the relevant pipelines and workflows.

Note: Cloud resources, credentials, catalogs, schemas, volumes and pipeline configurations must be configured in your own environment.

## Project Outcomes

- Built an end-to-end streaming data pipeline integrating Kafka and Databricks.
- Implemented incremental ingestion from both Kafka and JSON file sources.
- Developed fraud detection logic using streaming joins and customer enrichment.
- Implemented event-time window aggregations.
- Automated fraud alert notifications through email.
- Created analytics-ready Gold tables and dashboards.

## Future Enhancements

- Introduce more advanced fraud detection models.
- Add centralized pipeline monitoring and alerting.
- Implement an idempotent alert delivery mechanism.
- Add automated testing and CI/CD deployment.

## Author

Jignesh Purabiya

Data Engineer | PySpark | Apache Kafka | Databricks | SQL

[LinkedIn](linkedin.com/in/jignesh-purabia-a166971ba) | [GitHub](https://github.com/JigneshPurabiya26)
# Credit Card Transaction Simulator

A production-oriented Python simulator that generates realistic credit card transactions and publishes them to a Kafka topic for downstream fraud detection workflows.

## Features

- Generates realistic customer and merchant datasets
- Simulates spending behavior that is consistent per customer
- Applies configurable fraud rules and scores
- Publishes JSON transactions to Confluent Cloud Kafka
- Logs delivery status, transaction metadata, and runtime stats
- Handles keyboard interrupts and graceful shutdown

## Installation

### 1. Create a virtual environment

```bash
python -m venv .venv
```

### 2. Activate the environment

On Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file from the example:

```bash
copy .env.example .env
```

Update the values for your Kafka cluster:

```env
BOOTSTRAP_SERVERS=your-bootstrap-servers:9092
API_KEY=your_api_key
API_SECRET=your_api_secret
TOPIC_NAME=credit_card_transactions
TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

## Running the Producer

```bash
python producer.py
```

The simulator will:

1. Generate customer and merchant datasets if they do not already exist.
2. Start producing transactions to the configured Kafka topic.
3. Print periodic statistics every 100 messages.

## Example Output

```text
INFO - TXN000001 | Amount=1450.50 | Merchant=Amazon India | Fraud=15 | Reason=NORMAL | Offset=None
INFO - TXN000002 | Amount=9500.00 | Merchant=Delhi Grocers | Fraud=45 | Reason=VELOCITY_FRAUD | Offset=12
```

## Project Architecture

- [config.py](config.py) loads configuration values from `.env`
- [models.py](models.py) defines dataclasses for customers, merchants, transactions, and stats
- [customer_generator.py](customer_generator.py) creates realistic customer records
- [merchant_generator.py](merchant_generator.py) creates merchant records
- [transaction_generator.py](transaction_generator.py) simulates transaction behavior
- [fraud_engine.py](fraud_engine.py) evaluates fraud conditions and scores
- [producer.py](producer.py) publishes messages to Kafka and handles runtime control
- [utils.py](utils.py) contains shared helpers

## Folder Explanation

- `data/` stores generated customer and merchant CSV files
- `.env` contains runtime secrets and Kafka configuration
- `requirements.txt` lists project dependencies

## Future Enhancements

- Add schema validation using JSON Schema or Pydantic
- Support multiple Kafka topics for fraud vs. clean traffic
- Persist transaction history for replay and testing
- Add Prometheus metrics for monitoring

# Job Aggregator ETL Pipeline (Databricks + PySpark)

This project is an end-to-end **ETL pipeline** built on **Databricks** that collects job postings from Google Jobs (via SerpAPI), processes them using **PySpark + Delta Lake**, and generates insights such as job demand by company, city, and role.

The pipeline follows the **Medallion architecture (Bronze → Silver → Gold)** and includes visualizations for analytics.

---

## 🚀 Tech Stack

- **Databricks**
- **Apache Spark / PySpark**
- **Delta Lake**
- **Python**
- **SerpAPI (Google Jobs API)**
- **Pandas**
- **Matplotlib**

---

## 🧱 Architecture Overview

High-level flow of the pipeline:

1. **Bronze Layer** – Ingest raw job postings from SerpAPI  
2. **Silver Layer** – Clean & transform data (trim, deduplicate, select useful fields)  
3. **Gold Layer** – Aggregate and prepare data for analytics and visualizations

⚙️ Setup & Running (Local)

Although this project was built in Databricks, it can be adapted locally.

1️⃣ Install dependencies
pip install -r requirements.txt

2️⃣ Configure API Key

Set your SerpAPI API key as an environment variable:

export API_KEY="your-serpapi-key-here"

3️⃣ Run ETL Scripts (if using modular src/ files)
# Bronze ingestion
spark-submit src/bronze_layer.py

# Silver transforms
spark-submit src/silver_layer.py

# Gold aggregations & visualizations
spark-submit src/gold_layer.py

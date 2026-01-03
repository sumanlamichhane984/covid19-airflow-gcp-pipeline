# 🦠 COVID-19 Airflow GCP Data Pipeline

## 📌 Overview
This project implements a production-grade data engineering pipeline using Apache Airflow (Google Cloud Composer) to ingest live COVID-19 country-level data from the Disease.sh API, store raw snapshots in Google Cloud Storage, and load a historical, partitioned dataset into BigQuery for analytics and BI dashboards.

The pipeline runs daily and maintains a full time-series history for each country.

---

## 🛠️ Tech Stack
- Apache Airflow (Google Cloud Composer)
- Google Cloud Storage (GCS)
- BigQuery
- Python 3.10
- Disease.sh COVID-19 API

---

## 🧩 Pipeline Architecture
![Pipeline Diagram](images/covid_pipeline_diagram.png)

---

## 🔄 Workflow Summary
1. Airflow DAG triggers on a daily schedule  
2. Fetches live COVID-19 country-level data from the API  
3. Writes raw NDJSON snapshots to GCS (partitioned by date)  
4. Loads data into a BigQuery staging table (truncated each run)  
5. A SQL `MERGE` operation upserts data into a partitioned history table  
6. BI tools (Looker, Power BI) query the history table for analytics  

---

## 📂 Repository Structure
<pre>
covid19-airflow-gcp-pipeline/
├── dags/
│   └── covid_daily_pipeline.py
├── data/
│   └── sample/
│       └── covid_sample.ndjson
├── images/
│   └── covid_pipeline_diagram.png
├── requirements.txt
├── README.md
├── LICENSE
└── .gitignore
</pre>

---

## 🗄️ BigQuery Data Model

### `covid_country_staging`
- Temporary landing table loaded from GCS  
- Truncated and reloaded daily  

### `covid_country_history`
- Partitioned by `snapshot_date`  
- Stores full historical COVID-19 data per country  
- Used for:
  - Trend analysis  
  - Country-level comparisons  
  - Daily change tracking  

---

## 🚀 How to Run
1. Deploy `covid_daily_pipeline.py` to a Google Cloud Composer environment  
2. Set the Airflow Variable `COMPOSER_BUCKET` to your GCS bucket name  
3. Ensure the BigQuery dataset and tables are created  
4. The DAG will run daily and populate the history table  

---

## 📄 License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

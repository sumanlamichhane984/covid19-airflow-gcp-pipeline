# 🦠 COVID-19 Airflow GCP Data Pipeline

## 📌 Overview
This project implements a production-grade data engineering pipeline using Apache Airflow (Google Cloud Composer) to ingest live COVID-19 country-level data from the Disease.sh API, store raw snapshots in Google Cloud Storage, and load a historical, partitioned dataset into BigQuery for analytics and BI dashboards.

The pipeline runs daily and maintains a full time-series history for each country.

---

## 🛠️ Tech Stack
- Apache Airflow (Google Cloud Composer)
- Google Cloud Storage
- BigQuery
- Python 3.10
- Disease.sh COVID-19 API

---

## 🧩 Pipeline Architecture
![Architecture Diagram](images/covid_pipeline_diagram.png)

---

## 🔄 Workflow
1. Airflow DAG triggers on a daily schedule  
2. Live COVID-19 country data is fetched from the Disease.sh API  
3. Raw NDJSON snapshots are written to Google Cloud Storage (partitioned by date)  
4. Data is loaded into a BigQuery staging table (truncated each run)  
5. A SQL MERGE upserts data into a partitioned history table  
6. BI tools (Looker / Power BI) query the history table for analytics  

---

## 📂 Repository Structure
```text
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

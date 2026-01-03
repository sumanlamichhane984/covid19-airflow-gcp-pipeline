# COVID-19 Airflow GCP Pipeline

## 📌 Overview
An automated data pipeline built with Apache Airflow (Cloud Composer) that ingests live COVID-19 country-level data, stores raw snapshots in Google Cloud Storage, and loads cleaned, historical data into BigQuery for analytics and BI dashboards.

## 🛠️ Tech Stack
- Apache Airflow (Google Cloud Composer)
- Google Cloud Storage
- BigQuery
- Python 3.10
- Disease.sh COVID-19 API

## 🧩 Pipeline Architecture
![Architecture Diagram](images/covid_pipeline_diagram.png)

## 🔄 Workflow
1. Airflow DAG triggers on a daily schedule  
2. Live COVID-19 country data is fetched from the Disease.sh API  
3. Raw NDJSON snapshots are written to Google Cloud Storage (partitioned by date)  
4. Data is loaded into a BigQuery staging table (truncated each run)  
5. A SQL MERGE upserts data into a partitioned history table  
6. BI tools (Looker / Power BI) query the history table for analytics  

## 📂 Repository Structure

```text
covid19-airflow-gcp-pipeline/
├── dags/
│   └── covid_daily_pipeline.py      # Airflow DAG for COVID-19 ingestion
├── data/
│   └── sample/
│       └── covid_sample.ndjson      # Sample API response for testing
├── images/
│   └── covid_pipeline_diagram.png   # Architecture diagram
├── requirements.txt                # Python dependencies
├── README.md                        # Project documentation
├── LICENSE                          # MIT License
└── .gitignore                       # Git ignore rules





## 🚀 How to Run
1. Deploy the DAG file to a Google Cloud Composer environment  
2. Set the Airflow Variable `COMPOSER_BUCKET` to your Composer GCS bucket  
3. The DAG runs daily and loads data into BigQuery automatically  

## 📄 License
MIT License

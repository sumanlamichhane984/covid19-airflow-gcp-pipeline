COVID-19 Airflow GCP Data Pipeline
📌 Overview

This project implements a production-grade data engineering pipeline using Apache Airflow (Google Cloud Composer) to ingest live COVID-19 country-level data from the Disease.sh API, store raw snapshots in Google Cloud Storage, and load a historical, partitioned dataset into BigQuery for analytics and BI dashboards.

The pipeline runs daily, captures a snapshot of COVID-19 metrics for every country, and maintains a time-series history that supports trend analysis, reporting, and visualization.

🛠️ Tech Stack

Apache Airflow (Google Cloud Composer)

Google Cloud Storage (GCS)

BigQuery

Python 3.10

Disease.sh COVID-19 API

🧩 Pipeline Architecture

🔄 Workflow

Airflow DAG triggers on a daily schedule

Live COVID-19 country data is fetched from the Disease.sh API

Raw NDJSON snapshots are written to Google Cloud Storage, partitioned by date

Data is loaded into a BigQuery staging table (truncated each run)

A SQL MERGE upserts data into a partitioned history table

BI tools (Looker / Power BI) query the history table for analytics

📂 Repository Structure
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

🗄️ BigQuery Data Model
Staging Table

covid_country_staging
Used as a temporary landing table for each daily snapshot.
This table is truncated and reloaded every run.

History Table

covid_country_history
Partitioned by snapshot_date and stores full historical COVID-19 data per country.
This enables:

Trend analysis

Country-level comparisons

Daily change tracking

🚀 How to Run

Deploy covid_daily_pipeline.py to a Google Cloud Composer environment

Set the Airflow Variable:

COMPOSER_BUCKET = <your-composer-gcs-bucket>


Enable the DAG in the Airflow UI

The pipeline runs daily and loads data into BigQuery automatically

📦 Install Dependencies (Local Testing)

If running locally with Airflow:

pip install -r requirements.txt

📊 Example Use Cases

COVID-19 trend analysis by country

Daily case growth tracking

BI dashboards using Looker or Power BI

Time-series epidemiological analysis

📄 License

MIT License

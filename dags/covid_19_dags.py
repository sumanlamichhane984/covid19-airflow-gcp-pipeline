from __future__ import annotations

import json
from datetime import timedelta

import pendulum
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# -----------------------------------
# CONFIG
# -----------------------------------
PROJECT_ID = "jenish-my-first-dog"
DATASET_ID = "Covid_19_datasets"

# BigQuery dataset + job location
BQ_LOCATION = "us-central1"

# GCS path prefix for raw data
RAW_PREFIX = "data/raw/covid"

# Tables
STAGING_TABLE = "covid_country_staging"
HISTORY_TABLE = "covid_country_history"

COVID_COUNTRIES_URL = "https://disease.sh/v3/covid-19/countries?allowNull=true"

# BigQuery schema (staging + history)
BQ_SCHEMA = [
    {"name": "updated", "type": "INT64", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "continent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "population", "type": "INT64", "mode": "NULLABLE"},
    {"name": "cases", "type": "INT64", "mode": "NULLABLE"},
    {"name": "todayCases", "type": "INT64", "mode": "NULLABLE"},
    {"name": "deaths", "type": "INT64", "mode": "NULLABLE"},
    {"name": "todayDeaths", "type": "INT64", "mode": "NULLABLE"},
    {"name": "recovered", "type": "INT64", "mode": "NULLABLE"},
    {"name": "todayRecovered", "type": "INT64", "mode": "NULLABLE"},
    {"name": "active", "type": "INT64", "mode": "NULLABLE"},
    {"name": "critical", "type": "INT64", "mode": "NULLABLE"},
    {"name": "tests", "type": "INT64", "mode": "NULLABLE"},
    {"name": "casesPerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "deathsPerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "testsPerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "oneCasePerPeople", "type": "INT64", "mode": "NULLABLE"},
    {"name": "oneDeathPerPeople", "type": "INT64", "mode": "NULLABLE"},
    {"name": "oneTestPerPeople", "type": "INT64", "mode": "NULLABLE"},
    {"name": "activePerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "recoveredPerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "criticalPerOneMillion", "type": "FLOAT64", "mode": "NULLABLE"},
    {
        "name": "countryInfo",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "iso2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "iso3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lat", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "long", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "flag", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "snapshot_date", "type": "DATE", "mode": "NULLABLE"},
]

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

UTC = pendulum.timezone("UTC")


def fetch_to_gcs(**context):
    """
    Fetch countries snapshot and store NDJSON in the Composer bucket.
    Bucket name is read from Airflow Variable: COMPOSER_BUCKET
    """

    logical_date = context["logical_date"].in_timezone("UTC")
    ds = logical_date.strftime("%Y-%m-%d")

    # Call API
    resp = requests.get(COVID_COUNTRIES_URL, timeout=60)
    resp.raise_for_status()
    rows = resp.json()

    ingested_at = pendulum.now("UTC").to_iso8601_string()

    lines = []
    for row in rows:
        row["ingested_at"] = ingested_at
        row["snapshot_date"] = ds
        lines.append(json.dumps(row, ensure_ascii=False))

    ndjson = "\n".join(lines) + "\n"
    object_name = f"{RAW_PREFIX}/snapshot_date={ds}/countries_{ds}.ndjson"

    # Get Composer bucket from Airflow Variable
    bucket_name = Variable.get("COMPOSER_BUCKET")

    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    gcs.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=ndjson,
        mime_type="application/x-ndjson",
    )

    ti = context["ti"]
    ti.xcom_push(key="bucket", value=bucket_name)
    ti.xcom_push(key="object", value=object_name)


with DAG(
    dag_id="covid_daily_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=UTC),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["covid", "composer3", "bigquery"],
) as dag:

    # 1) Ensure dataset exists in correct location
    ensure_dataset = BigQueryInsertJobOperator(
        task_id="ensure_dataset",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}`
                    OPTIONS(location="{BQ_LOCATION}");
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # 2) Ensure history table exists (partitioned)
    ensure_history_table = BigQueryInsertJobOperator(
        task_id="ensure_history_table",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{HISTORY_TABLE}`
                    (
                      updated INT64,
                      country STRING,
                      continent STRING,
                      population INT64,
                      cases INT64,
                      todayCases INT64,
                      deaths INT64,
                      todayDeaths INT64,
                      recovered INT64,
                      todayRecovered INT64,
                      active INT64,
                      critical INT64,
                      tests INT64,
                      casesPerOneMillion FLOAT64,
                      deathsPerOneMillion FLOAT64,
                      testsPerOneMillion FLOAT64,
                      oneCasePerPeople INT64,
                      oneDeathPerPeople INT64,
                      oneTestPerPeople INT64,
                      activePerOneMillion FLOAT64,
                      recoveredPerOneMillion FLOAT64,
                      criticalPerOneMillion FLOAT64,
                      countryInfo STRUCT<
                        _id INT64,
                        iso2 STRING,
                        iso3 STRING,
                        lat FLOAT64,
                        `long` FLOAT64,
                        flag STRING
                      >,
                      ingested_at TIMESTAMP,
                      snapshot_date DATE
                    )
                    PARTITION BY snapshot_date;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # 3) Fetch from API and store in GCS
    fetch_task = PythonOperator(
        task_id="fetch_to_gcs",
        python_callable=fetch_to_gcs,
        provide_context=True,
    )

    # 4) Load from GCS to staging table (truncate each run)
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_bq_staging",
        bucket="{{ ti.xcom_pull(task_ids='fetch_to_gcs', key='bucket') }}",
        source_objects=["{{ ti.xcom_pull(task_ids='fetch_to_gcs', key='object') }}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}",
        schema_fields=BQ_SCHEMA,
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=False,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default",
    )

    # 5) Upsert from staging into history
    upsert_history = BigQueryInsertJobOperator(
        task_id="upsert_history",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{DATASET_ID}.{HISTORY_TABLE}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}` S
                    ON T.snapshot_date = S.snapshot_date
                       AND T.country = S.country
                    WHEN MATCHED THEN
                      UPDATE SET
                        updated = S.updated,
                        continent = S.continent,
                        population = S.population,
                        cases = S.cases,
                        todayCases = S.todayCases,
                        deaths = S.deaths,
                        todayDeaths = S.todayDeaths,
                        recovered = S.recovered,
                        todayRecovered = S.todayRecovered,
                        active = S.active,
                        critical = S.critical,
                        tests = S.tests,
                        casesPerOneMillion = S.casesPerOneMillion,
                        deathsPerOneMillion = S.deathsPerOneMillion,
                        testsPerOneMillion = S.testsPerOneMillion,
                        oneCasePerPeople = S.oneCasePerPeople,
                        oneDeathPerPeople = S.oneDeathPerPeople,
                        oneTestPerPeople = S.oneTestPerPeople,
                        activePerOneMillion = S.activePerOneMillion,
                        recoveredPerOneMillion = S.recoveredPerOneMillion,
                        criticalPerOneMillion = S.criticalPerOneMillion,
                        countryInfo = S.countryInfo,
                        ingested_at = S.ingested_at
                    WHEN NOT MATCHED THEN
                      INSERT ROW;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    ensure_dataset >> ensure_history_table >> fetch_task >> load_to_staging >> upsert_history
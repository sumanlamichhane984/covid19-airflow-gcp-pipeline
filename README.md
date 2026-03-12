# COVID-19 Airflow GCP Pipeline

![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

A production-grade data pipeline that ingests live COVID-19 country-level data from the Disease.sh API on a daily schedule, stores raw NDJSON snapshots in Google Cloud Storage, loads and upserts a partitioned history table in BigQuery, and transforms it into analytics-ready mart tables using dbt.

---

## Architecture

![Pipeline Architecture](docs/Pipeline%20Architecture.png)

**Flow:** Disease.sh API → Cloud Composer (Airflow DAG) → GCS (raw NDJSON) → BigQuery Staging → BigQuery History Table (partitioned) → dbt Models → Analytics Marts

---

## Tech Stack

- **Orchestration:** Apache Airflow on Google Cloud Composer 3
- **Storage:** Google Cloud Storage (raw NDJSON snapshots)
- **Warehouse:** BigQuery (partitioned history table, staging table)
- **Transformation:** dbt (staging views + mart tables)
- **Language:** Python 3.10
- **Source API:** Disease.sh `/v3/covid-19/countries`

---

## Repository Structure

```
covid19-airflow-gcp-pipeline/
├── dags/
│   └── covid_19_dags.py           # Airflow DAG (5-task pipeline)
├── data/
│   └── sample/
│       └── covid_sample.ndjson    # Sample raw payload (first 30 countries)
├── dbt_covid/
│   ├── dbt_project.yml            # dbt project config
│   ├── models/
│   │   ├── staging/
│   │   │   └── stq_covid_country_history.sql   # Staging view on history table
│   │   └── marts/
│   │       ├── mart_country_core_daily.sql      # Core daily metrics per country
│   │       └── mart_country_rates_daily.sql     # Per-million rate metrics per country
├── docs/
│   └── Pipeline Architecture.png
├── .gitignore
└── LICENSE
```

---

## DAG: covid_daily_pipeline

The DAG runs on `@daily` schedule with `catchup=False` and `max_active_runs=1`.

**Task chain:**

```
ensure_dataset >> ensure_history_table >> fetch_to_gcs >> load_to_bq_staging >> upsert_history
```

| Task | Operator | Description |
|---|---|---|
| `ensure_dataset` | `BigQueryInsertJobOperator` | Creates BigQuery dataset if it doesn't exist |
| `ensure_history_table` | `BigQueryInsertJobOperator` | Creates partitioned history table if it doesn't exist |
| `fetch_to_gcs` | `PythonOperator` | Calls Disease.sh API, writes NDJSON to GCS partitioned by date |
| `load_to_bq_staging` | `GCSToBigQueryOperator` | Loads NDJSON from GCS into staging table (WRITE_TRUNCATE) |
| `upsert_history` | `BigQueryInsertJobOperator` | MERGE from staging into history table on (snapshot_date, country) |

**Key design decisions:**

- GCS path follows Hive-style partitioning: `data/raw/covid/snapshot_date={ds}/countries_{ds}.ndjson`
- Bucket name is read from Airflow Variable `COMPOSER_BUCKET` — no hardcoded infra references
- `retries=3` with a 3-minute delay for API reliability
- XCom passes bucket and object path from `fetch_to_gcs` to `load_to_bq_staging`

---

## BigQuery Schema

### covid_country_staging
Truncated and reloaded on every run. Serves as the landing zone for raw API data.

### covid_country_history
Partitioned by `snapshot_date`. Stores one row per country per day. MERGE deduplicates on `(snapshot_date, country)`.

**Key columns:**

| Column | Type | Description |
|---|---|---|
| `country` | STRING | Country name |
| `continent` | STRING | Continent |
| `population` | INT64 | Total population |
| `cases` | INT64 | Cumulative confirmed cases |
| `deaths` | INT64 | Cumulative deaths |
| `recovered` | INT64 | Cumulative recoveries |
| `active` | INT64 | Active cases |
| `tests` | INT64 | Total tests administered |
| `countryInfo` | RECORD | Nested struct: iso2, iso3, lat, long, flag URL |
| `snapshot_date` | DATE | Partition column — date of ingestion |
| `ingested_at` | TIMESTAMP | Exact UTC timestamp of API call |

---

## dbt Models

dbt runs on top of the BigQuery history table to produce two analytics marts.

**Staging view** (`stg_covid_country_history`): thin view directly over `covid_country_history`, filters out null countries, selects 15 core columns.

**Mart tables** (materialized as BigQuery tables):

| Model | Description |
|---|---|
| `mart_country_core_daily` | Daily snapshot of core case metrics: cases, deaths, recovered, active, critical per country |
| `mart_country_rates_daily` | Per-million rate metrics: casesPerOneMillion, deathsPerOneMillion, testsPerOneMillion, activePerOneMillion |

dbt project config (`dbt_project.yml`):
- Staging models materialized as **views**
- Mart models materialized as **tables**
- Profile: `dbt_covid`

---

## How to Run

**1. Deploy DAG to Cloud Composer**

Upload `dags/covid_19_dags.py` to your Composer environment's DAGs bucket.

**2. Set Airflow Variable**

In Composer UI → Admin → Variables:
```
Key:   COMPOSER_BUCKET
Value: your-composer-bucket-name
```

**3. BigQuery setup**

The DAG auto-creates the dataset and history table on first run via `CREATE IF NOT EXISTS`.

**4. Trigger the DAG**

Enable `covid_daily_pipeline` in the Airflow UI. It will run daily and upsert new country snapshots into the history table.

**5. Run dbt models**

```bash
cd dbt_covid
dbt run --profiles-dir . --target prod
```

---

## Design Decisions

**Why MERGE instead of INSERT OVERWRITE?**
MERGE on `(snapshot_date, country)` ensures exactly-once semantics. Re-running the DAG for the same date updates existing rows rather than creating duplicates.

**Why NDJSON over CSV?**
The Disease.sh API returns a nested JSON object with a `countryInfo` struct. NDJSON preserves the nested structure and maps directly to BigQuery's RECORD type without flattening.

**Why Hive-style GCS partitioning?**
`snapshot_date={ds}/` in the GCS path mirrors the BigQuery partition key, making it easy to reprocess a specific day by pointing directly at that prefix.

**Why separate staging and history tables?**
Staging is truncated every run — it acts as a safe landing zone. The history table is append-only via MERGE, which keeps a full time-series even if the API returns stale or null values on a given day.

---

## Challenges

- **Nested JSON schema:** The `countryInfo` field required defining a RECORD type in the BigQuery schema explicitly. Autodetect incorrectly parsed nested fields as strings.
- **XCom for dynamic GCS path:** Passing the bucket and object path from the Python task to `GCSToBigQueryOperator` required XCom with Jinja templating in operator parameters.
- **Composer variable injection:** Hardcoding the bucket name would tie the DAG to one environment. Reading from `Variable.get("COMPOSER_BUCKET")` keeps the code environment-agnostic.

---

## License

MIT License. See [LICENSE](LICENSE) for details.


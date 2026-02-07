{{ config(materialized='view') }}

select
  country,
  continent,
  population,
  cases,
  todayCases,
  deaths,
  todayDeaths,
  recovered,
  todayRecovered,
  active,
  critical,
  tests,
  ingested_at,
  snapshot_date
from `jenish-my-first-dog.Covid_19_datasets.covid_country_history`
where country is not null

{{ config(materialized='table') }}

select
  snapshot_date,
  ingested_at,
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
  critical
from {{ ref('stg_covid_country_history') }}
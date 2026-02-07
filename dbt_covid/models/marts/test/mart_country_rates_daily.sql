{{ config(materialized='table') }}

select
  snapshot_date,
  ingested_at,
  country,
  continent,
  population,
  tests,
  casesPerOneMillion,
  deathsPerOneMillion,
  testsPerOneMillion,
  oneCasePerPeople,
  oneDeathPerPeople,
  oneTestPerPeople,
  activePerOneMillion,
  recoveredPerOneMillion,
  criticalPerOneMillion
from {{ ref('stg_covid_country_history') }}

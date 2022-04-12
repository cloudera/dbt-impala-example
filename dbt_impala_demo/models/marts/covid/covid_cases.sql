{{
    config(
        materialized='incremental',
        partition_by='report_date'
    )
}}

with cases as(
  select 
    date_rep, 
    cases, 
    deaths, 
    geo_id
  from {{ ref('stg_covid__cases') }}
),
country_codes as(
  select
    country,
    alpha_2code
  from {{ ref('ref__country_codes') }}
), collected as(
select 
    cases.date_rep as report_date, 
    cases.cases as cases, 
    cases.deaths as deaths,
    country_codes.country as country
    from cases join country_codes on cases.geo_id = country_codes.alpha_2code
)
select 
  collected.cases,
  collected.deaths,
  collected.country,
  collected.report_date 
from collected


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where collected.report_date > (select max(report_date) from {{ this }})

{% endif %}

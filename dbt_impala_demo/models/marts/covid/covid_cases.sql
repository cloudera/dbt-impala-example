{{
    config(
        materialized='incremental'
    )
}}

with cases as(
  select 
    date_rep as report_date, 
    cases, 
    deaths, 
    geo_id
  from {{ ref('stg_covid__cases_view') }}
),
country_codes as(
  select
    country,
    alpha_2code
  from {{ ref('ref__country_codes') }}
)
select 
    cases.report_date, 
    cases.cases, 
    cases.deaths,
    country_codes.country
    from cases join country_codes on cases.geo_id = country_codes.alpha_2code;

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date_rep > (select max(report_date) from {{ this }})

{% endif %}

{{
    config(
        materialized='incremental'
    )
}}

select 
  date_rep as report_date, 
  cases, 
  deaths, 
  geo_id
from {{ ref('stg_covid__cases_view') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date_rep > (select max(report_date) from {{ this }})

{% endif %}

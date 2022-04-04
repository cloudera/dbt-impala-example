select 
  rep_date as report_date, 
  cases, 
  deaths, 
  geo_id
from {{ ref('stg_covid__cases') }};

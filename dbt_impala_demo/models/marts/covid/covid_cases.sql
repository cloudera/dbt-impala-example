{#
# /*
#  * Copyright 2022 Cloudera, Inc.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#}

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

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

select 
  cast(cast(unix_timestamp(date_rep, "dd/MM/yyyy") as timestamp) AS DATE) as date_rep, 
  cast(cases as BIGINT) as cases, 
  cast(deaths as BIGINT) as deaths, 
  geo_id
from {{ source('raw_covid','raw_covid__cases') }};

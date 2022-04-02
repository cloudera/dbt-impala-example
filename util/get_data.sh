dir=$(dirname "$(readlink -f "$0")")

cd $dir

# Covid cases data
wget https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/csv/data.csv -O ../dbt_impala_demo/seeds/covid_cases.csv

# Covid vaccine data
wget https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv -O ../dbt_impala_demo/seeds/covid_vacine.csv

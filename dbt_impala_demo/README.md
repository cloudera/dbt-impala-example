Features of the demo:

## Seeds
### Reference
Some 'reference' tables are included to demonstrate using dbt seeds
- ref__country_codes.csv
- ref__populations.csv 

## Sources
### covid_raw
Raw data is loaded into tables using impyla to demonstrate using dbt sources.
- dbt_demo_raw_covid
  - raw_covid__cases
  - raw_covid__vaccines

## Models
Models are created to demonstrate the different materialisation methods available, using sources and ref.
### Staging
Staging models use the source() function to refer to the raw tables.
Staging models are created as views by default.
- staging
  - stg_covid__cases.sql
### Marts
Mart models use the ref() function to refer to the staging models.
Mart models are created as tables by default.
- marts
  - covid_cases.sql

## Tests
### Standard tests
The reference seeds include tests for all columns using the standard dbt tests (null & accepted values).
### Custom tests
A custom test is included that checks for the length of a string field. This is used to verify several 'country code' columns that should be either 2 or 3 characters long.

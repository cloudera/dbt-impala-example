# dbt-impala-example
This repo provides an example project for the [dbt-impala](https://github.com/cloudera/dbt-impala) adapter for [dbt](https://www.getdbt.com/).

## dbt_impala_demo
This directory is a [dbt project](https://docs.getdbt.com/docs/building-a-dbt-project/projects).

## util
This directory contains some utilities for generating fake data.

# Getting started
It is recommended to use `venv` to create a Python virtual environment for the demo.
## Requirements
dbt >= 1.1.0

dbt-impala >= 1.1.0

impyla >= 0.18a5

## Install
Start by cloning this repo

`git clone https://github.com/cloudera/dbt-impala-example.git`

Next install the requirements

`pip install -r requirements.txt`

## Configure

Create a dbt profile in `~/.dbt/profiles.yml`

For a Cloudera Data Platform cluster (CDW or DataHub), it should look like this:

```
dbt_impala_demo:
  outputs:
    dev:
     type: impala
     host: <impala host>
     port: <impala port>
     dbname: <db name>
     schema: <db name>
     user: <user>
     password: <password>
     auth_type: ldap
     use_http_transport: true
     use_ssl: true
     http_path: <http path>
  target: dev
```

Test the profile  with `dbt debug`

## Generate fake raw data
To generate fake data, we must first add our Impala details to `util/data_gen/write_data.py`

Modify the following section to reflect your environment:

```
impala_conf = {
    'host': '',
    'port': '',
    'user': '',
    'password': '!',
    'auth_mechanism': 'ldap',
    'use_ssl': True,
    'use_http_transport': True,
    'http_path': ''
}
```

Next, move to the `util/data_gen` directory

`cd util/data_gen`

Run the `start.sh` helper to generate the first set of fake data

`./start.sh 1`

This generates 2 days of fake data for the dates 01/01/2022 and 02/01/2022 and writes it to Impala.
It will create a database `dbt_demo_raw_covid` with 2 tables.

## Using dbt
With our fake data loaded, we can start using dbt.

### dbt seed
First, run the [Seeds](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) to load some reference data.
Two Seeds are included in the demo, `populations` and `country_codes`.

Move to the `dbt_impala_demo` dbt project directory.

Run the seeds with

`dbt seed`

### dbt test
Our Seeds are configured with a couple of [Tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests)

We also have a custom test created in `dbt_impala_demo/tests/generic/test_length.sql` which is used to test the character length of a column.

Our reference data has columns that include ISO Alpha2 and Alpha3 country codes - we know that these columns should always be 2 or 3 columns respectively. To ensure that our reference data is high quality, we can use dbt to test these assumptions and report the results.

We expect that Alpha2 and Alpha3 columns are the correct length, and that no fields should be null.

Run the tests with

`dbt test`

### dbt run
We have 3 sets of models in this demo project.

Firstly, we have `raw`. Our raw models make use of [Sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources). This is data that already exists in our database that dbt needs to refer to. This is the fake data we loaded earlier.

Our raw models are defined in `models/raw/covid/`.

Next, we have `staging`. These are [Models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models). Our staging models use the `source()` method to refer to the Sources we defined in our raw models. The staging models are intermediate views created over our raw data to handle some basic type conversion. These are materialized as views, and we don't expect our end users to query the staging models.

Our staging models are defined in `models/staging/covid`

Lastly, we have `mart`. These are [Models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models). Our mart models use the `ref()` method to refer to the staging models and reference seeds we created using dbt. We use the staging views to handle most of the logic for type casting, but we do some renaming here to make our models easier for users to understand. These models are materialized as tables, as this gives greater performance for user queries. We can use incremental models to make the building of the model more performant.

Our mart models are defined in `models/mart/covid`

Run the models with

`dbt run`

### Review the data

You should now have the following databases & tables:

- reference (database)
  - ref__population (table)
  - ref__country_codes (table)
- raw_covid (database)
  - raw_covid__vaccines (table)
  - raw_covid__cases (table)
- staging_covid (database)
  - stg_covid__cases (view)
- mart_covid (database)
  - covid_cases (table)

In the raw, staging and mart tables, you should see 2 days worth of data for the dates 01/01/2022 and 02/01/2022.

### Generate more fake data
To demonstrate how we can handle new data arriving, let's generate some more data.

As before, move to the `util/data_gen` dir and generate the next 2 days of data with

`./start.sh 2`

This will generate fake data for 03/01/2022 and 04/01/2022 and write it into the raw tables in Impala.

Select the data in `raw_covid.raw_covid__cases` and you should see that you now have 4 days of data.

Select the data in `staging_covid.stg_covid__cases` and you should see that you also have 4 days of data, as this is a view ontop of the raw table.

Selecting the data from `mart_covid.covid_cases` will show that you still only have 2 days worth of data in the mart model. This is because we have not yet updated the model.

### Run the models again
To get the latest data into our mart model, we must run the models again.

Run the models again with

`dbt run`

This will trigger the incremental update of the mart model, by selecting only the days that are later than the maximum date we already have (our current maximum is 02/01/2022, so it will only select the dates 03/01/2022 and 04/01/2022).

When complete, review the `mart_covid.covid_cases` tables and you should see that you now have 4 days worth of data here.

# /*
#  * Copyright 2021 Cloudera, Inc.
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

import csv
import random
import argparse
from pathlib import Path
from datetime import date, timedelta, datetime

# Test data generated here is based on the data published by European Centre for Disease Prevention and Control (ECDC)
# We use real values for demographics/vaccines/country codes such that the data is 'similar' to the real data
# However, the actual numbers (e.g. how many cases, how many doses) are totally random and NOT real


parser = argparse.ArgumentParser(description='Generate fake input data')
parser.add_argument('--days', type=int,
                    help='an integer for the amount of days to generate data for', dest='num_days', default=3)
parser.add_argument('--start-date',
                    help='date string to begin producing at in the format yyyy-MM-dd, e.g. 2022-04-06', dest='start_date')

args = parser.parse_args()

# Gets the absolute path to the top level directory of this project
base_path = str(Path(__file__).resolve().parent.parent.parent)
# Gets the absolute path to the data sub directory for storing generated test data
data_path = str(Path(__file__).resolve().parent)+'/data/'

# Load the country_code.csv reference data so that we generate valid dummy data
country_code_file_loc = base_path + \
    '/dbt_impala_demo/seeds/reference/ref__country_codes.csv'
country_codes = []
with open(country_code_file_loc, mode='r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        country_codes.append(row)

# The ECDC define some set values that used to refer to different demographics (target_group) and different vaccines (vaccine)
vaccine_accepted_values = {
    'target_group':  [
        "ALL",
        "Age<18",
        "HCW",
        "LTCF",
        "Age0_4",
        "Age5_9",
        "Age10_14",
        "Age15_17",
        "Age18_24",
        "Age25_49",
        "Age50_59",
        "Age60_69",
        "Age70_79",
        "Age80+",
        "AgeUnk",
        "1_Age<60",
        "1_Age60+",
    ],
    'vaccine': [
        "AZ",
        "BECNBG",
        "BHACOV",
        "CHU",
        "COM",
        "CVAC",
        "JANSS",
        "HAYATVAC",
        "MOD",
        "NVX",
        "NVXD",
        "QAZVAQ",
        "SGSK",
        "SIICOV",
        "SIN",
        "SPU",
        "SPUL",
        "SRCVB",
        "WUCNBG",
        "CNBG",
        "UNK",
        "ZFUZ",
    ]
}

# Pick a random country code from the loaded CSV of reference country codes


def pick_random_country():
    country_count = len(country_codes)-1
    rand = random.randint(0, country_count)
    return country_codes[rand]


def pick_random_target_group():
    tgt_count = len(vaccine_accepted_values['target_group'])-1
    rand = random.randint(0, tgt_count)
    return vaccine_accepted_values['target_group'][rand]


def pick_random_vaccine():
    vac_count = len(vaccine_accepted_values['vaccine'])-1
    rand = random.randint(0, vac_count)
    return vaccine_accepted_values['vaccine'][rand]

# Generate a series of consecutive dates for which to produce dummy data
def generate_dates(days, start_date):
    if start_date is not None:
        start_date=date.fromisoformat(start_date)
    else:
        start_date = date.today()
    dates = [start_date]
    for i in range(1, days):
        dates.append((start_date + timedelta(days=i)))
    return dates

# Generate dummy vaccine data that shows numbers of doses received by different demographics with different vaccines
# For each country code, we generate data on *every* demographic & vaccine, similar to the real data
def generate_vaccine(dates):
    columns = ['year_week_iso', 'reporting_country', 'num_doses_recv', 'num_doses_exported', 'first_dose',
               'first_dose_refused', 'second_dose', 'unknown_dose', 'target_group', 'vaccine']

    data = []

    date_format = '%d/%m/%Y'
    data.append(columns)

    for date in dates:
        for i in range(0, 5):
            country_code = pick_random_country()['alpha_2code']
            for target_group in vaccine_accepted_values['target_group']:
                for vaccine in vaccine_accepted_values['vaccine']:
                    row = {
                        'year_week_iso': date.strftime(date_format),
                        'reporting_country': country_code,
                        'num_doses_recv': random.randint(0, 10000),
                        'num_doses_exported': random.randint(0, 10000),
                        'first_dose': random.randint(0, 10000),
                        'first_dose_refused': random.randint(0, 100),
                        'second_dose': random.randint(0, 1000),
                        'unknown_dose': random.randint(0, 100),
                        'target_group': target_group,
                        'vaccine': vaccine,
                    }
                    data.append([*row.values()])
    return data

# Generate dummy cases data that shows the number of cases & deaths per country
# For each of the countries defined in the reference country code data, produce X days worth of case data
def generate_cases(dates):
    columns = ['date_rep', 'day', 'month', 'year', 'cases', 'deaths', 'geo_id']

    date_format = '%d/%m/%Y'

    data = []
    data.append(columns)
    for date in dates:
        for country in country_codes:
            country_code = country['alpha_2code']
            row = {
                'date_rep': date.strftime(date_format),
                'day': date.strftime('%d'),
                'month': date.strftime('%m'),
                'year': date.strftime('%Y'),
                'cases': random.randint(0, 100000),
                'deaths': random.randint(0, 100),
                'geo_id': country_code,
            }
            data.append([*row.values()])
    return data

start_date = args.start_date
dates = generate_dates(args.num_days, start_date)
vaccine_data = generate_vaccine(dates)
cases_data = generate_cases(dates)

# Write files
with open(data_path+'raw_covid__cases.csv', 'w') as cases_file:
    cases_writer = csv.writer(cases_file, quoting=csv.QUOTE_MINIMAL)
    cases_writer.writerows(cases_data)

with open(data_path+'raw_covid__vaccines.csv', 'w') as vaccines_file:
    vaccines_writer = csv.writer(vaccines_file, quoting=csv.QUOTE_MINIMAL)
    vaccines_writer.writerows(vaccine_data)

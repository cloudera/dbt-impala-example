import csv
import os
from pathlib import Path
import pprint as pp
import random
from datetime import date, timedelta


base_path = Path(__file__).resolve().parent.parent.parent
country_code_file_loc = str(
    base_path)+'/dbt_impala_demo/seeds/country_codes.csv'

country_codes = []
with open(country_code_file_loc, mode='r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        country_codes.append(row)


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


def generate_dates(days):
    today = date.today()
    dates = [today]
    for i in range(1, days):
        dates.append((today - timedelta(days=i)))
    return dates


def generate_vaccine(dates):
    columns = ['year_week_iso', 'reporting_country', 'num_doses_recv', 'num_doses_exported', 'first_dose',
               'first_dose_refused', 'second_dose', 'unknown_dose', 'target_group', 'vaccine']

    data = []

    date_format = '%Y-W%W'
    data.append(columns)
    for date in dates:
        for i in range(0, 50):
            country_code = pick_random_country()['alpha_2code']
            pop = random.randint(10000, 999999)
            row = {
                'year_week_iso': date.strftime(date_format),
                'reporting_country': country_code,
                'num_doses_recv': random.randint(0, 100000),
                'num_doses_exported': random.randint(0, 100000),
                'first_dose': random.randint(0, 100000),
                'first_dose_refused': random.randint(0, 100),
                'second_dose': random.randint(0, 100000),
                'unknown_dose': random.randint(0, 100),
                'target_group': pick_random_target_group(),
                'vaccine': pick_random_vaccine(),
            }
            data.append([*row.values()])
    return data


def generate_cases(dates):
    columns = ['date_rep', 'day', 'month', 'year', 'cases', 'deaths','geo_id']

    date_format = '%d/%m/%Y'

    data = []
    data.append(columns)
    for date in dates:
        for i in range(0,50):
            country = pick_random_country()
            row = {
                'date_rep': date.strftime(date_format),
                'day': date.strftime('%d'),
                'month': date.strftime('%m'),
                'year': date.strftime('%Y'),
                'cases': random.randint(0,100000),
                'deaths': random.randint(0,100),
                'geo_id': country['alpha_2code'],
            }
            data.append([*row.values()])

    return data


days = 3
dates = generate_dates(3)
vaccine_data = generate_vaccine(dates)
cases_data = generate_cases(dates)
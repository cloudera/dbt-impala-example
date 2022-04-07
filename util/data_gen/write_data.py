import csv
import argparse
from impala.dbapi import connect
from pathlib import Path


parser = argparse.ArgumentParser(description='Write fake data to impala')
parser.add_argument('--refresh', dest='refresh', action='store_const',
                    const=1, default=0,
                    help='drops & recreates demo tables when set')

args = parser.parse_args()


# Gets the absolute path to this file
data_path = str(Path(__file__).resolve().parent)+'/data/'

# Connection properties for Impala
# Customise to your environment
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

# Builds of a string of values to be used in the INSERT
# Returns: STRING
# Example: ('val1', 'val2'),('val1', 'val2')


def build_values_string(values):
    value_strings = []
    for value in values:
        value_strings.append('({values})'.format(
            values=','.join(["'{}'".format(v) for v in value])))
    return ','.join(value_strings)

# Builds a column string to use in CREATE TABLE where all columns are type STRING
# Returns: STRING
# Example: col1 STRING, col2 STRING


def build_columns_string(columns):
    return ','.join(['{col} STRING'.format(col=col) for col in columns])

# Drops & recreates demo tables
def refresh_demo_tables(columns_string, database, table):
    cursor = conn.cursor()

    # Create database SQL
    create_db_sql = 'CREATE DATABASE IF NOT EXISTS {database}'.format(
        database=database)
    # Create table SQL
    drop_table_sql = 'DROP TABLE IF EXISTS {database}.{table} PURGE'.format(
        database=database, table=table)
    create_table_sql = 'CREATE TABLE IF NOT EXISTS {database}.{table} ({columns_string})'.format(
        table=table, columns_string=columns_string, database=database)
    # Truncate table SQL
    truncate_table_sql = 'TRUNCATE TABLE IF EXISTS {database}.{table}'.format(
        table=table, database=database)

    print("Creating database with: "+create_db_sql)
    cursor.execute(create_db_sql)
    print("Dropping table with: "+drop_table_sql)
    cursor.execute(drop_table_sql)
    print("Creating table with: "+create_table_sql)
    cursor.execute(create_table_sql)
    print("Truncating table with: "+truncate_table_sql)
    cursor.execute(truncate_table_sql)

    cursor.close()


# Write generated data files to an Impala table using INSERT INTO..VALUES(...,...,...)
# Returns:
# Example:
def write_file_to_impala(file_path, database, table):
    values = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            values.append(row)

    # Remove header from values, create COLUMN string
    header = values.pop(0)
    columns_string = build_columns_string(header)

    if(args.refresh):
        refresh_demo_tables(columns_string, database, table)

    # Build the INSERT VALUE statement
    values_string = build_values_string(values)
    insert_sql = 'INSERT INTO {database}.{table} VALUES ({values_string})'.format(
        database=database, table=table, values_string=values_string)

    cursor = conn.cursor()

    print("Inserting data from "+file_path)
    cursor.execute(insert_sql)
    cursor.close()

# Creates a connection to an Impala DB
# Returns:
# Example:


def create_cnx():
    return connect(**impala_conf)

conn = create_cnx()

write_file_to_impala(data_path+'raw_covid__cases.csv',
                     'dbt_demo_raw_covid', 'raw_covid__cases')

write_file_to_impala(data_path+'raw_covid__vaccines.csv',
                     'dbt_demo_raw_covid', 'raw_covid__vaccines')

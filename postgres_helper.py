"""This utility is used to load AVRO data into hyperstage, dump hyperstage data to csv"""
import csv
import io
import typing

import psycopg2
import time
import argparse
import logging
import jks
import pandas as pd
from fastavro import reader

logging.basicConfig(level=logging.INFO)


def arg_parser():
    parser = argparse.ArgumentParser(description='Utility to load avro data into postgres DB')
    parser.add_argument('--in-avro-file-location', help='location of avro file to copy to Postgres table',
                        required=False)
    parser.add_argument('--out-csv-file-location', help='location of csv file to copy from postgres table',
                        required=False)
    parser.add_argument('--out-csv-filter-condition', help='Filter condition to select data from postgres table',
                        required=False)
    parser.add_argument('--postgres-hostname', help='postgres hostname', required=True)
    parser.add_argument('--postgres-username', help='postgres username', required=True)
    parser.add_argument('--postgres-jceks-location', help='jceks file location', required=True)
    parser.add_argument('--postgres-password-alias', help='alias for password in jceks file', required=True)
    parser.add_argument('--postgres-db', help='postgres db name', required=True)
    parser.add_argument('--postgres-table', help='postgres table name', required=True)

    return parser.parse_args()


def copy_to_postgres(args: typing.List[str], postgres_secret: str):
    """
    Loads Avro to postgres DB
    :param args: system args to connect to postgres DB
    :param postgres_secret: postgres DB pass
    :return:
    """
    conn = psycopg2.connect(database=args.postgres_db,
                            user=args.postgres_username,
                            password=postgres_secret,
                            host=args.postgres_hostname,
                            port="8124")
    cur = conn.cursor()
    logging.info("connected to postgres db")
    start = time.time()
    # Query target table to get columns list
    cur.execute(f"SELECT * FROM {args.postgres_table} LIMIT 0")
    colnames = [desc[0].lower() for desc in cur.description]
    logging.info("Columns in target table %s.%s: %s", args.postgres_db, args.postgres_table, colnames)
    # Read Avro file
    rows = []
    with open(f'{args.in_avro_file_location}', 'rb') as f:
        for record in reader(f):
            rows.append(record)
    # Create pandas dataframe
    df = pd.DataFrame.from_records(rows)
    df.columns = map(str.lower, df.columns)
    if set(df.columns) - set(colnames):
        logging.info("Dropping these source columns %s", set(df.columns) - set(colnames))
    # create StringIO object with target columns
    sio = io.StringIO()
    sio.write(df[colnames].to_csv(index=False, header=False, quoting=csv.QUOTE_NONNUMERIC, sep=','))
    sio.seek(0)
    try:
        # Copy to Postgres
        cur.copy_expert(
            f"""COPY {args.postgres_table} FROM STDIN with (format txt_variable, lines_terminated_by e'\n', delimiter 
            ',', encoding 'UTF-8')""", sio)
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        raise Exception(e)
    conn.commit()
    sio.close()
    end = time.time()
    cur.execute(f"SELECT count(*)  from {args.postgres_table}")
    rows = cur.fetchall()
    logging.info(f"Number of rows in {args.postgres_table}: %s", rows[0])
    conn.close()
    logging.info(f"Load time %s secs", round(end - start, 2))


def get_pass_from_jceks(location: str, alias: str) -> str:
    """
    Get secret from jceks file

    :param location: location of jceks file
    :param alias: password to open jceks file
    :return: secret stored in jceks file
    """
    # Load jceks file
    store = jks.KeyStore.load(location, 'none')
    return store.secret_keys[alias].key.decode("utf-8")


def copy_from_postgres(args: typing.List[str], postgres_secret: str):
    """
    Creates CSV from postgres DB
    :param args: system args to connect to postgres DB
    :param postgres_secret: postgres DB pass
    :return:
    """
    conn = psycopg2.connect(database = args.postgres_db,
                            user = args.postgres_username,
                            password = postgres_secret,
                            host = args.postgres_hostname,
                            port = "8124")
    cur = conn.cursor()
    logging.info("connected to postgres db")
    cur.execute(f"SELECT * FROM {args.postgres_table} LIMIT 0")
    colnames = [desc[0] for desc in cur.description]
    start = time.time()
    try:
        with open(args.out_csv_file_location, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            cur.copy_expert(f"""COPY (select * from {args.postgres_table} where {args.out_csv_filter_condition}) TO 
            STDOUT with (format csv, delimiter ',', encoding 'UTF-8')""", f)
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        raise Exception(e)
    conn.commit()
    end = time.time()
    conn.close()
    logging.info(f"Load time %s secs", round(end - start, 2))


if __name__ == '__main__':
    args = arg_parser()
    postgres_secret = get_pass_from_jceks(
        location=args.postgres_jceks_location,
        alias=args.postgres_password_alias
    )
    if args.in_avro_file_location and args.out_csv_file_location:
        raise Exception('Please provide either --in-avro-file-location arg or --out-csv-file-location argument not both')
    elif args.in_avro_file_location:
        copy_to_postgres(args, postgres_secret)
    elif args.out_csv_file_location and args.out_csv_filter_condition:
        copy_from_postgres(args, postgres_secret)
    else:
        raise Exception('Please provide either --in-avro-file-location arg or (--out-csv-file-location argument and '
                        '--out-csv-filter-condition)')

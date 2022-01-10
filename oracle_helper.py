"""This utility is used to load AVRO data into Oracle, dump Oracle data to csv"""
import csv
import math
import typing

import cx_Oracle
import time
import argparse
import logging
import jks
import numpy as np
import pandas as pd
from fastavro import reader

logging.basicConfig(level=logging.INFO)


def arg_parser():
    parser = argparse.ArgumentParser(description='Utility to copy csv data into oracle DB')
    parser.add_argument('--in-avro-file-location', help='location of avro file to copy to oracle table',
                        required=False)
    parser.add_argument('--out-csv-file-location', help='location of csv file to copy from oracle table',
                        required=False)
    parser.add_argument('--out-csv-filter-condition', help='Filter condition to select data from oracle table',
                        required=False)
    parser.add_argument('--oracle-hostname', help='oracle hostname', required=True)
    parser.add_argument('--oracle-sid', help='oracle system identifier (SID)', required=True)
    parser.add_argument('--oracle-username', help='oracle username', required=True)
    parser.add_argument('--oracle-jceks-location', help='jceks file location', required=True)
    parser.add_argument('--oracle-password-alias', help='alias for password in jceks file', required=True)
    parser.add_argument('--oracle-db', help='oracle db name', required=True)
    parser.add_argument('--oracle-table', help='oracle table name', required=True)

    return parser.parse_args()


def copy_to_oracle(args: typing.List[str], oracle_secret: str):
    """
    Loads Avro to oracle DB
    :param args: system args to connect to oracle DB
    :param oracle_secret: oracle DB pass
    :return:
    """

    dsn = cx_Oracle.makedsn(args.oracle_hostname, '1521', service_name=args.oracle_sid)
    conn = cx_Oracle.connect(user=args.oracle_username, password=oracle_secret, dsn=dsn)
    cur = conn.cursor()
    batch_size = 100000
    logging.info("connected to oracle db")
    start = time.time()
    # Query target table to get columns list
    cur.execute(f'select * from {args.oracle_db}.{args.oracle_table}').fetchone()
    colnames = [desc[0].upper() for desc in cur.description]
    logging.info("Columns in target table %s.%s: %s", args.oracle_db, args.oracle_table, colnames)
    # Read Avro file
    rows = []
    with open(f'{args.in_avro_file_location}', 'rb') as f:
        for record in reader(f):
            rows.append(record)
    # Create pandas dataframe
    df = pd.DataFrame.from_records(rows)
    df.columns = map(str.upper, df.columns)
    if set(df.columns) - set(colnames):
        logging.info("Dropping these source columns %s", set(df.columns) - set(colnames))
    # convert pandas dataframe to list of tuples
    list_index = list(range(len(df[colnames].columns)))
    values = ','.join([f':{str(x)}' for x in list_index])
    cols = ','.join(colnames)
    insert_sql = f"insert into {args.oracle_db}.{args.oracle_table} ({cols})  values ( {values} )"
    bind_insert = df[colnames].values.tolist()
    # Replace float nan to None, so these values will be inserted as nulls
    for b in bind_insert:
        for index, value in enumerate(b):
            if isinstance(value, float) and math.isnan(value):
                b[index] = None
            elif isinstance(value, type(pd.NaT)):
                b[index] = None
    # split the data based on batch size
    if len(bind_insert) > batch_size:
        for rows in np.array_split(bind_insert, math.floor(len(bind_insert)/batch_size)):
            cur.executemany(insert_sql, list(rows))
    else:
        cur.executemany(insert_sql, bind_insert)
    conn.commit()
    end = time.time()
    cur.execute(f"SELECT count(*)  from {args.oracle_db}.{args.oracle_table}")
    rows = cur.fetchall()
    logging.info(f"Number of rows in {args.oracle_db}.{args.oracle_table}: %s", rows[0])
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


def copy_from_oracle(args: typing.List[str], oracle_secret: str):
    """
    Creates CSV from oracle DB
    :param args: system args to connect to oracle DB
    :param oracle_secret: oracle DB pass
    :return:
    """
    dsn = cx_Oracle.makedsn(args.oracle_hostname, '1521', service_name=args.oracle_sid)
    conn = cx_Oracle.connect(user=args.oracle_username, password=oracle_secret, dsn=dsn)
    cur = conn.cursor()
    logging.info("connected to oracle db")
    start = time.time()
    cur.execute(f'select * from {args.oracle_db}.{args.oracle_table} where {args.out_csv_filter_condition}')

    with open(f'{args.out_csv_file_location}', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow([i[0] for i in cur.description])
        writer.writerows(cur.fetchall())

    conn.commit()
    end = time.time()
    conn.close()
    logging.info(f"Load time %s secs", round(end - start, 2))


if __name__ == '__main__':
    args = arg_parser()
    oracle_secret = get_pass_from_jceks(
        location=args.oracle_jceks_location,
        alias=args.oracle_password_alias
    )
    if args.in_avro_file_location and args.out_csv_file_location:
        raise Exception('Please provide either --in-avro-file-location arg or --out-csv-file-location argument not both')
    elif args.in_avro_file_location:
        copy_to_oracle(args, oracle_secret)
    elif args.out_csv_file_location and args.out_csv_filter_condition:
        copy_from_oracle(args, oracle_secret)
    else:
        raise Exception('Please provide either --in-avro-file-location arg or (--out-csv-file-location argument and '
                        '--out-csv-filter-condition)')

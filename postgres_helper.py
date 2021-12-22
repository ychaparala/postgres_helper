"""This utility call native postgres C bindings to call COPY utility to load CSV data into postgresdb"""
import typing

import psycopg2
import time
import argparse
import logging
import jks

logging.basicConfig(level=logging.INFO)

def arg_parser():
    parser = argparse.ArgumentParser(description='Utility to copy csv data into postgres DB')
    parser.add_argument('--csv-file-location', help='location of csv file', required=True)
    parser.add_argument('--postgres-hostname', help='postgres hostname', required=True)
    parser.add_argument('--postgres-username', help='postgres username', required=True)
    parser.add_argument('--postgres-jceks-location', help='jceks file location', required=True)
    parser.add_argument('--postgres-password-alias', help='alias for password in jceks file', required=True)
    parser.add_argument('--postgres-db', help='postgres db name', required=True)
    parser.add_argument('--postgres-table', help='postgres table name', required=True)

    return parser.parse_args()


def copy_to_postgres(args: typing.List[str], postgres_secret: str):
    """
    Loads CSV to postgres DB
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
    start = time.time()
    # open CSV file
    with open(args.csv_file_location, encoding="latin1",) as f:
        cur.copy_expert(f"""COPY {args.postgres_table} FROM STDIN delimiter ',' CSV HEADER""", f)
        conn.commit()
    end = time.time()
    cur.execute(f"SELECT count(*)  from {args.postgres_table}")
    rows = cur.fetchall()
    for row in rows:
        logging.info(f"Number of rows in {args.postgres_table}: %s", row)
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


if __name__ == '__main__':
    args = arg_parser()
    postgres_secret = get_pass_from_jceks(
        location=args.postgres_jceks_location,
        alias=args.postgres_password_alias
    )
    copy_to_postgres(args, postgres_secret)

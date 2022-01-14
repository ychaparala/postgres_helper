"""This utility is used to dump sql anywhere query data to csv"""
import csv
import typing

import sqlanydb
import time
import argparse
import logging
import jks

logging.basicConfig(level=logging.INFO)


def arg_parser():
    parser = argparse.ArgumentParser(description='Utility to dump sqlanywhere data to csv')
    parser.add_argument('--out-csv-file-location', help='location of csv file to copy from sql anywhere table',
                        required=True)
    parser.add_argument('--sap-db-hostname', help='sql anywhere db hostname', required=True)
    parser.add_argument('--sap-db-username', help='sql anywhere db username', required=True)
    parser.add_argument('--sap-db-jceks-location', help='jceks file location', required=True)
    parser.add_argument('--sap-db-password-alias', help='alias for password in jceks file', required=True)
    parser.add_argument('--sap-db-query', help='query to select data from sql anywhere db',
                        required=True)

    return parser.parse_args()


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


def copy_from_sqlanywhere(args: typing.List[str], sap_db_secret: str):
    """
    Creates CSV from sql anywhere db
    :param args: system args to connect to sql anywhere db
    :param sap_db_secret: sql anywhere db pass
    :return:
    """
    conn = sqlanydb.connect(uid=args.sap_db_username,
                            pwd=sap_db_secret,
                            host=f'{args.sap_db_hostname}:2641')
    cur = conn.cursor()
    logging.info("connected to sql anywhere db")
    start = time.time()

    # Get column names and data
    cur.execute(args.sap_db_query)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    # write header and data to csv file
    with open(args.out_csv_file_location, 'w') as fout:
        writer = csv.writer(fout,
                            delimiter=',',
                            lineterminator='\r\n',
                            quoting=csv.QUOTE_MINIMAL,
                            escapechar='\\')
        writer.writerow(colnames)
        writer.writerows(rows)

    conn.close()
    end = time.time()
    logging.info(f"Load time %s secs", round(end - start, 2))


if __name__ == '__main__':
    args = arg_parser()
    sap_db_secret = get_pass_from_jceks(
        location=args.sap_db_jceks_location,
        alias=args.sap_db_password_alias
    )
    copy_from_sqlanywhere(args, sap_db_secret)

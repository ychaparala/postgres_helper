import psycopg2
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO)

def arg_parser():
    parser = argparse.ArgumentParser(description='Utility to copy csv data into postgres DB')
    parser.add_argument('--csv-file-location', help='location of csv file', required=True)
    parser.add_argument('--postgres-hostname', help='postgres hostname', required=True)
    parser.add_argument('--postgres-username', help='postgres username', required=True)
    parser.add_argument('--postgres-password', help='postgres password', required=True)
    parser.add_argument('--postgres-db', help='postgres db name', required=True)
    parser.add_argument('--postgres-table', help='postgres table name', required=True)

    return parser.parse_args()


def copy_to_postgres(args):
    conn = psycopg2.connect(database=args.postgres_db,
                            user = args.postgres_username,
                            password = args.postgres_password,
                            host =args.postgres_hostname,
                            port = "5432")
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
        logging.info(f"Number of rows in {args.postgres_table}: %s",row)
    conn.close()
    logging.info(f"Load time %s secs", round(end - start,2))

if __name__=='__main__':
    copy_to_postgres(arg_parser())

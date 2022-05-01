import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load all the data from the S3 to the staging tables in the redshift.
    """
    for table, query in copy_table_queries.items():
        print(f"Loading {table} table from S3")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Read the data from the staging tables and insert in the data warehouse tables.
    """
    for table, query in insert_table_queries.items():
        print(f"Inserting data from staging tables to {table} table ")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
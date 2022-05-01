import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Responsible for dropping all the tables used in this ETL processes.
    """
    for table, query in drop_table_queries.items():
        print(f"Droping {table} table")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This funcion create (if not exixts) all the tables used in this ETL processes.
    """    
    for table, query in create_table_queries.items():
        print(f"Creating {table} table")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
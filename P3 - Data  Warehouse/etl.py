import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time

def load_staging_tables(cur, conn):
    """
    Loop through each quueries and copy the data from S3 to the staging table
    Input: 
    cur : cursor
    conn : connection   
    """
    for query in copy_table_queries:
        start = time.time()
        print(query)
        cur.execute(query)
        print("time used {} s".format(time.time()-start))
        conn.commit()


def insert_tables(cur, conn):
    """
    Loop through each quueries and insert the data to the dimensional model (1 Fact table , 5 Dimension table)
    Input: 
    cur : cursor
    conn : connection   
    """
    for query in insert_table_queries:
        start = time.time()
        print(query)
        cur.execute(query)
        print("time used {} s".format(time.time()-start))
        conn.commit()


def main():
    """
    The main method to run the above queries
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
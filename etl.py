import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies data into staging tables
    '''
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)


def insert_tables(cur, conn):
    '''
    Inserts data into analytics tables from staging tables
    '''
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue inserting into table: " + query)
            print(e)


def main():
    '''
    - Reads config file
    
    - Connects to database
    
    - Copies data into staging tables
    
    - Processes data from staging tables into analytics tables
    '''
    # Reads in data from config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connects to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Copies data into staging tables and then processes and inserts into analytics tables
    print ('Loading data into staging tables...')
    load_staging_tables(cur, conn)
    print ('Successful loading of staging tables\n')
    print ('Loading data into analytics tables...')
    insert_tables(cur, conn)
    print ('Successful loading of staging tables')
    
    conn.close()


if __name__ == "__main__":
    main()
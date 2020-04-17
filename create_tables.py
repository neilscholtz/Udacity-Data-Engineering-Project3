import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops all tables from the Redshift database
    '''
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)
    
    print ('All tables successfully dropped.')

def create_tables(cur, conn):
    '''
    Creates all staging and final tables in the Redshift database
    '''
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
            
    print ('All tables successfully created.')

def main():
    '''
    Drops all tables and creates new tables for the Redshift database
    '''
    # Reads in data from config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connects to database
    print ('Connecting to database...')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print ('Successfully connected.\n')

        # Drops tables and creates new tables
        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        
    except psycopg2.Error as e:
            print("Error: Issue connecting to database")
            print(e)

if __name__ == "__main__":
    main()
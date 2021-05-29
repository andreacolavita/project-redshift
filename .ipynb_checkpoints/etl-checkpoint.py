import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load JSON input data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    
    Input:
    * cur -- cursor to connect to DB and execute SQL query
    * conn -- connection to Redshift database (postgres)
    """
    
    print("Loading data from S3 to AWS Redshift...")
    
    for query in copy_table_queries:
        print("----------------------")
        print("Processing query: {}".format(query))
        cur.execute(query)
        conn.commit()
        print("----------------------")
        print("{} processed with success".format(query))
    
    print("All JSON file copied with success into staging tables.")


def insert_tables(cur, conn):
    """
    Insert data from staging tables (staging_songs and staging_events) into start schema tables:
        * Fact: songplays
        * Dimensions: users, songs, artists, time
    
    Input:
    * cur -- cursor to connect to DB and execute SQL query
    * conn -- connection to Redshift database (postgres)
    """
    
    print("Inserting data from staging tables into analytics tables...")
    for query in insert_table_queries:
        print("----------------------")
        print("Processing query: {}".format(query))
        cur.execute(query)
        conn.commit()
        print("----------------------")
        print("{} processed with success".format(query))
    
    print("All analytics tables populated with success.")



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
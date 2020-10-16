import configparser
import psycopg2
from udacity_project_data_warehouse_aws.bb_sql_queries import copy_table_queries, insert_table_queries


# load staging tables with s3 data
def load_staging_tables(cur, conn):
    print('\n%%%%%%%%% S T A G I N G - T A B L E S %%%%%%%%%')
    print('...')
    for query in copy_table_queries:
        print('execute query: ', query)
        cur.execute(query)
        conn.commit()
    print('...done')


# insert data into the db tables. data comes from the staging tables
def insert_tables(cur, conn):
    print('\n%%%%%%%%% I N S E R T -  D A T A - I N T O - S T A R - S C H E M A %%%%%%%%%')
    print('...')
    for query in insert_table_queries:
        print('execute query: ', query)
        cur.execute(query)
        conn.commit()
    print('...done')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # get connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # execute functions to load data from s3 into staging db tables
    # from the staging tables the data will be inserted in the tables songs, users, artists, time, songplays
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #close connection
    conn.close()


if __name__ == "__main__":
    main()
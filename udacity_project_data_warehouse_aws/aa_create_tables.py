import configparser
import psycopg2
from udacity_project_data_warehouse_aws.bb_sql_queries import create_table_queries, drop_table_queries, select_db_statements

# function to delete tables from db
def drop_tables(cur, conn):
    print('\n%%%%%%%%% D R O P - T A B L E S %%%%%%%%%')
    print('...')
    for query in drop_table_queries:
        print('execute query: ', query)
        cur.execute(query)
        conn.commit()
    print('...done')

# function to create tables in db
def create_tables(cur, conn):
    print('\n%%%%%%%%% C R E A T E - T A B L E S %%%%%%%%%')
    print('...')
    for query in create_table_queries:
        print('execute query: ', query)
        cur.execute(query)
        conn.commit()
    print('...done')

# function to print out sql statements
def load_select_db_statements(cur, conn):
    print('\n%%%%%%%%% S T A G I N G - T A B L E S %%%%%%%%%')
    print('...')
    for query in select_db_statements:
        print('execute query: ', query)
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()
    print('...done')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # get connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # execute functions to delete and create tables
    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_select_db_statements(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()
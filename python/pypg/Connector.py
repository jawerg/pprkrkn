from psycopg2 import connect

DB_INFO = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postman_in_postgres'
}


def connect_to_pg_container():
    conn = connect(**DB_INFO)
    return conn

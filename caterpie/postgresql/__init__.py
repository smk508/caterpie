import psycopg2 as pg
from caterpie import read_environment

def get_connection(
    host=None,
    dbname=None,
    username=None,
    password=None,
    port=None,
):
    host = host or read_environment('psql_host')
    dbname = dbname or read_environment('dbname')
    username = username or read_environment('username')
    password = password or read_environment('password')
    port = port or read_environment('port')

    conn_string = "user=%s password=%s dbname=%s host=%s port = %s" % (username, password, dbname, host, port)
    return pg.connect(conn_string)
import psycopg2 as pg

def get_connection():
    conn_string = "user=%s password=%s dbname=%s host=%s port = %s" % (username, password, dbname, host, port)
    return pg.connect(conn_string)
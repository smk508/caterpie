import psycopg2  as pg
from caterpie import postgresql
from caterpie.postgresql import postgresql_utils as pu
import pandas as pd
import random
import names

def make_dummy_table(conn, table_name = 'test'):

    curr = conn.cursor()
    curr.execute("CREATE TABLE {0} ( id INT, name VARCHAR(20), gender CHAR(20), url VARCHAR(80), weight FLOAT );".format(table_name))
    conn.commit()

def make_dummy_data(n):

    dummy = {
            'id': [random.randint(0,10*n) for _ in range(n)],
            'name': [names.get_first_name() for _ in range(n)],
            'gender': [random.choice(['m','f','o']) for _ in range(n)],
            'url': [pu.random_string(5) for _ in range(n)],
            'weight': [random.random()*10*n for _ in range(n)]
            }
    return pd.DataFrame(dummy)

def count_none(iterable):

    counts = [item is None for item in iterable]
    return sum(counts)

def test_table_exists():

    conn = postgresql.get_connection()
    table_name = 'existence'
    pu.drop_table(table_name, conn)
    make_dummy_table(conn, table_name)
    assert pu.table_exists(table_name, conn)
    assert not pu.table_exists('lol', conn)
    pu.drop_table(table_name, conn)

def test_column_in_table():

    conn = postgresql.get_connection()
    table_name = 'columnz'
    pu.drop_table(table_name, conn)
    make_dummy_table(conn, table_name)
    assert pu.column_in_table(table_name, 'gender', conn)
    assert not pu.column_in_table(table_name, 'doggy', conn)
    pu.drop_table(table_name, conn)

def test_get_columns():

    conn = postgresql.get_connection()
    table_name = 'get_column'
    pu.drop_table(table_name, conn)
    make_dummy_table(conn, table_name)
    columns = pu.get_columns(table_name, conn)
    expected = ['id', 'name', 'gender', 'url', 'weight']
    for c in expected:
        assert c in columns
    pu.drop_table(table_name, conn)

def test_create_and_delete():

    conn = postgresql.get_connection()
    types_dict = {
        "id": "INT",
        "name": "VARCHAR(40)"
    }
    table = 'test'
    pu.drop_table(table, conn)
    pu.create_table(table, types_dict, conn)
    assert pu.table_exists(table, conn)
    pu.drop_table(table, conn)
    assert not pu.table_exists(table, conn)

def test_random_string():

    n = 12
    rando = pu.random_string(n)
    assert type(rando) is str
    assert len(rando) == n

def test_add_columns():

    conn = postgresql.get_connection()
    table_name = 'add_column'
    pu.drop_table(table_name, conn)
    make_dummy_table(conn,table_name)
    n = 20
    pu.update_table(table_name, make_dummy_data(n), conn)
    pu.add_columns(table_name, {'donkey': 'INT', 'chocobo': 'VARCHAR(9)'}, conn)
    pu.update_table(table_name, make_dummy_data(n), conn)
    curr = conn.cursor()
    curr.execute("SELECT * FROM {0};".format(table_name))
    results = curr.fetchall()
    assert len(results) == 2*n
    for result in results: #
        assert count_none(result) == 2
    pu.drop_table(table_name, conn)

def test_update_table():

    conn = postgresql.get_connection()
    table_name = 'update'
    pu.drop_table(table_name, conn)
    make_dummy_table(conn, table_name)
    n = 20
    df = make_dummy_data(n)
    pu.update_table(table_name, df, conn)
    pu.update_table(table_name, df, conn)
    pu.update_table(table_name, df, conn, columns = ['name', 'gender'])
    pu.update_table(table_name, df, conn, columns = ['name'])
    curr = conn.cursor()
    curr.execute("SELECT * FROM {0};".format(table_name))
    results = curr.fetchall()
    assert len(results) == 4*n
    for result in results[0:2*n]:
        assert count_none(result) == 0
    for result in results[2*n:3*n]:
        assert count_none(result) == 3
    for result in results[3*n:]:
        assert count_none(result) == 4
    pu.drop_table(table_name, conn)

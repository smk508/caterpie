import psycopg2  as pg
import pandas as pd
import os
import random
import string
from shutil import copyfile
import re

def table_exists(table, conn):
    """ Returns true if requested table exists in database. """
    curr = conn.cursor()
    curr.execute(
        "SELECT EXISTS ("
            "SELECT 1 "
            "FROM pg_tables "
            "WHERE schemaname = 'public' "
            "AND tablename = '{0}' "
            ");".format(table))
    return curr.fetchall()[0][0]

def column_in_table(table, column, conn):
    """ Returns true if requested column is present in requested table. """
    curr = conn.cursor()
    curr.execute(
        "SELECT EXISTS ("
            "SELECT attname "
            "FROM pg_attribute "
            "WHERE attrelid = (SELECT OID FROM pg_class WHERE relname = '{0}') "
            "AND attname = '{1}'"
            ");".format(table, column.lower())
    )
    return curr.fetchall()[0][0]

def get_columns(table, conn):
    """ Returns columns of requested table. """
    curr = conn.cursor()
    curr.execute(
        "SELECT attname "
        "FROM pg_attribute "
        "WHERE attrelid = (SELECT OID FROM pg_class WHERE relname = '{0}') "
        ";".format(table)
    )
    columns = curr.fetchall()
    return [c[0] for c in columns] # Formatting

def create_table(table, types_dict, conn):
    """ Creates table with paramaters specified by the types_dict. """

    print("Creating table {0}".format(table))
    curr = conn.cursor()
    code = "CREATE TABLE {0} (".format(table)
    columns = ["{0} {1}".format(column, types_dict[column]) for column in types_dict]
    code += ", ".join(columns)
    code += ");"
    curr.execute(code)
    conn.commit()

def drop_table(table, conn):
    """ Deletes table if it exists in connected db. """
    print("Dropping table {0}".format(table))
    curr = conn.cursor()
    curr.execute("DROP TABLE IF EXISTS {0}".format(table))
    conn.commit()

def add_columns(table, update_dict, conn):

    print("Adding columns {0} to table {1}".format(str(update_dict), table))
    curr = conn.cursor()
    code = "ALTER TABLE {0} ".format(table)
    updates = ["ADD COLUMN {0} {1}".format(column, column_type) for column, column_type in update_dict.items()]
    code += ", ".join(updates) + ';'
    curr.execute(code)
    conn.commit()

def update_table(table, update_file, conn, columns = None):

    temp = '/tmp/' + random_string() + '.csv' # TODO: Make sure files are unique
    try:
        os.remove(temp)
    except OSError:
        pass
    if type(update_file) is str:
        df = pd.read_csv(update_file, index_col=0)
    elif type(update_file) is pd.DataFrame:
        df = update_file
    else:
        raise TypeError("Update_file must be a path to a csv or a dataframe.")
    if columns is None: # Use all columns
        columns = df.columns

    df[columns].to_csv(temp, index=False, header=True)
    temp_file = open(temp)
    _update_table(table, temp_file, columns, conn) # Save df to temp file and upload
    temp_file.close()
    os.remove(temp)

def _update_table(table, path, columns, conn):

    curr = conn.cursor()
    curr.copy_expert(
        sql = "COPY {0} ({1}) FROM stdin WITH CSV HEADER DELIMITER as ',';".format(table, ', '.join(columns)),
        file = path
        )
    conn.commit()

def random_string(n = 10):
    """ Creates a random string of length n from lower case, upper case, and digits. """
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(n))

def clean(string):
    """ Replaces illegal characters in a string for postgres. """
    return re.sub('[-()]','_', string)

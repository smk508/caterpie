import psycopg2
from caterpie import postgresql
from caterpie import CSV, Writer
import caterpie.postgresql.postgresql_utils as pu
import pandas as pd


def make_test():

    df = pd.read_csv('test/phenoData')
    df = df.rename(columns = {'Unnamed: 0': 'sampleID'})

    types_dict = {
                'sampleID': 'VARCHAR(32)',
                'subjectID': 'VARCHAR(32)',
                'body_site': 'VARCHAR(30)',
                'antibiotics_current_use': 'BOOLEAN',
                'study_condition': 'VARCHAR(20)',
                'disease': 'VARCHAR(20)',
                'age': 'INT',
                'infant_age': 'INT',
                'age_category': 'VARCHAR(20)',
                'gender': 'VARCHAR(20)',
                'country': 'CHAR(3)',
                'non_westernized': 'BOOLEAN',
                'DNA_extraction_kit': 'VARCHAR(30)',
                'number_reads': 'INT',
                'number_bases': 'INT',
                'minimum_read_length': 'INT',
                'median_read_length': 'INT',
                'pregnant': 'BOOLEAN',
                'lactating': 'BOOLEAN',
                'NCBI_accession': 'CHAR(10)'
        }

    return df, types_dict

def make_other_test():

    df = pd.read_csv('test/trapData')

    types_dict = {
        'Date': 'DATE',
        'Open': 'float',
        'High': 'float',
        'Low': 'float',
        'Close': 'float'
    }
    return df, types_dict


def test_csv():

    df, types_dict = make_test()
    csv = CSV(df, types_dict, name='test')
    assert csv.table_name == 'test'
    for key in types_dict:
        assert csv.types_dict[key] == types_dict[key]
    assert csv.message.equals(df)

def test_update_table():

    conn = postgresql.get_connection()
    curr = conn.cursor()
    table_name = 'update_test'
    pu.drop_table(table_name, conn)
    df, types_dict = make_test()
    csv = CSV(df, types_dict, name=table_name)
    writer = Writer([csv], conn)
    pu.create_table(table_name, types_dict, conn)
    writer.update_table(csv)
    curr.execute("SELECT * FROM {0};".format(table_name))
    results = curr.fetchall()
    assert len(results) == 8
    for result in results:
        assert len(result) == 20


def test_write_tables():

    conn = postgresql.get_connection()
    curr = conn.cursor()
    table_name = 'write_test'
    pu.drop_table(table_name, conn)
    df, types_dict = make_test()
    csv = CSV(df, types_dict, name=table_name)
    df, types_dict = make_other_test()
    nvda = CSV(df, types_dict, name=table_name)
    writer = Writer([csv, nvda], conn)
    writer.write_tables()
    curr.execute("SELECT * FROM {0};".format(table_name))
    results = curr.fetchall()
    for result in results:
        assert len(result) == 25

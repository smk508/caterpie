import psycopg2
import caterpie
from caterpie import postgres_utils as pg_backend
from caterpie import sqlite_utils as sqlite_backend
import abc
import pandas as pd

class Table(abc) :

    def __init__(self, source, types_dict, name = '', **kwargs):

        self.load(source, **kwargs)
        self.types_dict = types_dict
        self.table_name = clean(name)

    def preprocess(self): pass # Can perform operations before writing

    def postprocess(self): pass # Can perform cleanup operations after writing

    @abc.abstractmethod
    def load(self, source): pass

    def unload(self):
        """ Removes df to clean memory. """
        self.message = None
        self.types_dict = None

    def infer_types(self): pass # Possible for future

class CSV(Table):

    def load(self, message, index_col = None):
        """ Converts csv into dataframe. """
        if type(message) is pd.DataFrame:
            self.message = df # NOTE: Assume argument is df for now.
        elif type(message) is str:
            self.message = pd.read_csv(df, index_col=index_col)
        else:
            raise TypeError("df must be a pandas dataframe or path to csv file on disk.")

class SourceTable(Table):

    def load(self, source):
        self.message = source.read()

class Writer:

    def __init__(self, csvs, conn, backend='sqlite'):

        self.conn = conn
        self.csvs = csvs
        self.backend = set_backend(backend)

    def write_tables(self): # TODO: Should be idempotent and add columns as needed
        """ Loop through csvs and call COPY FROM command to import them. """
        for csv in self.csvs:
            csv.preprocess()
            if not table_exists(csv.table_name, self.conn):
                create_table(csv.table_name, csv.types_dict, self.conn)
            if not self.table_up_to_date(csv): # TODO: Should check if data is already present
                self.update_table(csv)
            csv.postprocess()

    def update_table(self, csv):

        # Add columns if needed
        alterations = {key: value for key, value in csv.types_dict.items()
                        if not column_in_table(csv.table_name, key, self.conn)}
        if alterations:
            print("Adding columns {0} to table {1}".format(alterations, csv.table_name))
            add_columns(csv.table_name, alterations, self.conn)

        # Add rows
        update_table(csv.table_name, csv.message, self.conn, columns = list(csv.types_dict.keys()))

    def table_up_to_date(self, csv):
        """ Determined if csv needs to be written/updated or if it is already in database. """
        return False

def set_backend(backend):

    if backend == 'postgresql':
        return pg_backend
    if backend == 'sqlite':
        return sqlite_backend
    else:
        raise ValueError("Backend {backend} does not exist.".format(backend=backend))

import psycopg2
import caterpie
from caterpie.postgres_utils import *
import abc
import pandas as pd

class CSV:

    def __init__(self, df, types_dict, name = ''):

        self.load(df)
        self.types_dict = types_dict
        self.table_name = clean(name)

    def preprocess(self): pass # Can perform operations before writing

    def postprocess(self): pass # Can perform cleanup operations after writing

    def load(self, df, index_col = None):
        """ Converts csv into dataframe. """
        if type(df) is pd.DataFrame:
            self.df = df # NOTE: Assume argument is df for now.
        elif type(df) is str:
            self.df = pd.read_csv(df, index_col=index_col)
        else:
            raise TypeError("df must be a pandas dataframe or path to csv file on disk.")

    def unload(self):
        """ Removes df to clean memory. """
        self.df = None
        self.types_dict = None

    def infer_types(self): pass # Possible for future

class Writer:

    def __init__(self, csvs, conn):

        self.conn = conn
        self.csvs = csvs

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
        update_table(csv.table_name, csv.df, self.conn, columns = list(csv.types_dict.keys()))

    def table_up_to_date(self, csv):
        """ Determined if csv needs to be written/updated or if it is already in database. """
        return False

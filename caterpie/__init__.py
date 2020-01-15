import configparser
import os
from os import environ as env
from caterpie.table import CSV, Writer
from dotenv import load_dotenv

load_dotenv()

def read_environment(key):
    try:
        return env[key]
    except KeyError as e:
        raise KeyError("Could not infer a value for {0}. \
            You must either set an environment variable called {0} or \
            specify the value of {0} as a keyword argument.".format(key)
        )
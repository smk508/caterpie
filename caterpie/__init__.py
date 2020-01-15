import configparser
import os
from caterpie.table import CSV, Writer

filename = os.path.join(os.path.expanduser('~'), '.caterpie.config')

def save_config(filename = filename):

    config = configparser.ConfigParser()
    config['connection'] = {}

    ready = False
    for key in ['host', 'dbname', 'username', 'password', 'port']:
        config['connection'][key] = get_input(key)

    with open(filename, 'w') as configfile:
        config.write(configfile)
    print("Wrote configuration to %s" % filename)
    print("You may have to restart your python session for these changes to take effect.")

def get_input(key):

    done = False
    while not done:
        value = input("What is the {0}? ".format(key))
        done = True #yes_or_no("Use {0} for {1}? ".format(value, key))

    return value

def load_config(filename= filename):

    config = configparser.ConfigParser()
    config.read(filename)

    return config

def yes_or_no(question):
    while True:
        confirm = input(question)
        if confirm in ['', 'yes', 'Yes', 'YES', 'fckn hell ya m8', 'y', 'Y']:
            return True
        elif confirm in ['no', 'No', 'NO', 'n', 'N']:
            return False
        else:
            print("Please respond yes or no.")

try:
    config = load_config()
    host = config['connection']['host']
    dbname = config['connection']['dbname']
    username = config['connection']['username']
    password = config['connection']['password']
    port = config['connection']['port']

except:
    print("Could not find a valid configuration file in config.ini. Creating a new one.")
    save_config()

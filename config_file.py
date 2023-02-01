import configparser
from configparser   import ConfigParser
import pathlib

### Here are a couple of examples for how to parse a config file. This does not include writing new config files, only reading existing.

#-----------------------------------------------------------------------------------------------------------------------
### example 1:
### function source code video: {https://www.youtube.com/watch?v=hn5y1DQgCj0}

# def get_user_SQLdb():
#     dict_db = {}
#     config = ConfigParser()
#     conf_file_path = pathlib.Path(__file__).parent.absolute().joinpath('config.ini')
#     print('Parsing: ' + str(conf_file_path))
#     config.read(conf_file_path)
#
# # populates dict_db with key-value pairs within the config.ini
#     dict_db['db_tablename'] = config['SQLdb']['database']
#     dict_db['db_hostname'] = config['SQLdb']['hostname']
#     dict_db['db_username'] = config['SQLdb']['username']
#     dict_db['db_password'] = config['SQLdb']['pwd']
#     dict_db['db_port'] = config['SQLdb']['port_id']
#     print(dict_db)
#     print(config['SQLdb']['username'])
# get_user_SQLdb()

#-----------------------------------------------------------------------------------------------------------------------
### example 2:
### source code is from: https://www.c-sharpcorner.com/article/configuration-files-in-python/#:~:text=Config%20files%20are%20used%20to,at%20some%20point%2C%20of%20time.
### this site also explains how to write a new config file including how to add new config entry formats!

def read_config():
    config = configparser.ConfigParser()
    conf_file_path = pathlib.Path(__file__).parent.absolute().joinpath('config.ini')
    print('Parsing: ' + str(conf_file_path))
    config.read(conf_file_path)
    return config

read_config()
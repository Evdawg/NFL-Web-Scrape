import psycopg2
import sqlalchemy
from configparser import ConfigParser
import pathlib
import config_file

### TODO: Import SQLAlchemy to allow functionality for sending DataFrame object to SQL table.


### Set the SQL references to the config file settings:
#-----------------------------------------------------------------------------------------------------------------------

# Reads the config file for reference to user SQLdb
config = config_file.read_config()

conn = None
cur = None

try:
# Create the connection.
    conn = psycopg2.connect(
        host = config['SQLdb']['hostname'],
        database = config['SQLdb']['database'],
        user = config['SQLdb']['username'],
        password = config['SQLdb']['pwd'],
        port = config['SQLdb']['port_id'])

    cur = conn.cursor()

### Your code will run within the lines below here:
#-----------------------------------------------------------------------------------------------------------------------

# TODO: write code to send Pandas DataFrame to SQL db as a new table.
# Create table here.
    table_1 = '<specify table or SQL script to create table here>'


    cur.execute(table_1)

# Need to commit the table to PostgreSQL.
    conn.commit



#-----------------------------------------------------------------------------------------------------------------------


# Close the connection if no error in try-except code.
    cur.close()
    conn.close()
except Exception as error:
    print(error)

### Close the connection even if error is present.
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
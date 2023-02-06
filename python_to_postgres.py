import config_file

### This is a template for using config file settings to connect to user's SQL db.

#-----------------------------------------------------------------------------------------------------------------------

# Reads the config file for reference to user SQLdb
config = config_file.read_config()

# construct a URL object to utilize config.ini settings:
url_object = URL.create(
    "postgresql+psycopg2",
    username= config['SQLdb']['username'],
    password= config['SQLdb']['pwd'],
    host= config['SQLdb']['hostname'],
    database= config['SQLdb']['database'],)

try:
# Create the engine:
    engine = create_engine(url_object)


#-----------------------------------------------------------------------------------------------------------------------
### Place your code block here that outputs a Pandas DataFrame:





### End code block here.
#-----------------------------------------------------------------------------------------------------------------------

# Send the completed roster_df to a SQL database using psycopg2 and SQLalchemy libraries:
    with engine.begin() as connection:
        roster_df.to_sql(
            name= '<table name here>',
            con= engine,
            if_exists= 'replace', # or {‘fail’, ‘replace’, ‘append’}, default ‘fail’
            index= False)


# Close the connection if no error in try-except block:
except Exception as error:
    print(error)
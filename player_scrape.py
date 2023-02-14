from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine, URL, text
from sqlalchemy.orm import sessionmaker
import config_file
import psycopg2

### TODO: Rewrite this code for the player scrape
    # The full webpage list has been scraped and is available in NFLdb, just add the 'stats/' after hyperlink. loop through that.
    # send all to SQLdb. No csv files!
    # Need to split out DataFrames by player position: C, CB, DB, DE, DL, DT, FB, FS, G, ILB, K, LB, LS, MLB, NT, OLB, OT, P, QB, RB, S, SAF, TE, WR


# Reads the config file for reference to user SQLdb
config = config_file.read_config()

# construct a URL object to utilize config.ini settings:
url_object = URL.create(
    "postgresql+psycopg2",
    username= config['SQLdb']['username'],
    password= config['SQLdb']['pwd'],
    host= config['SQLdb']['hostname'],
    database= config['SQLdb']['database'],)

# create connection object:

# Create the engine and session:
engine = create_engine(url_object, echo= False)

#-----------------------------------------------------------------------------------------------------------------------
with engine.begin() as connection:
    links_df = pd.read_sql(
        sql=text(fr'SELECT "Pos", "Player link" FROM public."Rosters_2022"'),
        con=connection,)
#print(links_df)

# Send any loop iterations with error to an exceptions list:
exceptions = []

### TODO: Figure out a better way to make these global DataFrames for each Position name. All player positions will have their own table due to shared headers.
### See https://stackoverflow.com/questions/68607106/creating-multiple-dataframes-using-for-loop-with-pandas for example of storing the dataframes in a dictionary.
# Need to be able to concat scraped table data to the correct DataFrame within the webpage loop.
# dynamic variables is bad practice, so for now just type out each dataframe variable for each position:
# Need to split out DataFrames by player position: C, CB, DB, DE, DL, DT, FB, FS, G, ILB, K, LB, LS, MLB, NT, OLB, OT, P, QB, RB, S, SAF, TE, WR:

pos_list = ['C', 'CB', 'DB', 'DE', 'DL', 'DT', 'FB', 'FS', 'G', 'ILB', 'K', 'LB', 'LS', 'MLB', 'NT', 'OLB', 'OT', 'P', 'QB', 'RB', 'S', 'SAF', 'TE', 'WR']

pos_df = {}
for pos in pos_list:
    pos_df[pos] = pd.DataFrame()
#print(pos_df)

# Loop through links_df to scrape all players data:
for i in range(0, len(links_df)):
    try:
        webpage = links_df['Player link'][i] + 'stats/'
        print(webpage)

        player_pos = links_df['Pos'][i]
        #print(player_pos)


### Continue here with the webpage parsing code. See roster_scrape.py for reference:
        ### Need to create a beautifulsoup object to parse the whole page:
        res = requests.get(webpage)
        webpage = res.content
        soup = BeautifulSoup(webpage, 'html.parser')

    ### pd.read_html() parses for tables and returns a list of dataframes.
        dfs = pd.read_html(webpage)
        df = dfs[0]

        ### Add columns for player name, team name and year:
        df['Player'] = str(soup.find('h1', {'class': 'nfl-c-player-header__title'}).text.strip())

        try:
            df['Team'] = str(soup.find('a', {'class': 'nfl-o-cta--link'}).text.strip())
        except:
            df['Team'] = 'N/A'

        df['Year'] = 2022

        ### Now concatenate the player's dfs[0] with the global df that matches their position title:
        print('Player DataFrame concatenated with ' + str(player_pos) + ' DataFrame.')
        pos_df[player_pos] = pd.concat([pos_df[player_pos], df], ignore_index=True, axis=0)
        print(pos_df[player_pos])

    except AttributeError:  # the except statement sends Attribute errors to a list that can be exported after the loop is finished.
        exceptions.append(webpage)
        print('There was at least one error during web scrape loop')

    with open(
            r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\review_roster_scrape_output.csv', 'w') as fp:
        fp.write('\n'.join(exceptions))

### End code block here.
#-----------------------------------------------------------------------------------------------------------------------

### Send the completed positional DataFrames to SQL database using psycopg2 and SQLalchemy libraries:

for key in pos_df:
    df.to_sql(
        name= key + ' Data 2022',
        con= engine,
        if_exists= 'replace',
        index= False)

# Close the connection if no error in try-except block:
engine.dispose()
print('SQLAlchemy engine disposed. Program is done running.')
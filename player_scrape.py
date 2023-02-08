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

with engine.begin() as connection:
    links_df = pd.read_sql(
        sql=text(fr'SELECT "Pos", "Player link" FROM public."Rosters_2022"'),
        con=connection,)
print(links_df)

# Send any loop iterations with error to an exceptions list:
# TODO: add code to append to this exceptions list if error during run
exceptions = []

### TODO: Figure out a better way to make these global DataFrames for each Position name. All player positions will have their own table due to shared headers.
# Need to be able to concat scraped table data to the correct DataFrame within the webpage loop.
# dynamic variables is bad practice, so just type out each dataframe variable for each position:
# Need to split out DataFrames by player position: C, CB, DB, DE, DL, DT, FB, FS, G, ILB, K, LB, LS, MLB, NT, OLB, OT, P, QB, RB, S, SAF, TE, WR:
    pos_df_C = pd.DataFrame()
    pos_df_CB = pd.DataFrame()
    pos_df_DB = pd.DataFrame()
    pos_df_DE = pd.DataFrame()
    pos_df_DL = pd.DataFrame()
    pos_df_DT = pd.DataFrame()
    pos_df_FB = pd.DataFrame()
    pos_df_FS = pd.DataFrame()
    pos_df_G = pd.DataFrame()
    pos_df_ILB = pd.DataFrame()
    pos_df_K = pd.DataFrame()
    pos_df_LB = pd.DataFrame()
    pos_df_LS = pd.DataFrame()
    pos_df_MLB = pd.DataFrame()
    pos_df_NT = pd.DataFrame()
    pos_df_OLB = pd.DataFrame()
    pos_df_OT = pd.DataFrame()
    pos_df_P = pd.DataFrame()
    pos_df_QB = pd.DataFrame()
    pos_df_RB = pd.DataFrame()
    pos_df_S = pd.DataFrame()
    pos_df_SAF = pd.DataFrame()
    pos_df_TE = pd.DataFrame()
    pos_df_WR = pd.DataFrame()


# Loop through links_df to scrape all players data:
for i in range(0, 5): #len(links_df)):
    try:
        webpage = links_df['Player link'][i] + 'stats/'
        print(webpage)

        player_pos = links_df['Pos'][i]
        print(player_pos)


### TODO: continue here with the webpage parsing code. See roster_scrape.py for reference.


    except:
        print('error during web scrape loop')



#-----------------------------------------------------------------------------------------------------------------------
# for i in range(0, len(player_list)):
#     try:
#         player_choice = player_list[i]
#         i += 1
#         print(player_choice)
#         print(str(i) + '/' + str(len(player_list)))
#
#         root = 'https://www.nfl.com/players/'
#         url_end_piece = '/stats/'
#
#         url = root + player_choice + url_end_piece
#
#
#         # create the soup object here
#         # this is where I input the url and output the soup object that parses the webpage
#         result = requests.get(url)
#         webpage = result.content
#         soup = BeautifulSoup(webpage, "html.parser")
#
#         # find the html table mark up and then find all row tag objects inside that table object
#         # nfl.com stats pages only contain one table, so don't need to worry about multiple tables in this web scrape
#         table = soup.find('table', {'summary': 'Recent Games'})
#
#         headers = []
#         for i in table.find_all('th'):
#             title = i.text.strip()
#             headers.append(title)
#
#         df = pd.DataFrame(columns = headers)
#         for row in table.findAll('tr')[1:]:
#             data = row.findAll('td')
#             row_data = [td.text.strip() for td in data]
#             length = len(df)
#             df.loc[length] = row_data
#
#
#
#         #add columns to append player name, position, and team name:
#         df['Player'] = str(soup.find('h1', {'class': 'nfl-c-player-header__title'}).text.strip())
#
#         df['Position'] = str(soup.find('span', {'nfl-c-player-header__position'}).text.strip())
#
#         df['Team Name'] = str(soup.find('a', {'nfl-o-cta--link'}).text.strip())
#
#
#         filepath= r'C:\Users\EvanS\Programming\PyCharm\Projects\Web Scrape NFL\2022 weekly data by player'
#         outname = str(soup.find('span', {'nfl-c-player-header__position'}).text.strip()) + '_' + str(player_choice) + '_2022_addteam.csv'
#         #print(outname)
#         #print(filepath)
#         fullname = os.path.join(filepath, outname)
#         df.to_csv(fullname)
#
#
#     except AttributeError:    # the except statement sends Attribute errors to a list that is exported after the loop is finished
#         exceptions.append(player_choice)
#
#     with open(r'C:\Users\EvanS\Programming\PyCharm\Projects\Web Scrape NFL\review_list_4.csv', 'w') as fp:
#         fp.write('\n'.join(exceptions))

#-----------------------------------------------------------------------------------------------------------------------
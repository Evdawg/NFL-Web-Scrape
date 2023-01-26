# TODO: transfer nfl web scraping programs here. Clean and condense the code.
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from datetime import date
import numpy as np
import csv

team_list = ['arizona-cardinals',
'baltimore-ravens',
'atlanta-falcons',
'buffalo-bills',
'carolina-panthers',
'cincinnati-bengals',
'chicago-bears',
'cleveland-browns',
'dallas-cowboys',
'denver-broncos',
'detroit-lions',
'houston-texans',
'green-bay-packers',
'indianapolis-colts',
'los-angeles-rams',
'jacksonville-jaguars',
'minnesota-vikings',
'kansas-city-chiefs',
'new-orleans-saints',
'las-vegas-raiders',
'new-york-giants',
'los-angeles-chargers',
'philadelphia-eagles',
'miami-dolphins',
'san-francisco-49ers',
'new-england-patriots',
'seattle-seahawks',
'new-york-jets',
'tampa-bay-buccaneers',
'pittsburgh-steelers',
'washington-commanders',
'tennessee-titans',]

# print(team_list)



### Send any Errors for specific team choice to a list:
exceptions = []

### Player links list is global to allow for appending this list in loop farther below.

Player_links = []

roster_df = pd.DataFrame(columns=['Player', 'No', 'Pos', 'Status', 'Height', 'Weight', 'Experience', 'College', 'Player link', 'Team', 'Year'])


### levi recommends to write a function that has the dataframe as a parameter. Select statements if cannot concat straight up.
### define a function that performs the concatenation instead of calling the method in a loop:
# def data_concat():
#     print('concatenating global roster_df with the ' + team_choice + ' DataFrame.')
#     pd.concat([roster_df, df], ignore_index=True)


### loop through NFL.com team rosters and pull out all player names, positions, etc.
for i in range(0, 2):  # len(team_list)):
    try:
        team_choice = team_list[i]
        print(team_choice)
        print(str(i) + '/' + str(len(team_list)))

        root = 'https://www.nfl.com/teams/'
        url_end_piece = '/roster/'

        url = root + team_choice + url_end_piece
        print('Scraping roster table for ' + url)

        ### create the soup object here
        ### this is where you input the url and output the soup object that parses the webpage
        result = requests.get(url)
        webpage = result.content
        soup = BeautifulSoup(webpage, "html.parser")

        ### find the html table mark up and then find all row tag objects inside that table object
        ### nfl.com stats pages only contain one table, so don't need to worry about processing multiple tables in this web scrape
        table = soup.find('table', {'summary': 'Roster'})




        # headers = []
        # for i in table.find_all('th'):
        #     title = i.text.strip()
        #     headers.append(title)

        ### Send full table and all rows to a DataFrame object for the team_choice roster:
        df = pd.DataFrame(columns=['Player', 'No', 'Pos', 'Status', 'Height', 'Weight', 'Experience', 'College'])
        for row in table.findAll('tr')[1:]:

           # data = row.findAll('td')
            data = [[td.a['href'] if td.find('a') else
            ''.join(td.stripped_strings) for td in row.find_all('td')]
            for row in table.find_all('tr')]
            print(data)
            #row_data = [td.text.strip() for td in data]
            #length = len(df)
            #df.loc[length] = data



    ### TODO: Add three columns not included in the NFL.com table and populate within the loop:
            ### Attempt to concat the individual team roster table to a global DataFrame to save each df
            ### that is otherwise overwritten in each loop:
        roster_df = pd.concat([roster_df, df], ignore_index=True, axis=0)



        ### TODO:
        ### Working on building a second DF that will be merged with the NFL.com table scrape???
        ### or try to add the additional column and row data in the loop above.
        ### Need to add columns for the Player Name schema, Team, and Year.






    ### Try adding the player webpage link as a row item:
    # playerlink_df_headers = ['Player_Link', 'Team Name', 'Year']
    # playerlink_df = pd.DataFrame(columns = playerlink_df_headers)
    # df['Player_link'] = str(soup.find('tr', {'nfl-o-roster__player-name nfl-o-cta--link'}))
    # df['Team Name'] = str(soup.find('div', {'nfl-c-team-header__title'}).text.strip())
    # df['Year'] = date.today().year

    ### Then append the dataframes

    except AttributeError:  # the except statement sends Attribute errors to a list that is exported after the loop is finished
        exceptions.append(team_choice)

    with open(
            r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\review_roster_scrape_output.csv',
            'w') as fp:
        fp.write('\n'.join(exceptions))

### Manually put together second dataframe with only the Player link field:
# Player_link_df  = pd.DataFrame(data = Player_links, columns = ['Player link'])
# print(Player_link_df)


#print(roster_df)

roster_df.to_csv(r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\test.csv')

### The dataframe is being overwritten with each loop pass. Need to fix it so all pages scraped go to the same dataframe.
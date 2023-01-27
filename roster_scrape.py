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

### Send any Errors for specific team choice to a list:
exceptions = []

roster_df = pd.DataFrame()

### TODO: Levi recommends to write a function that has the dataframe as a parameter. May need to use Select statments.
#       define a function that performs the concatenation instead of calling the method repeatedly within a loop:
#       def data_concat():
#               .........


### Loop through NFL.com team rosters webpage schema:
for i in range(0, 1):  # len(team_list)):
    try:
        team_choice = team_list[i]
        print(team_choice)
        print(str(i) + '/' + str(len(team_list)))

        root = 'https://www.nfl.com/teams/'
        url_end_piece = '/roster/'

        url = root + team_choice + url_end_piece
        print('Scraping roster table for ' + url)

        ### Need to create a beautifulsoup object to parse the whole page:
        res = requests.get(url)
        webpage = res.content
        soup = BeautifulSoup(webpage, 'html.parser')

        ### pd.read_html() parses for tables and returns a list of dataframes.
        dfs = pd.read_html(url)
        df = dfs[0]

        df['Team'] = str(soup.find('div', {'class': 'nfl-c-team-header__title'}).text.strip())
        df['Year'] = 2022

        ### Create list of hyperlinks to player page, concat as a df to the roster_df:
        table = soup.find('table', {'summary': 'Roster'})
        Player_links = []

        ### Some players do not have hyperlink to their own page. Populates Player_links list with 'N/A' rather than that row being skipped.
        for row in table.findAll('tr')[1:]:
            link = row.find('a', {'class': 'nfl-o-roster__player-name nfl-o-cta--link'})
            if link != None:
                Player_links.append('https://www.nfl.com' + link.get('href'))
            else:
                if link == None:
                    Player_links.append(str('N/A'))
        df['Player link'] = Player_links
        # links_df = pd.DataFrame(Player_links, columns= ['Player link'])
        # print(links_df)
        # links_df.to_csv(r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\test_links.csv')

        print(team_choice + ' DataFrame concatenated with roster_df.')
        roster_df = pd.concat([roster_df, df], ignore_index=True, axis=0)


    except AttributeError:  # the except statement sends Attribute errors to a list that is exported after the loop is finished
        exceptions.append(team_choice)

    with open(
            r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\review_roster_scrape_output.csv', 'w') as fp:
        fp.write('\n'.join(exceptions))

print(roster_df)
roster_df.to_csv(r'C:\Users\EvanS\Programming\PyCharm\Projects\NFL-Web-Scrape-V2\Scrap files\test.csv')

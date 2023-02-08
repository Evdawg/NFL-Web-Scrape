from bs4 import BeautifulSoup
import requests
import pandas as pd
import os

### TODO: Rewrite the code for the player scrape
    # The full webpage list has been scraped and is available in NFLdb, just add the 'stats/' after hyperlink. loop through that.
    # send all to SQLdb. No csv files!
    # Need to split out DataFrames by player position: C, CB, DB, DE, DL, DT, FB, FS, G, ILB, K, LB, LS, MLB, NT, OLB, OT, P, QB, RB, S, SAF, TE, WR


#-----------------------------------------------------------------------------------------------------------------------
# your while and for loops will iterate through these lists during the web scrape
player_csv = pd.read_csv(<'list of all player names for current season here.csv'>)  #
#print(player_csv)

player_list = player_csv['Player'].tolist()



# Set up the dynamic URL schema
# Needs to be dynamic for i in player_list

exceptions = []   # This is to send any Attribute Errors for missing page element to a list that I can review after these all run

for i in range(0, len(player_list)):
    try:
        player_choice = player_list[i]
        i += 1
        print(player_choice)
        print(str(i) + '/' + str(len(player_list)))

        root = 'https://www.nfl.com/players/'
        url_end_piece = '/stats/'

        url = root + player_choice + url_end_piece


        # create the soup object here
        # this is where I input the url and output the soup object that parses the webpage
        result = requests.get(url)
        webpage = result.content
        soup = BeautifulSoup(webpage, "html.parser")

        # find the html table mark up and then find all row tag objects inside that table object
        # nfl.com stats pages only contain one table, so don't need to worry about multiple tables in this web scrape
        table = soup.find('table', {'summary': 'Recent Games'})

        headers = []
        for i in table.find_all('th'):
            title = i.text.strip()
            headers.append(title)

        df = pd.DataFrame(columns = headers)
        for row in table.findAll('tr')[1:]:
            data = row.findAll('td')
            row_data = [td.text.strip() for td in data]
            length = len(df)
            df.loc[length] = row_data

# TODO: 187 players do not have the team name element on their page and are being skipped by the program. Figure out how to automate this within the loop instead of just sending the error names to a list for manual parsing.



        #add columns to append player name, position, and team name:
        df['Player'] = str(soup.find('h1', {'class': 'nfl-c-player-header__title'}).text.strip())

        df['Position'] = str(soup.find('span', {'nfl-c-player-header__position'}).text.strip())

        df['Team Name'] = str(soup.find('a', {'nfl-o-cta--link'}).text.strip())


        filepath= r'C:\Users\EvanS\Programming\PyCharm\Projects\Web Scrape NFL\2022 weekly data by player'
        outname = str(soup.find('span', {'nfl-c-player-header__position'}).text.strip()) + '_' + str(player_choice) + '_2022_addteam.csv'
        #print(outname)
        #print(filepath)
        fullname = os.path.join(filepath, outname)
        df.to_csv(fullname)


    except AttributeError:    # the except statement sends Attribute errors to a list that is exported after the loop is finished
        exceptions.append(player_choice)

    with open(r'C:\Users\EvanS\Programming\PyCharm\Projects\Web Scrape NFL\review_list_4.csv', 'w') as fp:
        fp.write('\n'.join(exceptions))

#-----------------------------------------------------------------------------------------------------------------------
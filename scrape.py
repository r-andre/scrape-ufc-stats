#!/usr/bin/env python

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
# Local:
from utils import log

# Scraping event titles and links from main page:
url = 'http://ufcstats.com/statistics/events/completed?page=all'

site = requests.get(url)
soup = BeautifulSoup(site.text, 'lxml')

log("Loading list of events")

event_links = {}
for row in soup.findAll('a', {'class' : 'b-link b-link_style_black'}):
    text = row.text.strip()
    href = row['href']
    event_links[text] = href

# Scraping event pages for fights and links:
# summaries = pd.DataFrame()
event_fights = {} # stores links to event pages
event_dates = {} # stores dates of event
event_locations = {} # stores location of event

log("Loading list of fights")

for event in list(event_links.keys())[:3]: # event_links.keys(): # 
    # For every event, get the links to each fight page:
    log("Loading " + event)
    page = requests.get(event_links[event])
    soup = BeautifulSoup(page.text, 'lxml')

    # summary = pd.read_html(link, attrs={'class' : 'b-fight-details__table b-fight-details__table_style_margin-top b-fight-details__table_type_event-details js-fight-table'})[0]
    # summary['Event'] = event
    # summaries = summaries.append(summary, ignore_index=True)

    fight_links = [] # stores links to all fights
    for link in soup.findAll(
        'a', {'class' : 'b-flag b-flag_style_green'
        }, href=True):
        fight_links.append(link['href'])
    
    event_fights[event] = fight_links

    # fighter_links = []
    # for link in soup.findAll('a', {'class' : 'b-link b-link_style_black'}, href=True):
    #     fighter_links.append(link['href])

    event_dates[event] = soup.findAll(
        'li', {'class' : 'b-list__box-list-item'}
        )[0].text.strip().replace("Date:", "").strip()
    event_locations[event] = soup.findAll(
        'li', {'class' : 'b-list__box-list-item'}
        )[1].text.strip().replace("Location:", "").strip()

# Scraping fight page for stats:
columns = []
table = []
for event in event_fights.keys():
    # For every event in the list:
    log("Starting to scrape " + event)
    for fight in event_fights[event]:
        # For every fight of the event:
        page = requests.get(fight)
        soup = BeautifulSoup(page.text, 'lxml')

        # Create a list of column names (only once!):
        if columns == []:
            cat = [c.text.strip() for c in soup
                .findAll('th', {'class' : 'b-fight-details__table-col'})]
            for c in [c for c in cat if 'Round' not in c]:
                columns.append(c)
            columns = columns[0:10] + columns[21:29]
            table.append(
                ['Event'
                    , 'Date'
                    , 'Location'
                    , 'Bout'
                    , 'Fight'
                    , 'Winner'
                    , 'Method'
                    , 'Round'
                    , 'Time'
                    , 'Format'
                    , 'Referee'
                    , 'Details'
                    , 'Corner'
                    , 'Result']
                + columns
                )

        # Getting the data and storing it in respective variables:
        red_fighter = soup.findAll(
            'a', {'class' : 'b-link b-fight-details__person-link'}, href=True
            )[0].text.strip() # name of red fighter
        blue_fighter = soup.findAll(
            'a', {'class' : 'b-link b-fight-details__person-link'}, href=True
            )[1].text.strip() # name of blue fighter
        vs = (red_fighter + ' vs. ' + blue_fighter) # who is fighting?
        
        log("Scraping " + event + ", " + vs)

        weightclass = soup.find(
            'i', {'class' : 'b-fight-details__fight-title'}
            ).text.strip() # type of bout
        red_result = soup.findAll(
            'i', {'class' : 'b-fight-details__person-status'}
            )[0].text.strip() # did red win or lose?
        blue_result = soup.findAll(
            'i', {'class' : 'b-fight-details__person-status'}
            )[1].text.strip() # did blue win or lose?
        # Defining the name of the winner:
        if red_result == 'W' and blue_result == 'L':
            winner = red_fighter
        elif blue_result == 'W'  and red_result == 'L':
            winner = blue_fighter
        else:
            winner = ''
        method = soup.find('i', {'style' : 'font-style: normal'}).text.strip() # win method
        round = int(
            soup.find('i', {'class' : 'b-fight-details__text-item'})
            .text.replace("Round:", "").strip()
            ) # round the fight finished in
        time = soup.findAll(
            'i', {'class' : 'b-fight-details__text-item'}
            )[1].text.replace("Time:", "").strip() # timestamp of the finish
        format = soup.findAll(
            'i', {'class' : 'b-fight-details__text-item'}
            )[2].text.replace("Time format:", "").strip() # bout format (rounds)
        referee = soup.findAll(
            'i', {'class' : 'b-fight-details__text-item'}
            )[3].text.replace("Referee:", "").strip() # referee name
        details = soup.findAll(
            'p', {'class' : 'b-fight-details__text'}
            )[1].text.replace("Details:", "").strip()\
                .replace("\n", " ")\
                .replace("  ", "")\
                .replace(".", ", ")\
                .replace(" - ", "-").strip() # detailled method of winning

        # Trying to scrape fight data (not all fights have stats posted!):
        try:
            red_total = (
                [c.text.strip() for c in soup
                    .find('tbody', {'class' : 'b-fight-details__table-body'})
                    .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2]
                + [c.text.strip() for c in soup
                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[2]
                    .findAll('p', {'class' : 'b-fight-details__table-text'})][2::2]
                ) # total stats for red fighter
            blue_total = (
                [c.text.strip() for c in soup
                    .find('tbody', {'class' : 'b-fight-details__table-body'})
                    .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2]
                + [c.text.strip() for c in soup
                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[2]
                    .findAll('p', {'class' : 'b-fight-details__table-text'})][3::2]
                ) # total stats for red fighter
        except:
            # if no data was scrapend append empty data lists
            red_total = []
            blue_total = []

        # appending data to list of lists
        table.append(
            [event
                , event_dates[event]
                , event_locations[event]
                , weightclass
                , vs
                , winner
                , method
                , round
                , time
                , format
                , referee
                , details
                , 'Red'
                , red_result]
            + red_total
            )
        table.append(
            [event
                , event_dates[event]
                , event_locations[event]
                , weightclass
                , vs
                , winner
                , method
                , round
                , time
                , format
                , referee
                , details
                , 'Blue'
                , blue_result]
            + blue_total
            )

log("Finished scraping fights")

# Converting data in list of lists to DataFrame:
df = pd.DataFrame(table[1:], columns=table[0])
df = df.drop(['Sig. str', 'Sig. str. %.1'], axis=1)
df['Timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
df['Timestamp'] = pd.to_datetime(df['Timestamp'])

log("Writing scraped data to database")

# Saving scraped data to .csv
# df.to_csv('raw.csv', sep='\t', index=False)

# df = pd.read_csv('raw.csv', sep='\t')

# Loading scraped data into database
conn = sqlite3.connect('ufc.db')
df.to_sql('RAW', conn, index=False, if_exists='append')
conn.close()

# Code for scraping individual round data
'''
        try:
            red_one = (
            [c.text.strip() for c in soup
                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2][0:10]
            + [c.text.strip() for c in soup
                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                .findAll('p', {'class' : 'b-fight-details__table-text'})][2:18:2]
                )
            try:
                red_two = (
                    [c.text.strip() for c in soup
                        .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                        .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2][10:20]
                    + [c.text.strip() for c in soup
                        .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                        .findAll('p', {'class' : 'b-fight-details__table-text'})][20:36:2]
                    )
                try:
                    red_three = (
                        [c.text.strip() for c in soup
                            .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                            .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2][20:30]
                        + [c.text.strip() for c in soup
                            .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                            .findAll('p', {'class' : 'b-fight-details__table-text'})][38:54:2]
                        )
                    try:
                        red_four = (
                            [c.text.strip() for c in soup
                                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                                .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2][30:40]
                            + [c.text.strip() for c in soup
                                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                                .findAll('p', {'class' : 'b-fight-details__table-text'})][56:72:2]
                            )
                        try:
                            red_five = (
                                [c.text.strip() for c in soup
                                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                                    .findAll('p', {'class' : 'b-fight-details__table-text'})][0::2][40:50]
                                + [c.text.strip() for c in soup
                                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                                    .findAll('p', {'class' : 'b-fight-details__table-text'})][74:90:2]
                                )
                        except:
                            pass
                    except:
                        pass
                except:
                    pass
            except:
                pass
        except:
            pass
        try:
            blue_one = (
            [c.text.strip() for c in soup
                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2][0:10]
            + [c.text.strip() for c in soup
                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                .findAll('p', {'class' : 'b-fight-details__table-text'})][3:18:2]
                )
            try:
                blue_two = (
                    [c.text.strip() for c in soup
                        .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                        .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2][10:20]
                    + [c.text.strip() for c in soup
                        .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                        .findAll('p', {'class' : 'b-fight-details__table-text'})][21:36:2]
                    )
                try:
                    blue_three = (
                        [c.text.strip() for c in soup
                            .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                            .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2][20:30]
                        + [c.text.strip() for c in soup
                            .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                            .findAll('p', {'class' : 'b-fight-details__table-text'})][39:54:2]
                        )
                    try:
                        blue_four = (
                            [c.text.strip() for c in soup
                                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                                .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2][30:40]
                            + [c.text.strip() for c in soup
                                .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                                .findAll('p', {'class' : 'b-fight-details__table-text'})][57:72:2]
                            )
                        try:
                            blue_five = (
                                [c.text.strip() for c in soup
                                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[1]
                                    .findAll('p', {'class' : 'b-fight-details__table-text'})][1::2][40:50]
                                + [c.text.strip() for c in soup
                                    .findAll('tbody', {'class' : 'b-fight-details__table-body'})[3]
                                    .findAll('p', {'class' : 'b-fight-details__table-text'})][75:90:2]
                                )
                        except:
                            pass
                    except:
                        pass
                except:
                    pass
            except:
                pass
        except:
            pass
'''


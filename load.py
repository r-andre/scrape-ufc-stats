#!/usr/bin/env python

import sqlite3
from datetime import datetime as dt
import pandas as pd
# Local:
from utils import log

log("Reading scraped data")

# Connecting to the database:
con = sqlite3.connect('ufc.db')
RAW = pd.read_sql('SELECT * FROM RAW', con)

log("Building EVENTS table")

# Building EVENTS table:
evn = RAW.loc[:, ('Event', 'Date', 'Location')]
evn['Date'] = [dt.strptime(d, r'%B %d, %Y').date() for d in evn['Date']]
evn['Date'] = pd.to_datetime(evn['Date'])
evn = evn.iloc[::-1].drop_duplicates()
evn['Id'] = [i for i in range(2, evn.shape[0] + 2)]

EVENTS = evn.set_index('Id')

log("Transforming data")

# Building temporary table for data cleaning and wrangling:
tmp = RAW.loc[:, ('Event'
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
    , 'Result'
    , 'Scoring'
    , 'Fighter')].fillna(pd.NA)

# Categorizing weight classes:
tmp = tmp.rename(columns={'Bout' : 'Info'})
tmp.loc[tmp['Info'].str.contains("Open Weight")
    , 'WeightClass'] = "Open Weight"
tmp.loc[tmp['Info'].str.contains("Heavyweight")
    , 'WeightClass'] = "Heavyweight"   
tmp.loc[tmp['Info'].str.contains("Super Heavyweight")
    , 'WeightClass'] = "Super Heavyweight"
tmp.loc[tmp['Info'].str.contains("Light Heavyweight")
    , 'WeightClass'] = "Light Heavyweight"
tmp.loc[tmp['Info'].str.contains("Middleweight")
    , 'WeightClass'] = "Middleweight"
tmp.loc[tmp['Info'].str.contains("Welterweight")
    , 'WeightClass'] = "Welterweight"
tmp.loc[tmp['Info'].str.contains("Lightweight")
    , 'WeightClass'] = "Lightweight"
tmp.loc[tmp['Info'].str.contains("Featherweight")
    , 'WeightClass'] = "Featherweight"
tmp.loc[tmp['Info'].str.contains("Bantamweight")
    , 'WeightClass'] = "Bantamweight"
tmp.loc[tmp['Info'].str.contains("Flyweight")
    , 'WeightClass'] = "Flyweight"
tmp.loc[tmp['Info'].str.contains("Women's Featherweight")
    , 'WeightClass'] = "Women's Featherweight"
tmp.loc[tmp['Info'].str.contains("Women's Bantamweight")
    , 'WeightClass'] = "Women's Bantamweight"
tmp.loc[tmp['Info'].str.contains("Women's Flyweight")
    , 'WeightClass'] = "Women's Flyweight"
tmp.loc[tmp['Info'].str.contains("Women's Strawweight")
    , 'WeightClass'] = "Women's Strawweight"
tmp.loc[tmp['Info'].str.contains("Catch Weight")
    , 'WeightClass'] = "Catch Weight"
tmp.loc[tmp['Info'].str.contains("Women's Catch Weight")
    , 'WeightClass'] = "Women's Catch Weight"

# Adding title bout information:
tmp.loc[tmp['Info'].str.contains("Title"), 'TitleBout'] = True
tmp['TitleBout'] = tmp['TitleBout'].fillna(False)

# Adding knockdowns:
tmp['KD'] = RAW['KD']

# Adding strikes:
sigstr = RAW['Sig. str.'].str.split(" of ")
tmp['SignStrikes'] = sigstr.str[0]
tmp['SignStrikesAtt'] = sigstr.str[1]

totstr = RAW['Total str.'].str.split(" of ")
tmp['TotalStrikes'] = totstr.str[0]
tmp['TotalStrikesAtt'] = totstr.str[1]

# Adding takedowns:
td = RAW['Td'].str.split(" of ")
tmp['TD'] = td.str[0]
tmp['TDAtt'] = td.str[1]

# Adding submission attempts, reversals and control time:
tmp['SubmissionAtt'] = RAW['Sub. att']
tmp['Reversal'] = RAW['Rev.']
tmp['ControlTime'] = RAW['Ctrl']
tmp.loc[tmp['ControlTime'] == "--", 'ControlTime'] = pd.NA

# Adding additional striking information:
for type in ['Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']:
    tp = RAW[type].str.split(" of ")
    tmp[type + 'Strikes'] = tp.str[0]
    tmp[type + 'StrikesAtt'] = tp.str[1]

# Converting striking information to integers:
for c in ['KD', 'SignStrikes', 'SignStrikesAtt'
    , 'TotalStrikes', 'TotalStrikesAtt'
    , 'TD', 'TDAtt', 'SubmissionAtt', 'Reversal'
    , 'HeadStrikes', 'HeadStrikesAtt'
    , 'BodyStrikes', 'BodyStrikesAtt'
    , 'LegStrikes', 'LegStrikesAtt'
    , 'DistanceStrikes', 'DistanceStrikesAtt'
    , 'ClinchStrikes', 'ClinchStrikesAtt'
    , 'GroundStrikes', 'GroundStrikesAtt']:
    tmp[c] = tmp[c].astype(float).astype('Int64')

log("Building FIGHTS table")

# Building FIGHTS table:
fgh = tmp.loc[:, (
    'Event'
    , 'Info'
    , 'WeightClass'
    , 'TitleBout'
    , 'Fight'
    , 'Winner'
    , 'Method'
    , 'Round'
    , 'Time'
    , 'Format'
    , 'Referee'
    , 'Details'
    )].iloc[::-1].drop_duplicates()
fgh['Id'] = [i for i in range(9, fgh.shape[0] + 9)]
fgh['newIndex'] = fgh['Event']
fgh = fgh.set_index('newIndex')
fgh['Event'] = EVENTS.reset_index().set_index('Event')['Id']

FIGHTS = fgh.set_index('Id')

log("Building STATS table")

# Building STATS table:
sts = tmp.rename(columns={'Winner' : 'newIndex'})
sts = sts.set_index(['Event', 'Fight', 'newIndex'])
sts['Event'] = fgh.reset_index().set_index(
    ['newIndex', 'Fight', 'Winner']
    )['Event']
sts['Fight'] = fgh.reset_index().set_index(
    ['newIndex', 'Fight', 'Winner']
    )['Id']
sts = sts.drop([
    'Info', 'WeightClass', 'TitleBout'
    , 'Method', 'Round', 'Time', 'Format'
    , 'Referee', 'Details'
    ], axis=1).reset_index(drop=True)

STATS = sts.sort_values(['Event', 'Fight']).set_index(['Event', 'Fight'])

log("Writing transformed data to database")

# Writing tables to database:
EVENTS.reset_index().to_sql('EVENTS', con, index=False, if_exists='replace')
FIGHTS.reset_index().to_sql('FIGHTS', con, index=False, if_exists='replace')
STATS.reset_index().to_sql('STATS', con, index=False, if_exists='replace')

con.close()

log("Finished")

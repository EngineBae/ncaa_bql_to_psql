import pandas as pd
import os
from google.cloud import bigquery
import config

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.bq_token
client = bigquery.Client()

print('Querying Data...')
# load bigquery tables into pandas dataframe objects
# mbb_teams
teams_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_teams`
"""
mbb_teams = client.query(teams_query).to_dataframe()

# mbb_teams_games_sr
teams_games_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr`
WHERE market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`) AND
opp_market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`)
"""
mbb_teams_games_sr = client.query(teams_games_query).to_dataframe()
mbb_teams_games_sr.insert(0, 'index', mbb_teams_games_sr.index+1)

# mbb_historical_teams_games
h_teams_games_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_games`
WHERE season >= 2013 AND 
market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`) AND
opp_market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`)
"""
mbb_historical_teams_games = client.query(h_teams_games_query).to_dataframe()
mbb_historical_teams_games.insert(0, 'index', mbb_historical_teams_games.index+1)

# mbb_historical_teams_seasons
h_teams_seasons_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons`
WHERE season >= 2013 AND 
market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`)
"""
mbb_historical_teams_seasons = client.query(h_teams_seasons_query).to_dataframe()
mbb_historical_teams_seasons.insert(0, 'index', mbb_historical_teams_seasons.index+1)

# mbb_historical_tournament_games
h_tournament_games_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
WHERE season >= 2013 AND 
win_market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`) AND
lose_market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`)
"""
mbb_historical_tournament_games = client.query(h_tournament_games_query).to_dataframe()
mbb_historical_tournament_games.insert(0, 'index', mbb_historical_tournament_games.index+1)

# mbb_players_games_sr
players_games_query = """
SELECT *
FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE season >= 2013 AND 
team_market in (SELECT market FROM `bigquery-public-data.ncaa_basketball.mbb_teams`)
"""
mbb_players_games_sr = client.query(players_games_query).to_dataframe()
mbb_players_games_sr.insert(0, 'index', mbb_players_games_sr.index+1)

print('Creating Tables...')
# creating tables in local ncaa database
table_names = ['mbb_teams', 'mbb_historical_teams_seasons', 'mbb_teams_games_sr',
               'mbb_historical_teams_games', 'mbb_historical_tournament_games', 'mbb_players_games_sr']
dataset_ref = client.dataset('ncaa_basketball', project='bigquery-public-data')
ncaa_dataset = client.get_dataset(dataset_ref)
for table in table_names:
    # get schema from bigquery
    schema = client.get_table(ncaa_dataset.table(table)).schema

    # creating a string that contains column names and types
    if table != 'mbb_teams':
        col_vals = 'index INT PRIMARY KEY, '
    else:
        col_vals = ''

    for i, column in enumerate(schema):
        if table == 'mbb_teams' and column.name == 'market':
            col_vals += column.name + ' ' + column.field_type + ' ' + 'PRIMARY KEY' + ', '
        elif column.name in ['market', 'opp_market', 'win_market', 'lose_market', 'team_market']:
            col_vals += column.name + ' ' + column.field_type + ' REFERENCES mbb_teams(market)' + ', '
        elif i + 1 == len(schema):
            col_vals += column.name + ' ' + column.field_type
        else:
            col_vals += column.name + ' ' + column.field_type + ', '

    # replacing data types to postgres-specific ones
    col_vals = col_vals.replace('STRING', 'TEXT')

    # creating a stirng for create table command
    create_table = 'CREATE TABLE IF NOT EXISTS ' + table + '(' + col_vals + ');'
    config.curs_ncaa.execute(create_table)

print('Inserting Data...')
# inserting data into the tables
# replacing NaT with None values so that we don't get the integer out of range error
mbb_players_games_sr = mbb_players_games_sr.where((pd.notnull(mbb_players_games_sr)), None)
mbb_historical_tournament_games = mbb_historical_tournament_games.where((pd.notnull(mbb_historical_tournament_games)), None)
mbb_teams_games_sr = mbb_teams_games_sr.where((pd.notnull(mbb_teams_games_sr)), None)
dataframes = [mbb_teams, mbb_historical_teams_seasons, mbb_teams_games_sr,
              mbb_historical_teams_games, mbb_historical_tournament_games, mbb_players_games_sr]
for index, df in enumerate(dataframes):
    insert_data = 'INSERT INTO ' + table_names[index] + ' VALUES ('
    for i in range(len(df.columns)):
        if i == len(df.columns)-1:
            insert_data += '%s) on CONFLICT DO NOTHING;'
        else:
            insert_data += '%s, '
    print(table_names[index])
    df.apply(lambda row: config.curs_ncaa.execute(insert_data, row.values), axis=1)

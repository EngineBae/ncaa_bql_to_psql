import psycopg2

# starting ssh server connection
# if following instructions from GitHub, keep it as 127.0.0.1
local_host = '127.0.0.1'

# connecting to local ncaa database
conn_ncaa = psycopg2.connect(host=local_host,
                             port=5432,
                             dbname='ncaa',
                             user='<postgres username>')
conn_ncaa.set_session(autocommit=True)
curs_ncaa = conn_ncaa.cursor()

'''
once the bigquery token json file is downloaded, 
make sure that the directory to the token file is indicated below
for example, '/Users/john/Documents/NCAA/My First Project-6dcc578bf852.json'
'''
bq_token = '~/bigquery_token_json'
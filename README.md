# NCAA BigQuery to PostgreSQL Migration

This project migrates data from the NCAA dataset from BigQuery's public datasets to a local Postgres database to increase ease/speed of data extraction and enforce restrictions on the available data so that cross-table joins can be completed without issues.

## Steps

1. Follow [this guide](https://cloud.google.com/docs/authentication/getting-started) to receive the BigQuery token
2. Script requires python 3.7.3+
3. Install Brew package managers
    * `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
4. Install postgres - `brew install postgresql`
5. Create NCAA database
    * `psql postgres`
    * `create database ncaa;`
6. Createa a virtual envrionment for script
    * install virtual environment - `pip install virtualenv`
    * create virtual environment - `virtualenv <target>`
    * activate virtual environment - `source <target>/bin/activate`
7. `pip install -r requirements.txt` to install all requirements for script
8. Double check to make sure that `config.py` is properly set up. Information available in the script

## To Run
`sudo python bq_to_psql.py`

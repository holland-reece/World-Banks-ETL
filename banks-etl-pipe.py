# ETL Pipe for Large Banks (Final Project)
# IBM Data Engineering Foundations, Module 3: Python Project for Eng

# Updated 2025-09-15
# Created 2025-09-10

# Holland Brown (https://github.com/holland-reece)

# Description:
    # 2nd project for "IBM Data Engineering Foundations"
    # training program, Course 3: "Python Project for 
    # Engineering"

# -------------------------------------------------------

#%% Importing the required libraries
import os
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime
from typing import Union
import csv


def extract(url, table_attribs):
    html_page = requests.get(url).text # load webpage for webscraping
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody') # extract desired info from website

    # Find table I'm looking under heading 'By market capitalization'
    h2 = data.find(id="By_market_capitalization") # find section heading
    if h2:
        h2 = h2.find_parent(["h2", "h3"]) # id on <span>; go up to <h2>
        table = h2.find_next("table", class_="wikitable") # find table
        # print(f'Table exists on webpage: {table is not None}') # check

        # Get position of table (relative to all tables on page)
        all_tables = data.find_all("table")
        pos = all_tables.index(table) if table in all_tables else None

    # Extract and format data from target table
    rows = tables[pos].find_all('tr') # select all rows in table
    records = [] # init empty list to store dict pairs

    for row in rows:
        tds = row.find_all('td')
        if not tds:
            continue
        bank_names = tds[1].find("a", recursive=False) # direct child
        bank = bank_names.get_text(strip=True) # get only display title
        market_cap = tds[2].get_text(strip=True) # get market cap values
        records.append({"Name": bank, "MC_USD_Billion": market_cap})

    df1 = pd.DataFrame.from_records(records, columns=["Name", 
                                                      "MC_USD_Billion"])
    df = df1.rename({"Name":table_attribs[0],
                     "MC_USD_Billion":table_attribs[1]}, axis='columns')
    
    for i in range(len(df['MC_USD_Billion'])): # clean up market cap col
        if df.loc[i,'MC_USD_Billion'] != ' ': # remove whitespace
            df.loc[i,'MC_USD_Billion'] = df.loc[i,'MC_USD_Billion'].replace(' ', '')
        df.loc[i,'MC_USD_Billion'] = float(df.loc[i,'MC_USD_Billion'])

    log_progress(f'Data extraction complete. Initiating Transformation process')
    return df

def transform(df, input_csv):
    exchange_dict = {}
    with open(input_csv, 'r', newline='', encoding='utf-8') as c:
        reader = csv.reader(c)
        next(reader)  # skip the header row
        for row in reader:
            if len(row) >= 2:  # make sure there are at least two cols
                key = row[0]
                value = float(row[1].replace(' ', '')) # string to float
                exchange_dict[key] = value

    # Use exchange rates to add three currency cols to df
    df['MC_GBP_Billion'] = [np.round(x*exchange_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_dict['INR'],2) for x in df['MC_USD_Billion']]
    log_progress(f'Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    log_progress(f'Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)
    log_progress(f'Data loaded to Database as a table, Executing queries')

def run_query(query_statement: Union[str,list], sql_connection):
    if isinstance(query_statement, list): # run list of multiple queries
        query_output = []
        for q in query_statement:
            query_output = pd.read_sql(q, sql_connection)
            print(f'\n\n{query_statement}\n')
            print(query_output)
    elif isinstance(query_statement, str): # run single given query
        query_output = pd.read_sql(query_statement, sql_connection)
        print(f'{query_statement}\n')
        print(query_output)
    log_progress(f'Process Complete')

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    if os.path.isfile(log_file): # append message to existing file
        with open(log_file, "a") as f:
            f.write(f'{timestamp} : {message}\n')
    else:
        with open(log_file, "w") as f: # create new file before writing
            f.write(f'------------------------------------------------\n{timestamp} : Initializing New Log File\n------------------------------------------------\n')




# Initialize known entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = f"{os.getcwd()}/exchange_rate.csv"
table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
log_file = f"{os.getcwd()}/code_log.txt" # full path to log file
csv_path = f"{os.getcwd()}/Largest_banks_data.csv" # full path
db_name = 'Banks.db' # name of the database that will be created
table_name = 'Largest_banks' # name of the table to be saved in db

# Set up log
log_progress(f'Preliminaries complete. Initiating ETL process\n')

# Extract data from website and ingest
df = extract(url, table_attribs)

# Transform the data
df = transform(df, exchange_rate_csv) # in: extracted df, input CSV

# Load Step A: save data in a CSV file
load_to_csv(df, csv_path)

# Load Step B: save the data as a table in the database
conn = sqlite3.connect(db_name) # define connection variable
log_progress('SQL Connection initiated')
load_to_db(df, conn, table_name)

# Querying the data
q1 = f"SELECT * FROM {table_name}" # print contents of entire table
q2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}" # avg market cap
q3 = f"SELECT Name from {table_name} LIMIT 5" # total number of entries
run_query([q1, q2, q3], conn) # in: list of strings or single str

conn.close() # close SQL server connection
log_progress(f'Server Connection closed\n\n')


# %%

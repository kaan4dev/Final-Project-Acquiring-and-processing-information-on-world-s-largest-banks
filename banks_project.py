import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
exchange_rate_csv = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
log_file = "code_log.txt"
output_csv_path = "./Largest_banks_data.csv"
table_name = "Largest_banks"

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('table')
    for table in tables:
        heading = table.find_previous('h2')
        if heading and "By market capitalization" in heading.text:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) > 1:
                    print("Columns:", cols)  # Debug
                    bank_name = cols[1].find('a').text if cols[1].find('a') else cols[1].text.strip()
                    market_cap = float(cols[2].text.strip()[:-1])
                    data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
    print("Extracted Data:", df)
    return df

def transform(df, csv_path):
    try:
        exchange_rate_df = pd.read_csv(csv_path)
        exchange_rate = dict(zip(exchange_rate_df['Currency'], exchange_rate_df['Rate']))
        df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    except Exception as e:
        print("Error in transform:", e)
    print("Transformed Data:", df)
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name,sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
    
df = transform(df, exchange_rate_csv)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

run_query("SELECT * FROM Largest_banks LIMIT 5", sql_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query("SELECT Name from Largest_banks LIMIT 5", sql_connection)

log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")
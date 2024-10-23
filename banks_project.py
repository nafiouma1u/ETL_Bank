import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# Constants
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'C:\Users\NAFIOU MAHAMANE\Desktop\Etl_Banks_Project\exchange_rate.csv'
output_csv = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

# Task 1: Logging progress
def log_progress(message):
    timestamp_format = '%Y-%h-%d %H:%M:%S'  # Year-Monthname-Day Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} - {message}\n")

log_progress("Script started")

# Task 2: Data extraction
def extract():
    log_progress("Data extraction started")
    
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    
    # Finding the table under the heading 'By market capitalization'
    tables = soup.find_all('table', {'class': 'wikitable'})
    
    # Assuming the first table is the correct one
    rows = tables[0].find_all('tr')

    bank_data = []
    for row in rows[1:11]:  # Only the top 10 banks
        cols = row.find_all('td')
        name = cols[1].text.strip()
        mc_usd = float(cols[2].text.strip().replace(',', ''))  # Market Capitalization in USD
        bank_data.append({"Name": name, "MC_USD_Billion": mc_usd})
    
    df = pd.DataFrame(bank_data)
    log_progress("Data extraction completed")
    return df

# Task 3: Data transformation
def transform(df):
    log_progress("Data transformation started")

    # Loading exchange rates
    exchange_rates = pd.read_csv(exchange_rate_csv)
    usd_to_gbp = exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    usd_to_eur = exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    usd_to_inr = exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]

    # Adding new columns for GBP, EUR, INR
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * usd_to_gbp, 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * usd_to_eur, 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * usd_to_inr, 2)

    log_progress("Data transformation completed")
    return df

# Task 4: Load to CSV
def load_to_csv(df):
    log_progress("Saving data to CSV started")
    df.to_csv(output_csv, index=False)
    log_progress("Data saved to CSV successfully")

# Task 5: Load to SQL database
def load_to_db(df):
    log_progress("Saving data to SQL database started")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress("Data saved to SQL database successfully")

# Task 6: Database queries (as needed)
def run_queries():
    log_progress("Running queries on database started")
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Example: Fetch all data
    cur.execute(f"SELECT * FROM {table_name}")
    results = cur.fetchall()
    print(results)

    conn.close()
    log_progress("Database queries executed successfully")

# Execution of tasks
if __name__ == "__main__":
    df_extracted = extract()  # Task 2
    df_transformed = transform(df_extracted)  # Task 3
    load_to_csv(df_transformed)  # Task 4
    load_to_db(df_transformed)  # Task 5
    run_queries()  # Task 6
    log_progress("Script completed")

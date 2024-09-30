"""
Country level production, consumption, imports, exports by energy source (petroleum, natural gas, electricity, renewable, etc.)
From Dec 2002 to June 2024 
Interactive product: https://www.eia.gov/international/data/world

Demonstration of importing the API data and writing it to a SQLite database, followed by a query to retrieve the data.
"""

import config
import sqlite3
import requests
import pandas as pd

def retrieve_and_write_data():
    data = get_data()
    sorted_data = sort_data(data)
    write_to_db(sorted_data)

def get_data():
    EIA_API_KEY = config.EIA_API_KEY
    API_ROUTE = "https://api.eia.gov/v2/international/data/?frequency=monthly&data[0]=value&start=2002-12&end=2024-06&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
    response = requests.get(API_ROUTE, headers={"X-Api-Key": EIA_API_KEY})
    if response.status_code == 200:
        data = response.json()
        return data['response']['data']
    else:
        print(f"Error: {response.status_code}")
        return []

def sort_data(data):
    df = pd.DataFrame(data)
    # Sort by period
    df_sorted = df.sort_values(by='period', ascending=False)
    return df_sorted

def write_to_db(data):
    conn = sqlite3.connect('energy_data.db')
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS energy_data (
                        country TEXT,
                        period TEXT,
                        energy_source TEXT,
                        value REAL,
                        unit TEXT)''')

    for index, row in data.iterrows():
        cursor.execute('''INSERT INTO energy_data (country, period, energy_source, value, unit) 
                          VALUES (?, ?, ?, ?, ?)''', 
                          (row['countryRegionName'], row['period'], row['productName'], row['value'], row['unit']))

    conn.commit()
    conn.close()

def query_data():
    conn = sqlite3.connect('energy_data.db')
    cursor = conn.cursor()

    # Example query: get all data for a specific country and energy source
    country = 'United States'
    energy_source = 'Other liquids'
    cursor.execute('''SELECT * FROM energy_data WHERE country=? AND energy_source=?''', (country, energy_source))
    
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    conn.close()

def main():
    # retrieve_and_write_data()
    query_data()

if __name__ == "__main__":
    main()

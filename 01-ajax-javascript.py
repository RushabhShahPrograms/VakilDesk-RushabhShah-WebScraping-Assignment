#Importing Libraries
import requests
from bs4 import BeautifulSoup
import psycopg2
import logging
import pandas as pd

# Logger setup
logging.basicConfig(filename='ajax-scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL config
DB_HOST = 'localhost'
DB_NAME = 'VD_Scraper'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_PORT = '5432'

#Creating Table
def create_table(cursor):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS films (
        title TEXT,
        nominations INTEGER,
        awards INTEGER,
        best_picture INTEGER,
        year INTEGER
    );
    '''
    cursor.execute(create_table_query)

#Inserting the data
def insert_data(cursor, data):
    insert_query = '''
    INSERT INTO films (title, nominations, awards, best_picture, year) VALUES (%s, %s, %s, %s, %s)
    '''
    cursor.executemany(insert_query, data)

#Scraping of the data from url
def fetch_and_process_data():
    url = "https://www.scrapethissite.com/pages/ajax-javascript/"
    try:
        page = requests.get(url, timeout=10)
        page.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Error fetching the initial page: {e}")
        return []

    soup = BeautifulSoup(page.content, 'html.parser')
    years = ['2010', '2011', '2012', '2013', '2014', '2015'] #scrape the data as per year
    all_data = []

    for year in years:
        year_link = soup.find('a', {'id': year})
        if year_link:
            ajax_url = url + f"?ajax=true&year={year_link.get('id')}"
            try:
                ajax_response = requests.get(ajax_url, timeout=10)
                ajax_response.raise_for_status()
                ajax_data = ajax_response.json()
            except requests.RequestException as e:
                logging.error(f"Error fetching data for year {year}: {e}")
                continue

            for film_data in ajax_data:
                title = film_data['title']
                nominations = film_data['nominations']
                awards = film_data['awards']
                best_picture = 1 if film_data.get('best_picture', False) else 0
                all_data.append((title, nominations, awards, best_picture, year))

    return all_data


def main():
    data = fetch_and_process_data()
    if not data:
        logging.info("No data to insert into the database.")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        create_table(cursor)
        insert_data(cursor, data)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data successfully inserted into the database.")
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
#Import Libraries
import requests
from bs4 import BeautifulSoup
import psycopg2
import logging

# Configure logging
logging.basicConfig(filename='scraper-forms.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

def get_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch HTML content: {e}")
        return None
    
# Postgres Config
DB_PARAMS = {
    'dbname': 'VD_Scraper',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Creating database
def create_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None
    
# Create table
def create_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS scraped_data_forms (
        id SERIAL PRIMARY KEY,
        team_name TEXT,
        year INTEGER,
        wins INTEGER,
        losses INTEGER,
        ot_losses INTEGER NULL,
        win_per DECIMAL,
        goals_for INTEGER,
        goals_against INTEGER,
        plus_minus INTEGER
    );
    '''
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
    except Exception as e:
        logging.error(f"Table creation failed: {e}")

# Inserting data into table
def insert_data(conn, data):
    insert_query = '''
    INSERT INTO scraped_data_forms (team_name, year, wins, losses, ot_losses, win_per, goals_for, goals_against, plus_minus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (
                data['Team Name'],
                int(data['Year']),
                int(data['Wins']),
                int(data['Losses']),
                int(data['OT Losses']) if data['OT Losses'] else None,
                float(data['Win %']),
                int(data['Goals For (GF)']),
                int(data['Goals Against (GA)']),
                int(data['+ / -'])
            ))
            conn.commit()
    except Exception as e:
        logging.error(f"Data insertion failed: {e}")


# As this page contains pagination so we are iterating through each page and fetching out the data of table present.
current_page = 1
last_page_reached = False

# Database Connection
conn = create_db_connection()
if conn:
    create_table(conn)

    while not last_page_reached:
        url = f"https://www.scrapethissite.com/pages/forms/?page_num={current_page}"

        html_content = get_html_content(url)
        if html_content is None:
            break

        soup = BeautifulSoup(html_content, 'html.parser')

        tr_elements = soup.find_all('tr', class_='team')

        # Checking for last page
        if not tr_elements:
            last_page_reached = True
            break

        for tr in tr_elements[1:]: #adding[1:] so that it can start from second value not from the first value as first value was empty
            td_elements = tr.find_all('td')
            td_texts = [td.get_text(strip=True) for td in td_elements]

            output = {
                'Team Name': td_texts[0],
                'Year': td_texts[1],
                'Wins': td_texts[2],
                'Losses': td_texts[3],
                'OT Losses': td_texts[4],
                'Win %': td_texts[5],
                'Goals For (GF)': td_texts[6],
                'Goals Against (GA)': td_texts[7],
                '+ / -': td_texts[8]
            }

            insert_data(conn, output)
        current_page += 1

    # Shut the connection
    conn.close()
else:
    logging.error("Failed to create database connection.")
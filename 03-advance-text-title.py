#Import Libraries
import requests
from bs4 import BeautifulSoup
import psycopg2
import logging
from psycopg2 import sql

# Configure logging
logging.basicConfig(filename='scrap-advance.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

# Database connection parameters
DB_PARAMS = {
    'dbname': 'VD_Scraper',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# database connection
def create_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

# create table
def create_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS scraped_data_advanced (
        id SERIAL PRIMARY KEY,
        header TEXT,
        url TEXT,
        paragraph TEXT
    );
    '''
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
    except Exception as e:
        logging.error(f"Table creation failed: {e}")

# insert data into table
def insert_data(conn, header, url, paragraph):
    insert_query = sql.SQL('''
    INSERT INTO scraped_data_advanced (header, url, paragraph)
    VALUES (%s, %s, %s)
    ''')
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (header, url, paragraph))
            conn.commit()
    except Exception as e:
        logging.error(f"Data insertion failed: {e}")


# Scraping
def scrape_website(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        #fetch head and urls
        head4_tags = soup.find_all('h4')
        headers = [(tag.text.strip(), tag.find('a')['href']) for tag in head4_tags]
        
        # fetch paras, skip the first one
        para_tags = soup.find_all('p')
        paras = [para.text.strip() for para in para_tags[1:]]

        return headers, paras
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return [], []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return [], []
    
def main():
    url = "https://www.scrapethissite.com/pages/advanced/"
    conn = create_db_connection()
    
    if conn:
        create_table(conn)
        headers, paras = scrape_website(url)
        
        if headers and paras:
            for (header, href), para in zip(headers, paras):
                insert_data(conn, header, href, para)
        
        conn.close()

if __name__ == "__main__":
    main()
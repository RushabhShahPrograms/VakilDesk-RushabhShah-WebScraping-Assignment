# VakilDesk-RushabhShah-WebScraping-Assignment

#### Documentation
Installation
```python
pip install requests
pip install psycopg2
pip install pandas
pip install beautifulsoup4
pip install logger
```

For this assignment I have used Postgresql for storing the data that we scrape. For connecting it with postgresql you have to change this code with your config

This config is for my setup
```python
DB_HOST = 'localhost' #type your host_name
DB_NAME = 'VD_Scraper' #type your #db_name
DB_USER = 'postgres' #type your user_name
DB_PASSWORD = 'postgres' #type your password
DB_PORT = '5432' #type your port but default one is 5432
```

After installation of libraries and configuring the connection with postgresql. You can run those 3 py files provided in this repo one by one.

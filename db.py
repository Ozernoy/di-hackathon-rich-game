import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor, RealDictCursor
from stock_api import StockClient
from settings import *
from utils import process_string, add_quotes
import settings
import pandas as pd

class DB:

    def __init__(self, host, db_name, password, username, port='5432', autocommit=True) -> None:
        self.host = host
        self.db_name = db_name
        self.password = password
        self.username = username
        self.port = port

    def create_db(self):
        """Create a new database."""
        try:
            self.init_connection(dbname='postgres')  # Connect to default 'postgres' first

            with self.connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE {self.db_name};')
                print(f"Database '{self.db_name}' created successfully.")

        except psycopg2.Error as e:
            print(f"An error occurred while creating the database: {e}")
        finally:
            self.close_db_if_necessary()  # Ensure the connection to 'postgres' is closed even if we got error

        self.init_connection()  # Reinitialize connection to the newly created database

    def init_connection(self, dbname=None, autocommit=True):
        """Initialize a connection to the database."""
        dbname = dbname or self.db_name  # Default to the database name provided during initialization
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.connection.autocommit = autocommit
    
    # instead of using __del__
    def close_db_if_necessary(self):
        """Close the database connection if it is open."""
        if self.connection:
            self.connection.close()

    @property
    def conn(self):
        return self.connection
    
    def cursor(self):
        """Return a database cursor."""
        return self.connection.cursor(cursor_factory=NamedTupleCursor)

    def execute(self, query):
        """Execute a database query."""
        with self.cursor() as cursor:
            cursor.execute(query)
            return cursor
    
    def fetchone(self, query):
        """Fetch a single row from a database query."""
        with self.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()
    
    def fetchall(self, query):
        """Fetch all rows from a database query."""
        with self.cursor() as cursor:
            cursor.execute(query) 
            return cursor.fetchall()
    
    def create_tables(self, drop=False):
        if drop:
            self.execute("DROP TABLE IF EXISTS companies CASCADE;")
            self.execute("DROP TABLE IF EXISTS stock_prices;")

        self.execute("""
            CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            DESCRIPTION TEXT NOT NULL
            -- date_listing DATE NOT NULL
            );

            CREATE TABLE stock_prices (
            stock_price_id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES companies (company_id) ON DELETE CASCADE,
            price_date DATE NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume NUMERIC NOT NULL,
            adjusted_close NUMERIC NOT NULL,
            UNIQUE(company_id, price_date)
            );
                     
            CREATE INDEX company_id_idx ON stock_prices (company_id);
        """)

    def drop_database(self):
        """
        Drops the entire database. Connects to the 'postgres' database first and ensures all active connections are terminated.
        """
        try:
            # Close current connection to the target database
            self.close_db_if_necessary()

            # Connect to 'postgres' to perform the drop
            self.init_connection(dbname='postgres')

            with self.connection.cursor() as cursor:
                # Terminate all connections to the target database
                cursor.execute(f"""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{self.db_name}' AND pid <> pg_backend_pid();
                """)
                print(f"Terminated all active connections to database '{self.db_name}'.")

                # Drop the database
                cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
                print(f"Database '{self.db_name}' dropped successfully.")

        except psycopg2.Error as e:
            print(f"An error occurred while dropping the database '{self.db_name}': {e}")
        finally:
            self.close_db_if_necessary()
            self.connection = None  # Reset connection


    def get_companies_stock_price(self, companies_id, start_date=None, end_date=None):
        """Create a filtered table for specific companies."""
        query = """
            SELECT name, symbol, sp.*,
                date_part('year', price_date) AS year,
                date_part('month', price_date) AS month
            FROM stock_prices sp
            JOIN companies c ON sp.company_id = c.company_id
            WHERE c.company_id IN ({})
        """.format(', '.join(map(str, companies_id)))

        if start_date:
            query += f" AND price_date >= '{start_date}'"
        
        if end_date:
            query += f" AND price_date <= '{end_date}'"

        print(query)
        return self.fetchall(query)

    def get_company(self, symbol):
        """Retrieve a company from the database by its symbol."""
        query = f"SELECT * FROM companies WHERE symbol = '{symbol}' limit 1;"
        return self.fetchone(query)
    
    def get_random_companies(self, n):
        query = f"SELECT * FROM companies ORDER BY RANDOM() LIMIT {n};"
        return self.fetchall(query)
    
    def get_companies(self):
        query = "SELECT * FROM companies;"
        return self.fetchall(query)

    def add_company(self, name, symbol, description):
        """Add a company to the database."""
        name = process_string(name)
        symbol = process_string(symbol)
        description = process_string(description)
        query = f"INSERT INTO companies (name, symbol, description) VALUES ({name}, {symbol}, {description});"
        self.execute(query)

    def add_companies_from_csv(self, filename):
        """Add companies from a CSV file to the database."""
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                name, symbol, description = row
                self.add_company(name, symbol, description)

    def insert_stock_prices(self, company_id, sr_date, sr_open, sr_high, sr_low, sr_close, volume, adjusted_close):
        """Insert a stock rate into the database."""
        sr_date = add_quotes(sr_date)
        query = f"""
            INSERT INTO stock_prices (company_id, price_date, open, high, low, close, volume, adjusted_close)
            VALUES ({company_id}, {sr_date}, {sr_open}, {sr_high}, {sr_low}, {sr_close}, {volume}, {adjusted_close});
        """
        self.execute(query)

    def add_stock_price_company_id(self, company_id, symbol):
        """Add stock history for a company."""
        data = StockClient.get_ts_monthly(symbol)
        print(list(data.keys()), data)
        for date, values in data['Monthly Adjusted Time Series'].items():
            try:
                self.insert_stock_prices(
                    company_id=company_id,
                    sr_date=date,
                    sr_open=values['1. open'],
                    sr_high=values['2. high'],
                    sr_low=values['3. low'],
                    sr_close=values['4. close'],
                    volume=values['6. volume'],
                    adjusted_close=values['5. adjusted close']
                )
            except Exception as e:
                print(date, values)
                print(e.__class__.__name__, e)

    def add_stock_price(self, symbol):
        """Add stock history for a company."""
        company_id = self.get_company(symbol)
        print(company_id)
        company_id = company_id.company_id
        self.add_stock_price_company_id(company_id, symbol)
    
    def add_stock_price_all(self, companies):
        """Add stock history for a company."""
        for c in companies: 
            self.add_stock_price_company_id(c.company_id, c.symbol)


def test():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

    db.create_db()
    db.create_tables()

    # try:
    #     db.create_db()
    #     db.create_tables()
    # except:
    #     pass

    # db.add_company('Apple', 'AAPL', 'Apple Inc.')
    # db.add_stock_price_all('AAPL')

def main():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
    db.init_connection(db.db_name)
    print('API_KEY:', StockClient.api_key)

    # db.create_db()
    # db.create_tables()
    
    # try:
    #     db.create_db()
    #     db.create_tables()
    # except Exception as e:
    #     print(e)

    db.add_companies_from_csv('companies.csv')
    companies = db.get_companies()[:25]
    db.add_stock_price_all(companies)


if __name__ == '__main__':
    # test()
    main()
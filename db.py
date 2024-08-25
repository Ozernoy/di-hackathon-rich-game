# db.py

import psycopg2
from psycopg2.extras import NamedTupleCursor
from stock_api import StockClient
from settings import *
from utils import process_string, add_quotes
import settings

class DB:

    def __init__(self, host, db_name, password, username, port='5432', autocommit=True) -> None:
        self.host = host
        self.db_name = db_name
        self.password = password
        self.username = username
        self.port = port

    def create_db(self):
        """Create a new database using the existing connection."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE {self.db_name};')
                print(f"Database '{self.db_name}' created successfully.")
        
        except psycopg2.Error as e:
            print(f"An error occurred while creating the database: {e}")

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
    
    #Create DB and add tables
    def create_tables(self):
        """Create the necessary tables in the database."""
        self.execute("""
            CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            symbol VARCHAR(50) NOT NULL UNIQUE,
            DESCRIPTION TEXT NOT NULL
            -- date_listing DATE NOT NULL
            );

            CREATE TABLE stock_rate (
            stock_rate_id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES companies (company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume NUMERIC NOT NULL,
            adjusted_close NUMERIC NOT NULL,
            UNIQUE(company_id, date)
            );
                     
            CREATE INDEX company_id_idx ON stock_rate (company_id);
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


    def get_filtered_table(self, available_companies):
        """Create a filtered table for specific companies."""
        self.execute(f"""
                    CREATE TABLE filtered_table AS
                    SELECT company_id, name, date, adjusted_close
                     FROM stock_rate
                     LEFT JOIN companies ON stock_rate.company_id = companies.company_id
                    WHERE name IN {available_companies}

        """)

    def get_company(self, symbol):
        """Retrieve a company from the database by its symbol."""
        query = f"SELECT * FROM companies WHERE symbol = '{symbol}' limit 1;"
        return self.fetchone(query)
    
    def add_company(self, name, symbol, description):
        """Add a company to the database."""
        name = process_string(name)
        symbol = process_string(symbol)
        description = process_string(description)
        query = f"INSERT INTO companies (name, symbol, description) VALUES ({name}, {symbol}, {description});"
        self.execute(query)

    def insert_stock_rate(self, company_id, sr_date, sr_open, sr_high, sr_low, sr_close, volume, adjusted_close):
        """Insert a stock rate into the database."""
        sr_date = add_quotes(sr_date)
        query = f"""
            INSERT INTO stock_rate (company_id, date, open, high, low, close, volume, adjusted_close)
            VALUES ({company_id}, {sr_date}, {sr_open}, {sr_high}, {sr_low}, {sr_close}, {volume}, {adjusted_close});
        """
        self.execute(query)

    def add_stock_history_all(self, symbol):
        """Add stock history for a company."""
        company_id = self.get_company(symbol)
        #print(company_id)
        company_id = company_id.company_id
        data = StockClient.get_ts_monthly(symbol)
        for date, values in data['Monthly Adjusted Time Series'].items():
            try:
                self.insert_stock_rate(
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
                #print(date, values)
                #print(e.__class__.__name__, e)
                pass


    def add_all_companies(self, default_description="Default company description"):
        """
        Retrieve all symbols and names from the StockClient and add them to the companies table.
        """
        stock_client = StockClient()
        symbols_and_names = stock_client.get_symbols_and_names()
        counter = 0
        for symbol, name in symbols_and_names:
            try:
                self.add_company(name, symbol, default_description)
                counter += 1
            except psycopg2.Error as e:
                print(f"Error adding company {name} ({symbol}): {e}")
        print(f"Added {counter} companies.")

    def get_all_companies(self):
        """Retrieve all companies from the database."""
        query = "SELECT * FROM companies;"
        return self.fetchall(query)
    
    def get_companies_by_criteria(self, ids=None, names=None, symbols=None):
        """Retrieve companies based on provided criteria: ids, names, or symbols."""
        conditions = []
        if ids:
            conditions.append("company_id IN ({})".format(','.join(map(str, ids))))
        if names:
            formatted_names = ','.join("'{}'".format(name) for name in names)
            conditions.append("name IN ({})".format(formatted_names))
        if symbols:
            formatted_symbols = ','.join("'{}'".format(symbol) for symbol in symbols)
            conditions.append("symbol IN ({})".format(formatted_symbols))
        
        where_clause = " OR ".join(conditions)
        query = "SELECT * FROM companies WHERE {};".format(where_clause)
        return self.fetchall(query)
    
    def add_stock_history_for_selected_companies(self, ids=None, names=None, symbols=None):
        """Add stock history for selected companies based on IDs, names, or symbols."""
        companies = self.get_companies_by_criteria(ids=ids, names=names, symbols=symbols)
        for company in companies:
            symbol = company.symbol
            print(f"Adding stock history for {company.name} ({symbol})")
            try:
                self.add_stock_history_all(symbol)
            except Exception as e:
                print(f"Failed to add stock history for {symbol}: {e}")


def test():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

    try:
        # If the database does not exist, create it
        db.init_connection(dbname='postgres')
        db.create_db()
        db.close_db_if_necessary()  # Close the connection to 'postgres' after creating the database
        
        # Now connect to the newly created database
        db.init_connection()
        db.create_tables()
        #db.drop_database()
        db.add_all_companies()
        db.add_stock_history_for_selected_companies(ids=list(range(1, 10)))
    finally:
        db.close_db_if_necessary()

if __name__ == '__main__':
    test()
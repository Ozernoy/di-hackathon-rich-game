import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor, RealDictCursor
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
    
    def create_tables(self, drop=False):
        if drop:
            self.execute("DROP TABLE IF EXISTS companies CASCADE;")
            self.execute("DROP TABLE IF EXISTS stock_prices;")

        self.execute("""
            CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            symbol VARCHAR(50) NOT NULL UNIQUE,
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

        # Create the users table
        self.execute("""
            CREATE TABLE users (
            record_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            user_name TEXT NOT NULL,
            company_id INTEGER REFERENCES companies (company_id) ON DELETE CASCADE
            );
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
        query = f"""
            INSERT INTO companies (name, symbol, description) 
            VALUES ({name}, {symbol}, {description})
            ON CONFLICT (symbol) DO UPDATE
            SET name = EXCLUDED.name,
                description = EXCLUDED.description;
        """
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
        print(list(data.keys()))
        if 'Monthly Adjusted Time Series' not in data:
                print(f"No valid stock data found for symbol: {symbol}")
                return
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
    
    def get_companies_no_stock_history(self):
        ''' Get all the companies which have no stock history yet. '''
        query = """
            select DISTINCT c.* from companies c 
            left join stock_prices sp using (company_id)
            where sp.stock_price_id is null;
        """
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
                self.add_stock_price_by_symbol(symbol)
            except Exception as e:
                print(f"Failed to add stock history for {symbol}: {e}")
    

    def get_stock_history_date_range(self):
        """
        Find the latest start date and the earliest end date of stock history intersection across all companies.
        Returns a tuple (latest_start_date, earliest_end_date).
        """
        # Query to find the latest start date (i.e., the maximum of the earliest dates for all companies)
        query_latest_start_date = """
            SELECT MAX(start_date) AS latest_start_date
            FROM (
                SELECT MIN(price_date) AS start_date
                FROM stock_prices
                GROUP BY company_id
            ) AS subquery;
        """

        # Query to find the earliest end date (i.e., the minimum of the latest dates for all companies)
        query_earliest_end_date = """
            SELECT MIN(end_date) AS earliest_end_date
            FROM (
                SELECT MAX(price_date) AS end_date
                FROM stock_prices
                GROUP BY company_id
            ) AS subquery;
        """

        latest_start_date = self.fetchone(query_latest_start_date).latest_start_date
        earliest_end_date = self.fetchone(query_earliest_end_date).earliest_end_date

        return latest_start_date, earliest_end_date
    
    def exclude_outside_date_range(self):
        """
        Exclude (delete) all rows from the stock_rate table that fall outside 
        the intersection of the date ranges across all companies.
        """
        # Get the latest start date and earliest end date that overlap across all companies
        latest_start_date, earliest_end_date = self.get_stock_history_date_range()

        # Delete rows outside this date range
        delete_query = f"""
            DELETE FROM stock_prices
            WHERE price_date < '{latest_start_date}' OR price_date > '{earliest_end_date}';
        """

        # Execute the delete query
        self.execute(delete_query)
        print(f"Deleted rows outside the date range {latest_start_date} to {earliest_end_date}.")


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


company_symbols = [
    "AAPL",  # Apple Inc.
    "MSFT",  # Microsoft Corporation
    "GOOGL",  # Alphabet Inc. (Google)
    "AMZN",  # Amazon.com Inc.
    "FB",  # Meta Platforms Inc. (formerly Facebook)
    "TSLA",  # Tesla Inc.
    "BRK.B",  # Berkshire Hathaway Inc. (Class B)
    "NVDA",  # NVIDIA Corporation
    "JPM",  # JPMorgan Chase & Co.
    "V",  # Visa Inc.
    "JNJ",  # Johnson & Johnson
    "WMT",  # Walmart Inc.
    "PG",  # Procter & Gamble Co.
    "DIS",  # The Walt Disney Company
    "NFLX"  # Netflix Inc.
]


db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

def test():

    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

    try:
        # If the database does not exist, create it
        db.init_connection(dbname='postgres')
        db.create_db()
        db.close_db_if_necessary()  # Close the connection to 'postgres' after creating the database
        
        # Now connect to the newly created database
        db.init_connection()

        #db.create_tables()
        #db.drop_database()
        #db.add_all_companies()
        db.add_stock_history_for_selected_companies(symbols=company_symbols)
        db.exclude_outside_date_range()
    finally:
        db.close_db_if_necessary()

def main():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
    #db.init_connection(dbname='postgres')
    #db.create_db()
    #db.close_db_if_necessary()  # Close the connection to 'postgres' after creating the database
    db.init_connection(db.db_name)
    #db.create_tables()
    print('API_KEY:', StockClient.api_key)

    # db.create_db()
    # db.create_tables()
    
    # try:
    #     db.create_db()
    #     db.create_tables()
    # except Exception as e:
    #     print(e)

    #db.add_companies_from_csv('companies.csv')
    companies = db.get_companies_no_stock_history()
    # companies = db.get_companies()[:25]
    db.add_stock_price_all(companies)
    # print(len(companies), companies)


if __name__ == '__main__':
    # test()
    main()
import psycopg2
from psycopg2.extras import NamedTupleCursor
from stock_api import StockClient
from settings import *
from utils import process_string, add_quotes

class DB:
    def __init__(self, host, db_name, password, username, port='5432', autocommit=True) -> None:
        self.host = host
        self.db_name = db_name
        self.password = password
        self.username = username
        self.port = port

    def create_db(self):
        conn = psycopg2.connect(
            dbname = 'postgres',
            user = self.username,
            password = self.password,
            host = self.host,
            port = self.port
        )
        conn.autocommit=True

        with conn.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE {self.db_name};')

        conn.close()

    def init_connection(self, autocommit=True):
        self.connection = psycopg2.connect(
            dbname = self.db_name,
            user = self.username,
            password = self.password,
            host = self.host,
            port = self.port
        )

        self.connection.autocommit = autocommit
    
    # instead of using __del__
    def close_db_if_necessary(self):
        if self.connection:
            self.connection.close()

    @property
    def conn(self):
        return self.connection
    
    def cursor(self):
        return self.connection.cursor(cursor_factory=NamedTupleCursor)

    def execute(self, query):
        with self.cursor() as cursor:
            cursor.execute(query)
            if not self.conn.autocommit:
                self.connection.commit()
            return cursor
    
    def fetchone(self, query):
        with self.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()
    
    def fetchall(self, query):
        with self.cursor() as cursor:
            cursor.execute(query) 
            return cursor.fetchall()
    
    #Create DB and add tables
    def create_tables(self, drop=False):
        if drop:
            self.execute("DROP TABLE IF EXISTS companies CASCADE;")
            self.execute("DROP TABLE IF EXISTS stock_rate;")

        self.execute("""
            CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            symbol VARCHAR(50) NOT NULL,
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

    def get_company(self, symbol):
        query = f"SELECT * FROM companies WHERE symbol = '{symbol}' limit 1;"
        return self.fetchone(query)
    
    def add_company(self, name, symbol, description):
        name = process_string(name)
        symbol = process_string(symbol)
        description = process_string(description)
        query = f"INSERT INTO companies (name, symbol, description) VALUES ({name}, {symbol}, {description});"
        self.execute(query)

    def insert_stock_rate(self, company_id, sr_date, sr_open, sr_high, sr_low, sr_close, volume, adjusted_close):
        sr_date = add_quotes(sr_date)
        query = f"""
            INSERT INTO stock_rate (company_id, date, open, high, low, close, volume, adjusted_close)
            VALUES ({company_id}, {sr_date}, {sr_open}, {sr_high}, {sr_low}, {sr_close}, {volume}, {adjusted_close});
        """
        self.execute(query)

    def add_stock_history_all(self, symbol):
        company_id = self.get_company(symbol)
        print(company_id)
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
                print(date, values)
                print(e.__class__.__name__, e)

    def get_united_table():
        pass



def test():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

    db.create_db()
    db.init_connection()
    db.create_tables(drop=False)

    db.add_company('Apple', 'AAPL', 'Apple Inc.')
    db.add_stock_history_all('AAPL')

if __name__ == '__main__':
    test()
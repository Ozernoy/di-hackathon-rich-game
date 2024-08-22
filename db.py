import psycopg2
from psycopg2.extras import NamedTupleCursor
from stock_api import StockClient
import settings

class DB:
    def __init__(self, host, db_name, password, username, port='5432', autocommit=True) -> None:
        self.host = host
        self.db_name = db_name
        self.password = password
        self.username = username
        self.port = port

        self.connection = psycopg2.connect(
            dbname = db_name,
            user = username,
            password = password,
            host = host,
            port = port
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
    def connect_to_default_db(self):
        self.close_db_if_necessary()
        self.connection = psycopg2.connect(
            dbname = 'postgres',
            user = 'postgress',
            password = settings.default_password,
            host = settings.default_host,
            port = settings.default_port
        )
    
    #Create DB and add tables
    def initiate_db(self, db_name):
        
        self.connect_to_default_db()
        self.execute(f'CREATE DATABASE {db_name};')

         # Reconnect to the newly created database
        self.connection = psycopg2.connect(
        dbname=db_name,
        user=self.username,
        password=self.password,
        host=self.host,
        port=self.port
    )
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
            open INTEGER NOT NULL,
            high INTEGER NOT NULL,
            low INTEGER NOT NULL,
            close INTEGER NOT NULL,
            volume INTEGER NOT NULL,
            adjusted_close INTEGER NOT NULL
            );
""")

    def get_company(self, symbol):
        query = f"SELECT * FROM companies WHERE symbol = '{symbol}';"
        return self.execute(query).fetchone()

    def add_company(self, name, symbol, description):
        query = f"INSERT INTO companies (name, symbol, description) VALUES ('{name}', '{symbol}', '{description}');"
        self.execute(query)

    def insert_stock_rate(self, company_id, sr_date, sr_open, sr_high, sr_low, sr_close, volume, adjusted_close):
        query = f"""
            INSERT INTO stock_rate (company_id, date, open, high, low, close, volume, adjusted_close)
            VALUES ({company_id}, '{sr_date}', {sr_open}, {sr_high}, {sr_low}, {sr_close}, {volume}, {adjusted_close});
        """
        self.execute(query)

    def add_stock_history_all(self, symbol):
        company_id = self.get_company(symbol)['company_id']
        data = StockClient.get_ts_monthly(symbol)
        for date, values in data['Monthly Adjusted Time Series']:
            try:
                self.insert_stock_rate(
                    company_id=company_id,
                    sr_date=date,
                    sr_open=values['open'],
                    sr_high=values['high'],
                    sr_low=values['low'],
                    sr_close=values['close'],
                    volume=values['volume'],
                    adjusted_close=values.adjusted_close
                )
            except Exception as e:
                print(e)
    

    def get_united_table():
        pass

    
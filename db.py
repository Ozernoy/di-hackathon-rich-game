import psycopg2
from psycopg2.extras import NamedTupleCursor
import settings

class DB:
    def __init__(self, host, db_name, password, username, port='5432') -> None:
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
    
    def __del__(self) -> None:
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
    
    #Create DB and add tables
    def initiate_db(self, db_name):
        self.execute(f'CREATE DATABASE {db_name};')
        self.connection = psycopg2.connect(
            dbname = db_name,
            user = self.username,
            password = self.password,
            host = self.host,
            port = self.port
        )
        self.execute("""
            CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            DESCRIPTION TEXT NOT NULL,
            date_listing DATE NOT NULL
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

    def get_united_table():
        pass

    
import psycopg2
from psycopg2.extras import NamedTupleCursor
from stock_api import StockClient

class DB:
    stock_cl = StockClient

    def __init__(self, host, db_name, password, username, port='5432', autocommit=True) -> None:
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
    
        
    
import psycopg2
from psycopg2.extras import NamedTupleCursor

class DB:
    def __init__(self, host, db_name, password, username, port='5432') -> None:
        self.connection = psycopg2.connect(
            dbname = db_name,
            user = username,
            password = password,
            host = host,
            port = port
        )
    
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
            self.connection.commit()
            return cursor
    
    
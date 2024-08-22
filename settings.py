import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

STOCK_API_KEY = os.environ.get("STOCK_API_KEY")

DB_NAME = 'stocks'
USERNAME = 'postgres'
HOST = 'localhost'
# PORT = '25432'
# PASSWORD = '1234'
PORT = '5432'
PASSWORD = 'vlad312312'


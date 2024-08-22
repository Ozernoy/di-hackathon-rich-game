import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

STOCK_API_KEY = os.environ.get("STOCK_API_KEY")

db_name = 'postgres'
user = 'postgres'
host = 'localhost'
port= '25432'
password = '1234'


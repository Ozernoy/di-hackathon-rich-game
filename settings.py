import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

STOCK_API_KEY = os.environ.get("STOCK_API_KEY")
# STOCK_API_KEY="UF23FMMH1ID5VA95"
# STOCK_API_KEY="OFQDB4IUUML8G1OT"
STOCK_API_KEY="JPZZMQXDYNL9XFJX"

DB_NAME = 'stocks'
USERNAME = 'postgres'
HOST = 'localhost'
# PASSWORD = '1234'
PORT = '5432'
PASSWORD = 'vlad312312'


# STOCK_API_KEY=THQSP5BS8B7CGOYI # https://www.alphavantage.co/documentation/


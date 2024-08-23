from settings import *
from ui import UI
from db import DB




db = DB(HOST,DB_NAME,PASSWORD, USERNAME, PORT)
db.create_db()
db.init_connection()
db.create_tables(drop=False)
db.add_company('Apple',  'AAPL', 'Apple Inc.')
db.add_stock_history_all('AAPL')

'''
available_users = GUI.ask_names()

list_of_companies = GUI.ask_companies(available_companies, available_users)

'''

db.close_db_if_necessary()


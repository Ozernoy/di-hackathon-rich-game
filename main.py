import settings
from UI import GUI
from db import DB
#from extract import Extractor




db = DB(settings.default_db_name, settings.default_user_name, settings.default_host, settings.rename_port, settings.rename_password)
db.initiate_db('companies_stock_db')

'''
available_users = GUI.ask_names()

list_of_companies = GUI.ask_companies(available_companies, available_users)

'''

db.close()


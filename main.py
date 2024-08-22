import settings
from UI import GUI
from db import DB
#from extract import Extractor




db = DB(settings.db_name, settings.user, settings.host, settings.port, settings.password)
db.initiate_db('companies_stock_db')

'''
available_users = GUI.ask_names()

list_of_companies = GUI.ask_companies(available_companies, available_users)
'''



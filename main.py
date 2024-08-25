from settings import *
from ui import UI
from db import DB

company_symbols = [
    "AAPL",  # Apple Inc.
    "MSFT",  # Microsoft Corporation
    "GOOGL",  # Alphabet Inc. (Google)
    "AMZN",  # Amazon.com Inc.
    "FB",  # Meta Platforms Inc. (formerly Facebook)
    "TSLA",  # Tesla Inc.
    "BRK.B",  # Berkshire Hathaway Inc. (Class B)
    "NVDA",  # NVIDIA Corporation
    "JPM",  # JPMorgan Chase & Co.
    "V",  # Visa Inc.
    "JNJ",  # Johnson & Johnson
    "WMT",  # Walmart Inc.
    "PG",  # Procter & Gamble Co.
    "DIS",  # The Walt Disney Company
    "NFLX"  # Netflix Inc.
]


def test():
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)

    try:
        # If the database does not exist, create it
        db.init_connection(dbname='postgres')
        db.create_db()
        db.close_db_if_necessary()  # Close the connection to 'postgres' after creating the database
        
        # Now connect to the newly created database
        db.init_connection()
        db.create_tables()
        
        
        #db.drop_database()
        db.add_all_companies()
        db.add_stock_history_for_selected_companies(symbols=company_symbols)
        db.exclude_outside_date_range()
    finally:
        db.close_db_if_necessary()

if __name__ == '__main__':
    test()
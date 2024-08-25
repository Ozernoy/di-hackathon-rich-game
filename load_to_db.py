import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

DB_NAME = 'stocks'
USERNAME = 'postgres'
HOST = 'localhost'
# PASSWORD = '1234'
PORT = '5432'
PASSWORD = 'vlad312312'

db_params = {
    'dbname': DB_NAME,
    'user': USERNAME,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}

csv_file = 'data.csv'

def load_data_to_db():
    df = pd.read_csv(csv_file)

    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=False)
    df['Close/Last'] = df['Close/Last'].str.replace('$', '').astype(float)
    df['Volume'] = df['Volume'].astype(int)
    df['Open'] = df['Open'].str.replace('$', '').astype(float)
    df['High'] = df['High'].str.replace('$', '').astype(float)
    df['Low'] = df['Low'].str.replace('$', '').astype(float)
    
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        unique_symbols = df['Company'].unique()

        cursor.execute("SELECT symbol, company_id FROM companies WHERE symbol IN %s", (tuple(unique_symbols),))
        company_ids = dict(cursor.fetchall())

        data_to_insert = []
        for _, row in df.iterrows():
            symbol = row['Company']
            if symbol in company_ids:
                data_to_insert.append((
                    company_ids[symbol],
                    row['Date'],
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close/Last'],
                    row['Volume'],
                    row['Close/Last']
                ))
            else:
                print(f"Warning: Company symbol '{symbol}' not found in the database. Skipping this record.")

        insert_query = """
        INSERT INTO stock_prices (company_id, price_date, open, high, low, close, volume, adjusted_close)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, price_date) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            adjusted_close = EXCLUDED.adjusted_close
        """
        execute_batch(cursor, insert_query, data_to_insert)

        conn.commit()
        print(f"Successfully inserted {len(data_to_insert)} rows into stock_prices table.")

    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into PostgreSQL:", error)
        conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()
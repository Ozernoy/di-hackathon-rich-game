#stock_api.py

import requests
import csv
from io import StringIO
from settings import STOCK_API_KEY

class StockClient:
    api_key = STOCK_API_KEY
    base_url = 'https://www.alphavantage.co/query'    

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_response_data(url):
        try:
            resp = requests.get(url)
            resp.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            #print(f"Response status code: {resp.status_code}")
            #print(f"Response content: {resp.text[:500]}")  # Print the first 500 characters of the response for debugging

            # Check if the response is CSV (This happens when function = LISTING_STATUS)
            if "symbol,name,exchange" in resp.text:
                # Parse the CSV data
                csv_data = StringIO(resp.text)
                reader = csv.DictReader(csv_data)
                return list(reader)
            else:
                # Assume the response should be JSON
                return resp.json()

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
        except requests.exceptions.JSONDecodeError as json_err:
            print(f"JSON decode error: {json_err} - Response content: {resp.text[:500]}")

        return None  # Return None if there was an error
    
    @classmethod
    def construct_url(cls, func, symbol=None, **kwargs):
        """
        Construct the API URL based on the function and optional symbol and parameters.
        """
        url = f"{cls.base_url}?function={func}"
        
        if symbol:
            url += f"&symbol={symbol}"
        
        url += f"&apikey={cls.api_key}"
        
        if kwargs:
            url += '&' + '&'.join([f'{key}={val}' for key, val in kwargs.items()])
        
        return url

    @classmethod
    def get_ts_monthly(cls, symbol, adj=True):
        #! Looks like we do not need condition here
        url = cls.construct_url('TIME_SERIES_MONTHLY_ADJUSTED' if adj else 'TIME_SERIES_MONTHLY_ADJUSTED', symbol)
        return cls.get_response_data(url)
    
    @classmethod
    def get_company_info(cls, symbol):
        url = cls.construct_url('OVERVIEW', symbol)
        return cls.get_response_data(url)

    @classmethod
    def get_symbols_and_names(cls):
        url = cls.construct_url('LISTING_STATUS')
        data = cls.get_response_data(url)

        if not data:
            print("No data retrieved or data is invalid.")
            return []

        symbols_and_names = [(entry['symbol'], entry['name']) for entry in data if 'symbol' in entry and 'name' in entry]
        return symbols_and_names



if __name__ == '__main__':
    res = StockClient.get_ts_monthly('AAPL')
    print(res)
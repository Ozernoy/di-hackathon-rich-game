from settings import STOCK_API_KEY
import requests
import os

class StockClient:
    api_key = STOCK_API_KEY
    base_url = 'https://www.alphavantage.co/query'    

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_response_data(url):
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(f'Error. Response code: {resp.status_code}')
    
    @classmethod
    def construct_url(cls, func, symbol, **kwargs):
        url = cls.base_url + '?function={func}&symbol={symbol}&apikey={api_key}'
        if kwargs:
            url += '&' + '&'.join([f'{key}={val}' for key, val in kwargs.items()])
        url = url.format(
            func=func,
            symbol=symbol,
            api_key=cls.api_key
        )
        return url

    @classmethod
    def get_ts_monthly(cls, symbol, adj=True):
        url = cls.construct_url('TIME_SERIES_MONTHLY_ADJUSTED' if adj else 'TIME_SERIES_MONTHLY_ADJUSTED', symbol)
        return cls.get_response_data(url)
    
    @classmethod
    def get_company_info(cls, symbol):
        url = cls.construct_url('OVERVIEW', symbol)
        return cls.get_response_data(url)




if __name__ == '__main__':
    res = StockClient.get_ts_monthly('AAPL')
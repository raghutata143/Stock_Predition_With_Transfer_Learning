
# coding: utf-8

# In[ ]:


## This code is to save S&P 500 Stock tickers
## I have executed each single function defined below in Test_Run file.
## This file will be used as helpful library for my thesis instead of having all the DFS building code in my analysis part
#!/usr/bin/env python3

# coding: utf8

import os
import codecs
import plotly
import pickle
import requests
import warnings
import bs4 as bs
import math, time
import numpy as np
import pandas as pd
import datetime as dt
import plotly.plotly as py
from matplotlib import style
import plotly.graph_objs as go
import fix_yahoo_finance as yf
import matplotlib.pyplot as plt
import pandas_datareader.data as web

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' #Hide messy TensorFlow warnings
warnings.filterwarnings("ignore") #Hide messy Numpy warnings





# saving sp500 tickers from sp500 list of companines
def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text)
    table = soup.find('table',{'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)
        
    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers, f)
#         return tickers

def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle","rb") as f:
            tickers = pickle.load(f)
        
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')
        
    start_date = dt.datetime(2010,1,1)
    end_date = dt.datetime(2018,5,31)
    
    #This function is to get the data of all the tickers (500)
    for ticker in tickers:
        print("Parsing",ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df = yf.download(ticker,start_date, end_date)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
            time.sleep(22)
        else:
            print('Already have {}'.format(ticker))


# Joing all stocks closing prices

def compile_data():
    with open("sp500tickers.pickle","rb") as f:
        tickers = pickle.load(f)
        
        
    main_df = pd.DataFrame()
    
    for count,ticker in enumerate(tickers):
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.Date = pd.to_datetime(df.Date)
        df.set_index('Date', inplace=True)
        df.rename(columns = {'Adj Close': ticker}, inplace = True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Volume'], axis = 1, inplace = True)
        
        if main_df.empty:
            main_df = df
            
        else:
            main_df = main_df.join(df, how = 'outer')
            
        if count % 10 == 0: # if count = 0, then print the count
            print(count)
    main_df.to_csv('sp500_joined_closing_prices.csv')

print("\n --------------------------Saving Tickers in Local System-------------------- \n")
save_tickers = save_sp500_tickers()

print("\n --------------------------Creating Stock DFS in Local System\n--------------------")
stock_dfs = get_data_from_yahoo()

print("\n --------------------------Creating a joined Adj Closing price dataframe for all stocks\n--------------------")
join_closed_data = compile_data()






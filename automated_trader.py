
import alpaca_trade_api as tradeapi
from math import floor
import logging
from time import sleep
import pandas as pd
from threading import Thread
from datetime import datetime
import requests_cache

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from multiprocessing.pool import ThreadPool

import sqlite3
import warnings

warnings.simplefilter('ignore')

logging.basicConfig(format='%(asctime)s %(message)s')
logging.basicConfig(filename='./trader.log', level=logging.INFO)

class automated_trader():
    def __init__(self, mode):

        logging.info('started automated trader')

        self.conn = sqlite3.connect('dividends.db')
        self.cur = self.conn.cursor()

        self.api = tradeapi.REST(
                                'PKU23YDAWZVS4PUESHQ6',
                                'dqkM6qqmUbkAuWtnzcwg7wrsUOmp8hNn80dnyGh9',
                                'https://paper-api.alpaca.markets'
                                )

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('log-level=3')
        self.driver = webdriver.Chrome(options=chrome_options, executable_path='./chromedriver.exe')
        self.delay = 10

        if mode == 'buy':
            self.get_dividend_plays()
            self.make_new_trades()
        elif mode == 'sell':
            self.close_trades()


    def get_dividend_plays(self):
        
        dividend_dfs = []
        url = 'https://www.dividend.com/ex-dividend-dates/#tm=3-ex-div-dates&r=Webpage%231280&f_22_from=next_business_day&f_22_to=next_business_day&only=meta%2Cdata%2Cthead&sort_by=latest_yield&sort_direction=desc'
        self.driver.get(url)
        while True:
            try:
                WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, 'Next ')))
                next_element = self.driver.find_element_by_link_text("Next ›")

                dividend_page_df = pd.read_html(self.driver.page_source)[0]
                #print(dividend_page_df)

                dividend_dfs.append(dividend_page_df)

                next_element.click()
            except Exception as e:
                #print(e)
                break

        dividends = pd.concat(dividend_dfs)
        
        del dividends['Unnamed: 0']
        del dividends['DARS™ Rating']
        
        for col_name in ['Yield', 'Stock Price', 'Div Payout']:
            dividends[col_name] = dividends[col_name].str.replace(',', '')
            dividends[col_name] = dividends[col_name].str.replace('$', '')
            dividends[col_name] = dividends[col_name].str.replace('%', '')
            dividends[col_name] = dividends[col_name].astype(float)

        dividends['Yield'] = dividends['Yield'] / 100

        #dividends['Ex-Div Date'] = pd.to_datetime(dividends['Ex-Div Date'])
        
        dividends['Div Payout Percent'] = dividends['Div Payout'] / dividends['Stock Price']

        dividends = dividends.sort_values(by=['Div Payout Percent'], ascending=False)
        
        
        dividends['Stock Symbol']= dividends['Stock Symbol'].str.split("-", n = 1, expand = True) 
        dividends = dividends.drop_duplicates(subset=['Stock Symbol'])
        dividends = dividends.set_index('Stock Symbol')

        dividends = dividends[ dividends['Div Payout Percent'] > .01 ]

        print('Trading these dividends')
        print(dividends)
        
        self.dividends = dividends


    def make_new_trades(self):
        order_queue = []
        for symbol, dividend_row in self.dividends.iterrows():
            symbol = symbol.split('-')[0]
            
            #price = dividend_row['Stock Price']*1.1
            #order_queue.append( (symbol, 'buy', price) )
            order_queue.append( (symbol, 'buy') )

            #order_status, fill_price, order_id = self.submit_order(symbol, 'buy', price)
        
        t_pool = ThreadPool(5)
        orders = t_pool.map(self.submit_order, order_queue)
        
        
        for order_result in orders:
            order_status, buy_fill_price, order_id, symbol = order_result
        
            dividend_row = self.dividends.loc[symbol]
            dividend_row['buy_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dividend_row['buy_order_status'] = order_status
            dividend_row['buy_fill_price'] = buy_fill_price
            dividend_row['buy_order_id'] = order_id
            
            dividend_row['sell_date'] = None
            dividend_row['sell_order_status'] = None
            dividend_row['sell_fill_price'] = None
            dividend_row['sell_order_id'] = None
            dividend_row['stock_roi'] = None

            dividend_row['unique_id'] = symbol+'_'+str(dividend_row['Ex-Div Date'])
            dividend_row = dividend_row.to_frame().T

            for col_name in dividend_row.columns:
                dividend_row[col_name] = dividend_row[col_name].apply(pd.to_numeric, errors='ignore')
            
            dividend_row.to_sql('trades', self.conn, if_exists='append')


    def close_trades(self):
        sql = 'select * from trades where buy_order_status == "filled" and sell_order_status is null'
        trades_df = pd.read_sql(sql, self.conn)

        order_queue = []
        for symbol, trade_row in trades_df.iterrows():
            order_queue.append( (symbol, 'sell') )

        #t_pool = ThreadPool(5)
        #orders = t_pool.map(submit_order, order_queue)
        
        orders = submit_order(order_queue[0])
        orders = [orders]
        

        for order_result in orders:
            order_status, sell_fill_price, order_id, symbol = order_result

            sell_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            #get roi
            buy_price = float(trades_df.loc[symbol]['buy_fill_price'])
            roi = (sell_fill_price - buy_price) / buy_price

            sql = '''update trades set sell_date = "%s", 
                                       sell_order_status = "%s", 
                                       sell_fill_price = %s, 
                                       sell_order_id = "%s",
                                       stock_roi = %s 
                     where unique_id == "%s"
                '''
            sql = sql % ( sell_date, order_status, sell_fill_price, order_id, roi )
            print('submitting close order sql')
            print(sql)

            self.cur.execute(sql)
            self.conn.commit()


    def submit_order(self, params):
        #symbol, side, limit_price = params
        symbol, side = params
        order_status = None
        fill_price = None
        order_id = None
        try:
            order = self.api.submit_order( 
                                    symbol=symbol, 
                                    qty = 1, 
                                    side = side, 
                                    type = 'market', 
                                    time_in_force = 'day',
                                    #extended_hours = True,
                                    #limit_price = float(round(limit_price, 2))
                                    )
            #print('order submitted', symbol, side, limit_price, order.id)
            print('order submitted', symbol, side, order.id)
            order_id = order.id
            for _ in range(60):
                sleep(1)
                order = self.api.get_order(order.id)
                order_status = order.status
                
                if order.status == 'filled':
                    #print('order filled', symbol, side, limit_price, order.id)
                    print('order filled', symbol, side, order.id)
                    fill_price = float(order.filled_avg_price)
                    
                    break
        except Exception as e:
            order_status = str(e)
            print('got exception', e)
        
        if order_id is not None and order.status != 'filled':
            print('not filled. Cancelling order')
            self.api.cancel_order(order_id)
            
            order_status = 'cancelled'
        
        return order_status, fill_price, order_id, symbol


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'buy' or mode == 'sell':
        automated_trader(mode)
    else:
        print('correct mode argument not provided')


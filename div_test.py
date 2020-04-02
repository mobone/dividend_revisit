import pandas as pd
import requests as r
import pandas_datareader as pdr
import requests_cache
from time import time
from datetime import datetime

requests_cache.install_cache('dividend_cache')

for cutoff in [.01,.02,.025,.03,.035]:
    payouts = []
    cutoff_2 = cutoff+.02

    for page in range(1,135,20):
        f = r.get("http://finviz.com/screener.ashx?v=111&f=fa_div_veryhigh&r=" + str(page))
        df = pd.read_html(f.content, header=0)[14]


        for symbol in df['Ticker'].values:

            f = r.get('http://www.nasdaq.com/symbol/%s/dividend-history' % symbol)
            dividend_dates = pd.read_html(f.content)[5]

            try:
                price_history_df = pdr.get_data_yahoo(symbol)
                print("got yahoo")
                price_history_df = price_history_df.reset_index()
                dividend_dates['Ex/Eff Date'] = pd.to_datetime(dividend_dates['Ex/Eff Date'])
            except Exception as e:
                continue

            for row in dividend_dates.iterrows():
                try:
                    date = row[1]['Ex/Eff Date']
                    if date<datetime.strptime('01/01/2015', '%m/%d/%Y'):
                        continue
                    div_cash = float(row[1]['Cash Amount'])
                    dividend_date = price_history_df[price_history_df['Date']==date].index

                    start_price = float(price_history_df.iloc[dividend_date-1]['Open'])

                    div_payout = div_cash/start_price
                    if div_payout<cutoff or div_payout>cutoff_2:
                        continue

                    end_price = 0
                    success = 1
                    i = 1
                    while end_price<start_price and i<15:
                        this_end_price = float(price_history_df.iloc[dividend_date+i]['Open'])
                        if this_end_price>start_price:
                            end_price = this_end_price
                            break
                        i = i + 1
                    if end_price == 0:
                        end_price = this_end_price
                        success = 0

                    percent_change = end_price/start_price - 1

                    payouts.append([symbol, date, div_cash, start_price, end_price, div_payout, percent_change, success, i])
                    #print([symbol, date, div_cash, start_price, end_price, div_payout, percent_change, success, i])
                except:
                    continue



    df = pd.DataFrame(payouts, columns = ['Symbol', 'Date', 'Div_Pay', 'Open', 'Close', 'Div_Perc', 'Perc_Change', 'Success','Sell_Num'])
    df = df.dropna(subset=['Perc_Change'])
    if len(df)==0:
        continue
    df['Diff'] = df['Div_Perc']+df['Perc_Change']
    df.to_csv("div_test.csv")
    print(cutoff, symbol, df['Success'].sum()/len(df), df['Diff'].mean(), df['Sell_Num'].mean(), len(df)/3)

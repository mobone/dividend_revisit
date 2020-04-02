import yfinance
import requests as r
import pandas as pd

def get_div_history(symbol):

    profits = []

    history = yfinance.Ticker(symbol).history(period='3y', auto_adjust=False)

    history = history.reset_index()
    #print(history)

    # dividends
    dividends = history[history['Dividends']>0]

    #print(dividends)
    for div_index, dividend in dividends.iterrows():
        for start_date in range(1,4):
            for sell_date in range(0,5):
                for sell_time in ['Open', 'Close']:

                    #start_entry = history.loc[div_index - start_date]
                    #start_price = float(start_entry['Close'])

                    #potential_sell_dates = history.loc[div_index:div_index+sell_date]
                    #print(potential_sell_dates)
                    #potential_sell_dates = potential_sell_dates[ (potential_sell_dates['Open']>start_price) | (potential_sell_dates['High']>start_price) | (potential_sell_dates['Low']>start_price) | (potential_sell_dates['Close']>start_price) ]
                    #if potential_sell_dates.empty:
                    #    end_entry = history.loc[div_index + sell_date]
                    #    end_price = float(end_entry[sell_time])
                    #else:
                    #    end_price = start_price
                    try:
                        start_entry = history.loc[div_index - start_date]
                        start_price = float(start_entry['Close'])
                        end_entry = history.loc[div_index + sell_date]
                        end_price = float(end_entry[sell_time])
                    except:
                        continue

                    div_amount = float(dividend['Dividends'])
                    stock_change = (end_price - start_price) / start_price
                    div_change = div_amount/start_price
                    profit = div_change + stock_change

                    #print(profit)
                    profits.append( [symbol, start_date, sell_date, sell_time, div_change, stock_change, profit] )


    return profits

all_profits = []
for page in range(0,100):
    page = (page * 20) + 1
    f = r.get("https://finviz.com/screener.ashx?v=111&f=fa_div_o5&r=" + str(page))
    symbols = pd.read_html(f.content, header=0)[14]['Ticker'].values
    for symbol in symbols:

        profits = get_div_history(symbol)
        all_profits.extend(profits)

        df = pd.DataFrame(all_profits, columns = ['symbol', 'start_date', 'sell_date', 'sell_time', 'div_change', 'stock_change', 'profit'])
        print(df)
        df.to_csv('div_test.csv')

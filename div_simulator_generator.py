import yfinance
import requests as r
import pandas as pd
import requests_cache

requests_cache.install_cache('dividend_cache')

def get_div_history(symbol, start_date, sell_date, sell_time):

    profits = []

    history = yfinance.Ticker(symbol).history(period='6y', auto_adjust=False)

    history = history.reset_index()
    #print(history)

    # dividends
    dividends = history[history['Dividends']>0]

    #print(dividends)
    for div_index, dividend in dividends.iterrows():


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
            start_datetime = start_entry['Date']
            start_price = float(start_entry['Close'])
            end_entry = history.loc[div_index + sell_date]
            #end_datetime = end_entry['Date']
            end_price = float(end_entry[sell_time])
        except Exception as e:
            print(e)
            continue

        div_amount = float(dividend['Dividends'])
        stock_change = (end_price - start_price) / start_price
        div_change = div_amount/start_price
        profit = div_change + stock_change

        #print(profit)
        profits.append( [symbol, start_datetime, div_amount, start_price, end_price] )


    return profits

all_profits = []
for page in range(0,100):
    page = (page * 20) + 1
    if page > 1148:
        break
    f = r.get("https://finviz.com/screener.ashx?v=111&f=fa_div_o6&r=" + str(page))
    symbols = pd.read_html(f.content, header=0)[14]['Ticker'].values
    for symbol in symbols:

        profits = get_div_history(symbol, 1, 0, 'Open')
        all_profits.extend(profits)

        df = pd.DataFrame(all_profits, columns = ['symbol', 'start_date', 'div_amount', 'start_price', 'end_price'])
        print(df)
        df.to_csv('simulation_input_6y.csv')

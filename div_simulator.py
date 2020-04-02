import pandas as pd

import numpy as np

def run_simulation(cutoff, end_cutoff, num_trades):
    df = pd.read_csv('simulation_input_6y.csv')
    df = df.dropna()
    dfs = df.groupby(by=['start_date'])
    balance = 10000
    total_num_trades = 0
    for start_date, df in dfs:

        df['div_percent'] = df['div_amount'] / df['start_price']

        df = df[ df['div_percent'] > cutoff ]
        df = df[ df['div_percent'] < end_cutoff ]

        trades = df

        if len(trades)>num_trades:
            trades = df.sample(n=num_trades)

        avg_price = trades['start_price'].mean()

        balance_to_use = balance * (1/num_trades)

        for _, trade in trades.iterrows():

            #print(trade)
            try:

                num_shares = int( balance_to_use / float(trade['start_price']) )
                #print(num_shares)



                #share_profit = num_shares * (float(trade['end_price']) - float(trade['start_price']))
                dividend_profit = num_shares * float(trade['div_amount'])

                balance = balance - (num_shares * float(trade['start_price']))
                balance = balance + (num_shares * float(trade['end_price'])) + dividend_profit

            except Exception as e:
                print(e)
                pass
            total_num_trades = total_num_trades + 1
    #print(balance)
    #print(num_trades)
    return [ cutoff, end_cutoff, num_trades, total_num_trades,  balance, (balance - 10000) / 10000 ]
results = []
for cutoff in [.005, .01, .015, .02]:
    for end_cutoff in [.01, .015, .025, .03, .035, 2]:
        for num_trades in [5,10,15]:
            result = run_simulation(cutoff, end_cutoff, num_trades)
            results.append(result)
            result_df = pd.DataFrame(results, columns = ['cutoff', 'end_cutoff', 'num_trades', 'total_num_trades', 'balance', 'percent_change'])
            print(result_df.sort_values(by=['percent_change']))
            result_df.to_csv('div_results.csv')

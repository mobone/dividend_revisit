import pandas as pd

import numpy as np
from itertools import product
from multiprocessing import Pool
import warnings
warnings.simplefilter("ignore")
def run_simulation(params):
    #print(params)
    cutoff, num_trades, start_balance, strat, max_shares = params
    df = pd.read_csv('simulation_input_6y.csv')
    df = df.dropna()
    dfs = df.groupby(by=['start_date'])
    balance = start_balance
    total_num_trades = 0
    total_wins = 0
    avg_prices = []
    
    for start_date, df in dfs:

        df['div_percent'] = df['div_amount'] / df['start_price']

        df = df[ df['div_percent'] > cutoff ]
        #df = df[ df['div_percent'] < end_cutoff ]

        trades = df

        if len(trades)>num_trades and strat == 'random':
            trades = df.sample(n=num_trades)
        elif len(trades)>num_trades and strat == 'sorted':
            df = df.sort_values(by=['div_percent'])
            trades = df.tail(num_trades)
        
        if trades.empty:
            continue
        avg_price = trades['start_price'].mean()
        avg_prices.append(avg_price)

        balance_to_use = np.floor(balance * (1/ len(trades) ))
        #if balance_to_use>10000:
            #balance_to_use = 10000
        #print(start_date, balance)
        
        for _, trade in trades.iterrows():

            #print(trade)
            try:

                num_shares = np.floor( balance_to_use / float(trade['start_price']) )
                if num_shares == 0:
                    continue
                if num_shares>max_shares:
                    #print('max shares', balance_to_use, float(trade['start_price']))
                    num_shares = max_shares

                

                #share_profit = num_shares * (float(trade['end_price']) - float(trade['start_price']))
                dividend_profit = num_shares * float(trade['div_amount'])
                investment_cost = (num_shares * float(trade['start_price']))
                investment_return = (num_shares * float(trade['end_price']))
                #print(trade)
                #print(start_date, num_shares, investment_cost, investment_return, dividend_profit)
                
                balance = balance - investment_cost
                
                balance = balance + investment_return + dividend_profit
                #print(balance)
                #input()

            except Exception as e:
                #print(e)
                pass
            #input()

            if (investment_return - investment_cost + dividend_profit) > 0:
                total_wins = total_wins + 1
            
            total_num_trades = total_num_trades + 1
    try:
        avg_price_per_share = sum(avg_prices) / len(avg_prices) 
    except:
        avg_price_per_share = None
    #input()
    #print(balance)
    #print(num_trades)
    win_rate = total_wins / float(total_num_trades)
    return [ cutoff, start_balance, num_trades, strat, max_shares, win_rate, total_num_trades, avg_price_per_share, balance, (balance - start_balance) / start_balance ]
if __name__ == "__main__":
    
    params = []
    results = []
    cutoffs = [.005, .01, .015, .02, .025]
    #end_cutoff = [.035, 2]
    num_trades = [3,5,10,15]
    start_balance = [2000]
    strat = ['sorted']
    max_shares = [100,200,300,400]
    p = Pool(12)
    all_results = p.map(run_simulation, product( cutoffs, num_trades, start_balance, strat, max_shares ) )

    for result in all_results:
                    #result = run_simulation(cutoff, num_trades, start_balance)
                    #results.append(result)
        results.append(result)
    result_df = pd.DataFrame(results, columns = ['cutoff', 'start_balance', 'num_trades', 'strat', 'max_shares', 'win_rate', 'total_num_trades', 'avg_price_per_share', 'balance', 'percent_change'])
    print(result_df.sort_values(by=['balance']))
    result_df.to_csv('div_results.csv')
    print( result_df.groupby(by=['max_shares','num_trades'])['percent_change'].mean() )
    """
    # 0.020  2.000  500  1  1171  38.309430  89010.710714  177.021421
    params = [.02,2,1,500, 'sorted']
    run_simulation(params)
    """
    
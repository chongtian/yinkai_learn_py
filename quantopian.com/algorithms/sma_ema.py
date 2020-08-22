"""
This algorithm checks buy/sell signals through moving average.
Raw Signals
	Buy Signal = Close price is larger than both SMA(10) and SMA(20)
	Sell Signal = Close price is smaller than both SMA(10) and SMA(20)

Operation
	1. After the market close, scan moving averages of the assets from my watchlist. If there are two consecutive Buy Signals or two consecutive Sell Signals, I will mark this asset for further watch.
	2. The next day, 30 minutes before the market close, I will check the below things for the marked asset:
		a. For a Buy Signal., I will buy if:
			i. The real time price is larger than both SMA(10) and SMA(20) which are from yesterday.
		b. For a Sell Signal. I will sell if:
			i. The real time price is smaller than both SMA(10) and SMA(20) which are from yesterday, 
			ii. And, I did not buy on the previous day,
			iii. And, The profit is already larger than 20%, or the loss is already greater than 5%
	3. On the day I sell, I will check Sell Signal again after the market closes. If there is a Sell signal, I will buy 30 minutes before the market close on the next day. This time, I will not check the real time price.
	4. Regardless Sell or Buy Signals, 30 minutes before market close on each day, I will sell to stop loss if:
		a. The daily price change of the asset is over -5%,
		b. And my total loss is already greater than 5%
    5. Attention: I will start to count consecutive sell or buy signals from the day I sell for stop loss.
"""

import quantopian.algorithm as algo
import quantopian.optimize as opt
from quantopian.algorithm import attach_pipeline, pipeline_output

# imports for pipeline
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage, EWMA
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.filters import StaticAssets

def initialize(context):
    # Parameters of the algorithm
    context.fast_ma_periods = 10
    context.slow_ma_periods = 20
    # sma or ema
    context.slow_ma_type='sma'
    context.fast_ma_type='sma'
    context.assets = [symbol('STAG')]
    context.out_of_market = symbol('BND')
    context.threshold_sell_loss = -0.05
    context.threshold_sell_win = 0.2
    context.threshold_signal_count = 2
    context.threshold_stop_loss = -0.05
    context.threshold_hold_days = 1
    # Buy immediately after sell
    context.aggressive_buy = True
    context.move_fund_out_of_market = True
    
    # variable used by the algorithm
    context.buy_signal_count = 0
    context.sell_signal_count = 0
    context.hold_days = 0 # -1 means buy on the first day regardless signal
        
    algo.set_benchmark(context.assets[0])
    pipe = make_pipeline(context)
    attach_pipeline(pipe, name='etf_pipeline')

    # Rebalance every day, 30 minutes before the market close.
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        time_rule=algo.time_rules.market_close(minutes=30),
        calendar=algo.calendars.US_EQUITIES
    )

def make_pipeline(context):
    # Parameters
    fast_ma_periods = context.fast_ma_periods
    slow_ma_periods = context.slow_ma_periods
    assets = context.assets

    # Define factors.
    if context.fast_ma_type == 'sma':
        ma_fast = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=fast_ma_periods)
    else:
        ma_fast = EWMA.from_span(inputs=[EquityPricing.close], window_length=fast_ma_periods*2+20, span=fast_ma_periods,)
    if context.slow_ma_type == 'sma':
        ma_slow = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=slow_ma_periods)
    else:
        ma_slow = EWMA.from_span(inputs=[EquityPricing.close], window_length=slow_ma_periods*2+20, span=slow_ma_periods,)
    last_close_price = EquityPricing.close.latest

    # Define a filter.
    base_universe = StaticAssets(assets) 

    # define pipeline
    pipe = Pipeline(
        columns={
            'price': last_close_price,
            'fast_ma': ma_fast,
            'slow_ma': ma_slow
        },
        screen= base_universe 
    )
    return pipe

def rebalance(context, data):
    # get the current minute-level price
    cur_price = data.current(context.assets[0],'close')
    if cur_price != cur_price:
        return
    
    # get position of the first asset (which is the only asset)
    p = context.portfolio.positions[context.assets[0]]
    
    # constaints are for optimize, which is not used here
    constraints = [] 
    
    return_percent = 0
    # calculate return
    if p.amount > 0:
        cost = p.cost_basis
        return_percent = (cur_price - cost)/cost
    
    # get moving average from pipeline and set buy and sell signals
    cur = pipeline_output('etf_pipeline').iloc[0]
    
    # handle Stop Loss transaction, trigger point: Daily loss or total loss 
    if (cur_price - cur.price)/cur.price < context.threshold_stop_loss and return_percent < context.threshold_sell_loss and p.amount > 0:
        objective = opt.TargetWeights({context.assets[0]: 0})
        algo.order_optimal_portfolio(objective, constraints)
        context.hold_days = 0
        context.sell_signal_count = 0
        context.buy_signal_count = 0
        log.info('Close all positions to stop loss: ' + str(return_percent))
        record(buy_count=0, sell_count=0, buy=0, sell=0)
        return
    
    # the signals are from yesterday
    # each day after market close, I check and count signals
    price_compare = cur.price
    if (price_compare > cur.fast_ma and price_compare > cur.slow_ma):
        context.buy_signal_count += 1
        context.sell_signal_count = 0
    if price_compare < cur.slow_ma and price_compare < cur.fast_ma:
        context.buy_signal_count = 0
        context.sell_signal_count += 1
    
    record(buy_count=context.buy_signal_count, sell_count=-1*context.sell_signal_count)
    
    buy = False
    sell = False
    if context.buy_signal_count >= context.threshold_signal_count and cur_price > cur.fast_ma and cur_price > cur.slow_ma:
        buy = True
        context.buy_signal_count = 0
    if context.sell_signal_count > context.threshold_signal_count:
        # aggressive buy
        buy = context.aggressive_buy or cur.fast_ma > cur.slow_ma
        sell = not buy
        context.sell_signal_count = 0
    elif context.sell_signal_count >= context.threshold_signal_count and cur_price < cur.slow_ma and cur_price < cur.fast_ma:
        sell = True
        
    record(buy=buy, sell=-1*sell)
        
    # handle buy transaction
    if buy and p.amount == 0 or context.hold_days == -1:
        # Buy asset    
        # Target a 100% long allocation of our portfolio in the given asset.
        objective = opt.TargetWeights({context.assets[0]: 1.0})
        algo.order_optimal_portfolio(objective, constraints)
        log.info('Buy {0} after {1} periods.'.format(context.assets[0], context.hold_days))
        context.hold_days = 0
            
    # handle sell transaction
    if sell and p.amount > 0 and context.hold_days > context.threshold_hold_days and (return_percent < context.threshold_sell_loss or return_percent > context.threshold_sell_win): 
        # Sell asset   
        if context.move_fund_out_of_market:
            objective = opt.TargetWeights({context.out_of_market: 1.0})
            log.info('Switch to {0} after {1} periods.{2:%}'.format(context.out_of_market, context.hold_days,return_percent))
        else:
            objective = opt.TargetWeights({context.assets[0]: 0})
            log.info('Sell {0} after {1} periods.{2:%}'.format(context.assets[0], context.hold_days, return_percent))
        algo.order_optimal_portfolio(objective, constraints)        
        context.hold_days = 0
        
    context.hold_days += 1
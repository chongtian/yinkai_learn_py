"""
This algorithm checks buy/sell signals through moving average.
Raw buy signal = current price is larger than both SMA(10) and EMA(20)
Raw sell signal = current price is smaller than both SMA(10) and EMA(20)
Note: Current price is the real-time minute-level price, while SMA and EMA are from the day before today
When there are two consecutive buy signals which are not interrupted by sell signal, I will buy.
When there are two consecutive sell signals which are not interrupted by buy signal, I will sell.
After I sell, if there is an immediate following sell signal, I will also buy.
However, if the current return is smaller than 1% and larger than negative 5%, I will not sell.
And, if I just bought the previous trade day, I will not sell either.
Anytime when the return is less than negative 10%, I sell. Notice, this will not interrput the consecutiveness of sell signals. It will reset buy signals.
"""

import quantopian.algorithm as algo
import quantopian.optimize as opt
from quantopian.algorithm import attach_pipeline, pipeline_output

# imports for pipeline
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage, EWMA
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.filters import StaticAssets


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


def initialize(context):
    # Parameters of the algorithm
    context.fast_ma_periods = 10
    context.slow_ma_periods = 20
    # sma or ema
    context.slow_ma_type='sma'
    context.fast_ma_type='sma'
    context.assets = [symbol('VGT')]
    context.out_of_market = symbol('BND')
    context.threshold_sell_loss = -0.05
    context.threshold_sell_win = 0.2
    context.threshold_signal_count = 2
    context.threshold_stop_loss = -0.1
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
    # handle Stop Loss transaction 
    if return_percent < context.threshold_stop_loss:
        objective = opt.TargetWeights({context.assets[0]: 0})
        algo.order_optimal_portfolio(objective, constraints)
        context.hold_days = 0
        # context.sell_signal_count = 0
        context.buy_signal_count = 0
        log.info('Close all positions to stop loss: ' + str(return_percent))
        return
    
    # get moving average from pipeline and set buy and sell signals
    cur = pipeline_output('etf_pipeline').iloc[0]
    # the signals are from yesterday
    price_compare = cur_price
    # price_compare = cur.price
    if (price_compare > cur.fast_ma and price_compare > cur.slow_ma):
        context.buy_signal_count += 1
        context.sell_signal_count = 0
    if (price_compare < cur.slow_ma and price_compare < cur.fast_ma):
        context.buy_signal_count = 0
        context.sell_signal_count += 1
    
    # Store data for record
    record(
        #price_today=cur_price,
        #price_yesterday=cur.price,
        return_today=return_percent,
        #slow_ma=cur.slow_ma,
        #fast_ma=cur.fast_ma
           )
    
    buy = False
    sell = False
    if context.buy_signal_count >= context.threshold_signal_count:
        buy = True
        context.buy_signal_count = 0
    if context.sell_signal_count >= context.threshold_signal_count:
        sell = True
        if context.sell_signal_count > context.threshold_signal_count:
            buy = context.aggressive_buy or cur.fast_ma > cur.slow_ma
            sell = not buy
            context.sell_signal_count = 0
    
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
    
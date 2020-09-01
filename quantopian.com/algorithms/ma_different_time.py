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
from quantopian.pipeline.factors import SimpleMovingAverage, EWMA, RSI
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.filters import StaticAssets

def initialize(context):
    # Parameters of the MA algorithm
    context.fast_ma_periods = 10
    context.slow_ma_periods = 20
    context.rsi_period = 5
    # sma or ema
    context.slow_ma_type='sma'
    context.fast_ma_type='sma'
    context.threshold_signal_count = 2
    context.aggressive_buy = True
    
    # parameters required by all algorithms
    context.assets = [symbol('VGT')]
    context.out_of_market = symbol('BND')
    context.threshold_sell_loss = -0.05
    context.threshold_sell_win = 0.1
    context.threshold_stop_loss = -0.05
    context.threshold_hold_days = 1
    context.move_fund_out_of_market = True
    context.log_performance_by_qty = True
    
    # variable used by the algorithm
    context.buy_signal_count = 0
    context.sell_signal_count = 0
    context.hold_days = 0 # -1 means buy on the first day regardless signal
    context.order = 0
    context.benchmark_asset = context.assets[0]
    
    algo.set_benchmark(context.benchmark_asset)
    pipe = make_pipeline(context)
    context.pipeline_name = 'etf_pipeline'
    attach_pipeline(pipe, name=context.pipeline_name)
    
    set_slippage(slippage.NoSlippage()) 
    
    # get the final buy and sell signals
    algo.schedule_function(get_final_signals, algo.date_rules.every_day(), time_rule=algo.time_rules.market_close(minutes=30), calendar=algo.calendars.US_EQUITIES)
        
    # Trade
    algo.schedule_function(trade, algo.date_rules.every_day(), time_rule=algo.time_rules.market_close(minutes=20), calendar=algo.calendars.US_EQUITIES)
    
    # log daily performance
    algo.schedule_function(log_performance, algo.date_rules.every_day(), time_rule=algo.time_rules.market_close(), calendar=algo.calendars.US_EQUITIES)

    
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
    rsi = RSI(inputs=[EquityPricing.close], window_length=context.rsi_period)

    # Define a filter.
    base_universe = StaticAssets(assets) 

    # define pipeline
    pipe = Pipeline(
        columns={
            'price': last_close_price,
            'fast_ma': ma_fast,
            'slow_ma': ma_slow,
            'rsi': rsi
        },
        screen= base_universe 
    )
    return pipe

def get_return(context, data, asset):
    # get the current minute-level price
    current_price = data.current(asset,'price')
    if current_price != current_price:
        log.info('WARNING: Failed to get price of {0}: {1}'.format(asset, current_price))
        return 0,0,0
    # get position of the asset
    p = context.portfolio.positions[asset]
    return_percent = 0
    # calculate return
    if p.amount > 0:
        cost = p.cost_basis
        return_percent = (current_price - cost)/cost
    return current_price, return_percent, p.amount

# get raw buy and sell signals
def get_raw_signals(context, data):
    if 'buy_signal_count' not in context:
        context.buy_signal_count = 0
    if 'sell_signal_count' not in context:
        context.sell_signal_count = 0

    asset = context.assets[0]
    pipe = pipeline_output(context.pipeline_name).loc[asset]
    price_compare = pipe.price
    context.fast_ma = pipe.fast_ma
    context.slow_ma = pipe.slow_ma
    context.rsi = pipe.rsi
    context.last_price = pipe.price
    
    if (price_compare > pipe.fast_ma and price_compare > pipe.slow_ma):
        context.buy_signal_count += 1
        context.sell_signal_count = 0
    if (price_compare < pipe.slow_ma ) and price_compare < pipe.fast_ma:
        context.buy_signal_count = 0
        context.sell_signal_count += 1
    #log.info('Buy Count = {0}, Sell Count = {1}'.format(context.buy_signal_count, context.sell_signal_count))

def before_trading_start(context, data):
    get_raw_signals(context, data)
    

def get_final_signals(context, data):
    context.buy = False
    context.sell = False
    
    asset = context.assets[0]
    current_price, return_percent, hold_amount = get_return(context, data, asset)
    if current_price == 0:
        return
    
    # handle Stop Loss transaction, trigger point: Daily loss or total loss 
    if (current_price - context.last_price)/context.last_price < context.threshold_stop_loss and return_percent < context.threshold_sell_loss and hold_amount > 0:
        context.sell_signal_count = 0
        context.buy_signal_count = 0
        log.info('Close all positions to stop loss. {0:%}'.format(return_percent))
        context.sell = True
        context.buy = False
        context.stop_loss = True
        return    
    
    if context.buy_signal_count >= context.threshold_signal_count and current_price > context.fast_ma and current_price > context.slow_ma:
        context.buy = True
        context.buy_signal_count = 0
    
    if context.sell_signal_count > context.threshold_signal_count:
        # aggressive buy
        context.buy = context.aggressive_buy or context.fast_ma > context.slow_ma
        context.sell = not context.buy
        context.sell_signal_count = 0
    elif context.sell_signal_count >= context.threshold_signal_count and current_price < context.slow_ma and current_price < context.fast_ma:
        context.sell = True
    
    # set up extra sell requirement
    if context.rsi >= 50:
       extra_sell_req = return_percent > context.threshold_sell_win
    elif context.rsi < 50:
       extra_sell_req = return_percent > context.threshold_sell_win or return_percent < context.threshold_sell_loss 
    else:
       extra_sell_req = True
    
    #log.info('Buy = {0}, Sell = {1}, HoldDay = {2}'.format(context.buy, context.sell, context.hold_days))
    context.buy = context.buy and hold_amount == 0 or context.hold_days == -1
    context.sell = context.sell and extra_sell_req and hold_amount > 0 and context.hold_days > context.threshold_hold_days
    # log.info('Buy = {0}, Sell = {1}'.format(context.buy, context.sell))
    
    

def trade(context, data):
    context.hold_days += 1
    # debug
    # log.info('{0} {1} {2} {3} {4} {5} {6} {7}'.format(context.fast_ma, context.slow_ma, context.rsi, context.last_price, context.buy_signal_count, context.sell_signal_count, context.buy, context.sell)) 
    if (not context.buy) and (not context.sell):
        return
    
    # Set target asset and other context variables
    asset = context.assets[0]
    
    # handle buy and sell transactions
    handle_transactions(context,data,asset)


def handle_transactions(context,data,asset):
    buy = context.buy
    sell = context.sell
    if 'stop_loss' not in context:
        stop_loss = False
    else:
        stop_loss = context.stop_loss

    current_price, return_percent, hold_amount = get_return(context, data, asset)
    if current_price == 0:
        return
        
    # handle buy transaction
    if buy:
        # Buy asset    
        # Target a 100% long allocation of our portfolio in the given asset.
        objective = opt.TargetWeights({asset: 1.0})
        algo.order_optimal_portfolio(objective, [])
        log.info('Buy {0} after {1} periods.'.format(asset, context.hold_days))
        context.hold_days = 0
            
    # handle sell transaction
    if sell: 
        # Sell asset   
        if context.move_fund_out_of_market and (not stop_loss):
            objective = opt.TargetWeights({context.out_of_market: 1.0})
            log.info('Switch to {0} after {1} periods. {2:%}'.format(context.out_of_market, context.hold_days,return_percent))
        else:
            objective = opt.TargetWeights({asset: 0})
            log.info('Sell {0} after {1} periods. {2:%}'.format(asset, context.hold_days, return_percent))
        algo.order_optimal_portfolio(objective, [])
        context.hold_days = 0

def log_performance(context, data):
    if context.log_performance_by_qty:
        if 'prev_qty' not in context:
            context.prev_qty = 0
        qty = context.portfolio.positions[context.assets[0]].amount
        if qty == 0:
            qty = context.prev_qty
        context.prev_qty = qty    
        record(qty=qty)
        return
        
    diff = 0
    benchmark_price = data.current(context.benchmark_asset,'close')
    # print(context.benchmark_asset, benchmark_price)
    if 'prev_asset_price' in context and 'prev_portfolio_value' in context:
        d1 = (benchmark_price - context.prev_asset_price) / context.prev_asset_price
        d2 = (context.portfolio.portfolio_value - context.prev_portfolio_value) / context.prev_portfolio_value
        diff=d2-d1
    else:
        #context.prev_asset_price = context.pipeline_output.price
        context.prev_asset_price = benchmark_price
        context.prev_portfolio_value = context.portfolio.portfolio_value
    record(diff=diff*100)
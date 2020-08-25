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
    context.threshold_stop_loss = -0.05
    context.threshold_hold_days = 1
    # Buy immediately after sell
    context.aggressive_buy = True
    context.move_fund_out_of_market = True
    
    # variable used by the algorithm
    context.buy_signal_count = 0
    context.sell_signal_count = 0
    context.hold_days = 0 # -1 means buy on the first day regardless signal
    context.order = 0
        
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
    # Set target asset and other context variables
    asset = context.assets[0]
    pipeline_name = 'etf_pipeline'
    
    # get the current minute-level price
    cur_price = data.current(asset,'close')
    if cur_price != cur_price:
        return
    context.current_price = cur_price
    
    # get position of the asset
    p = context.portfolio.positions[asset]
    
    context.return_percent = 0
    # calculate return
    if p.amount > 0:
        cost = p.cost_basis
        context.return_percent = (context.current_price - cost)/cost
    
    # get pipeline
    pipe = pipeline_output(pipeline_name).loc[asset]
    context.pipeline_output = pipe

    # log daily performance
    log_daily_performance(context, 0.01)

    # log win/loss 
    log_win_loss(context)
    
    # handle Stop Loss transaction, trigger point: Daily loss or total loss 
    if stop_loss(context, asset):
        return
    
    # the signals are from yesterday
    # each day after market close, I check and count signals
    price_compare = pipe.price
    if (price_compare > pipe.fast_ma and price_compare > pipe.slow_ma):
        context.buy_signal_count += 1
        context.sell_signal_count = 0
    if (price_compare < pipe.slow_ma ) and price_compare < pipe.fast_ma:
        context.buy_signal_count = 0
        context.sell_signal_count += 1
    
    buy = False
    sell = False
    extra_sell_req = True
    if context.buy_signal_count >= context.threshold_signal_count and context.current_price > pipe.fast_ma and context.current_price > pipe.slow_ma:
        buy = True
        context.buy_signal_count = 0
    if context.sell_signal_count > context.threshold_signal_count:
        # aggressive buy
        buy = context.aggressive_buy or pipe.fast_ma > pipe.slow_ma
        sell = not buy
        context.sell_signal_count = 0
    elif context.sell_signal_count >= context.threshold_signal_count and context.current_price < pipe.slow_ma and context.current_price < pipe.fast_ma:
        sell = True
    extra_sell_req = context.return_percent < context.threshold_sell_loss or context.return_percent > context.threshold_sell_win
        
    context.buy = buy
    context.sell = sell
    context.extra_sell_req = extra_sell_req
    
    # handle buy and sell transactions
    handle_transactions(context, asset)
        
    context.hold_days += 1


def stop_loss(context, asset):
    # handle Stop Loss transaction, trigger point: Daily loss or total loss 
    pipe = context.pipeline_output
    p = context.portfolio.positions[asset]

    if (context.current_price - pipe.price)/pipe.price < context.threshold_stop_loss and context.return_percent < context.threshold_sell_loss and p.amount > 0:
        objective = opt.TargetWeights({asset: 0})
        algo.order_optimal_portfolio(objective, [])
        context.hold_days = 0
        context.sell_signal_count = 0
        context.buy_signal_count = 0
        log.info('Close all positions to stop loss: ' + str(context.return_percent))
        context.order = context.current_price
        return True
    else:
        return False


def handle_transactions(context, asset):
    buy = context.buy
    sell = context.sell
    extra_sell_req = context.extra_sell_req
        
    # handle buy transaction
    if buy and context.portfolio.positions[asset].amount == 0 or context.hold_days == -1:
        # Buy asset    
        # Target a 100% long allocation of our portfolio in the given asset.
        objective = opt.TargetWeights({asset: 1.0})
        algo.order_optimal_portfolio(objective, [])
        log.info('Buy {0} after {1} periods.'.format(asset, context.hold_days))
        context.order = context.current_price 
        context.hold_days = 0
            
    # handle sell transaction
    if sell and context.portfolio.positions[asset].amount > 0 and context.hold_days > context.threshold_hold_days and extra_sell_req: 
        # Sell asset   
        if context.move_fund_out_of_market:
            objective = opt.TargetWeights({context.out_of_market: 1.0})
            log.info('Switch to {0} after {1} periods.{2:%}'.format(context.out_of_market, context.hold_days,context.return_percent))
        else:
            objective = opt.TargetWeights({asset: 0})
            log.info('Sell {0} after {1} periods.{2:%}'.format(asset, context.hold_days, context.return_percent))
        algo.order_optimal_portfolio(objective, [])
        context.order = -1 * context.current_price       
        context.hold_days = 0


def log_daily_performance(context, ignore=0.01):
    diff = 0
    if 'prev_asset_price' in context and 'prev_portfolio_value' in context:
        #d1 = (context.pipeline_output.price - context.prev_asset_price) / context.prev_asset_price
        d1 = (context.current_price - context.prev_asset_price) / context.prev_asset_price
        d2 = (context.portfolio.portfolio_value - context.prev_portfolio_value) / context.prev_portfolio_value
        diff=d2-d1
    if abs(diff) < ignore:
        diff = 0
    record(diff=diff)
    #context.prev_asset_price = context.pipeline_output.price
    context.prev_asset_price = context.current_price
    context.prev_portfolio_value = context.portfolio.portfolio_value


def log_win_loss(context):
    current_price = context.current_price
    if context.order > 0:
        if context.order > current_price:
            log.info('The Buy order is a loss: {0:.2f} vs {1:.2f}'.format(context.order,current_price))
        else:
            log.info('The Buy order is a win: {0:.2f} vs {1:.2f}'.format(context.order,current_price))
        context.order = 0
    elif context.order < 0:
        context.order = -1 * context.order
        if context.order > current_price:
            log.info('The Sell order is a win: {0:.2f} vs {1:.2f}'.format(context.order,current_price))
        else:
            log.info('The Sell order is a loss: {0:.2f} vs {1:.2f}'.format(context.order,current_price))
        context.order = 0
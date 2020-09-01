"""
This is a pipeline to return moving averages and price daily.
It compares moving averages with the price and gives recommendation.
"""

# Import pipeline built-ins.
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage, EWMA, ExponentialWeightedMovingAverage, PercentChange, RSI
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.filters import StaticAssets
from quantopian.research import run_pipeline

from datetime import date, timedelta

# Parameters
slow_ma_periods = 20
fast_ma_periods = 10
assets = [
    symbols('VGT'),
    symbols('ESPO'),
    symbols('IBUY'),
    symbols('TAN'),
    symbols('WCLD'),
    symbols('ARKF'),
    symbols('ARKG'),
    symbols('ARKW'),
    symbols('STAG'),
    symbols('IRT'),
    symbols('CHNG'),
]

# Define factors.
fast_ma = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=fast_ma_periods)
slow_ma = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=slow_ma_periods)
# ema = EWMA.from_span(inputs=[EquityPricing.close], window_length=slow_ma_periods*2+20, span=slow_ma_periods,)
rsi = RSI(inputs=[EquityPricing.close], window_length=5)
price = EquityPricing.close.latest
sell = (price < fast_ma) & (price < slow_ma)
buy = (price > fast_ma) & (price > slow_ma)

# Define a filter.
base_universe = StaticAssets(assets)

# define pipeline
pipe = Pipeline(
    columns={
        'yersterday_price': price,
        'ma_fast': fast_ma,
        'ma_slow': slow_ma,
        #'ema': ema,
        'rsi': rsi,
        'signal_sell': sell,
        'signal_buy': buy
    },
    screen= base_universe 
)

end = (date.today() - timedelta(days=0)).strftime('%Y-%m-%d')
start = (date.today() - timedelta(days=100)).strftime('%Y-%m-%d')
# print(start,end)
my_pipeline_result = run_pipeline(pipe, start, end)

# show results of last two days
#retro_days = 1
#indecies = []
#for i in range(len(assets)):
#    indecies.append(-1*(i+1))
#    indecies.append(-1*(i+1) - len(assets)*retro_days)
#my_pipeline_result.iloc[indecies]

# display summarized result
# these dates are used as the first index
all_dates=my_pipeline_result.index.get_level_values(0)
unique_dates = sorted(list(dict.fromkeys(all_dates)))
current_date = unique_dates[-1]
previous_date = unique_dates[-2]
for asset in assets:
    current = my_pipeline_result.loc[(current_date,asset)]
    previous = my_pipeline_result.loc[(previous_date,asset)]
    signal = 'None'
    if current.signal_buy and previous.signal_buy:
        signal = 'Buy'
    if current.signal_sell and previous.signal_sell:
        signal = 'Sell'
    print('{0} {1} RSI={2:.2f} SMA10={3:.2f} SMA20={4:.2f}'.format(str(asset).ljust(22), signal.ljust(8), current.rsi,current.ma_fast, current.ma_slow))

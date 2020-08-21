"""
This is a pipeline to return moving averages and price daily.
It compares moving averages with the price and gives recommendation.
"""

# Import pipeline built-ins.
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.factors import EWMA, ExponentialWeightedMovingAverage
from quantopian.pipeline.factors import PercentChange
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.filters import StaticAssets
from quantopian.research import run_pipeline

from datetime import date, timedelta

# Parameters
slow_ma_periods = 20
fast_ma_periods = 10
assets = [
    symbols('VGT'),
    symbols('STAG'),
    symbols('TAN'),
    symbols('IBUY'),
    symbols('ADMA'),
    symbols('ARKF'),
    symbols('ARKG'),
    symbols('ARKW'),
    symbols('CHNG'),
    symbols('ESPO'),
    symbols('IRT'),
]


# Define factors.
fast_ma = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=fast_ma_periods)
slow_ma = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=slow_ma_periods)
ema = EWMA.from_span(inputs=[EquityPricing.close], window_length=slow_ma_periods*2+20, span=slow_ma_periods,)
price = EquityPricing.close.latest
sell = price < fast_ma and price < slow_ma
buy = price > fast_ma and price > slow_ma

# Define a filter.
base_universe = StaticAssets(assets)

# define pipeline
pipe = Pipeline(
    columns={
        'yersterday_price': price,
        'fast_ma': fast_ma,
        'slow_ma': slow_ma,
        'ema': ema,
        'sell': sell,
        'buy': buy
    },
    screen= base_universe 
)

end = (date.today()).strftime('%Y-%m-%d')
start = (date.today() - timedelta(days=100)).strftime('%Y-%m-%d')
my_pipeline_result = run_pipeline(pipe, start, end)

# show results of last two days
retro_days = 1
indecies = []
for i in range(len(assets)):
    indecies.append(-1*(i+1))
    indecies.append(-1*(i+1) - len(assets)*retro_days)
my_pipeline_result.iloc[indecies]
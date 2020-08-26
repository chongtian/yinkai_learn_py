import numpy as np
import pandas as pd

# Import pipeline built-ins.
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import PercentChange, FastStochasticOscillator, SimpleMovingAverage, RSI
from quantopian.research import run_pipeline
from quantopian.pipeline.filters import QTradableStocksUS, StaticAssets
from quantopian.pipeline.data import EquityPricing

# The list of tickers is from extract_etfdb.py
#tickers = ['AAXJ', 'ACWI', 'ACWX', 'ARKG', 'ARKK', 'ARKW', 'ASHR', 'BBEU', 'BBJP', 'BOTZ', 'CLOU', 'DBEF', 'DGRO', 'DIA', 'DON', 'DVY', 'DXJ', 'EEM', 'EFA', 'EFAV', 'EFG', 'EFV', 'EIDO', 'EPP', 'ESGE', 'ESGU', 'EUFN', 'EWA', 'EWC', 'EWG', 'EWH', 'EWI', 'EWJ', 'EWL', 'EWP', 'EWQ', 'EWS', 'EWT', 'EWU', 'EWW', 'EWY', 'EWZ', 'EZU', 'FCG', 'FDN', 'FENY', 'FEZ', 'FNDA', 'FNDE', 'FNDF', 'FNDX', 'FVD', 'FXD', 'FXI', 'FXN', 'FXU', 'GDX', 'GDXJ', 'GUNR', 'HEFA', 'HEZU', 'IBB', 'ICLN', 'IDV', 'IEFA', 'IEMG', 'IEZ', 'IGV', 'IJH', 'IJR', 'ILF', 'INDA', 'ITB', 'ITOT', 'IUSG', 'IUSV', 'IVE', 'IVV', 'IVW', 'IWB', 'IWD', 'IWF', 'IWM', 'IWN', 'IWR', 'IWS', 'IXC', 'IXUS', 'IYE', 'JETS', 'KBE', 'KBWB', 'KRE', 'KWEB', 'MCHI', 'MDY', 'MDYG', 'MDYV', 'MJ', 'MTUM', 'NOBL', 'OIH', 'PXH', 'QQQ', 'QUAL', 'QYLD', 'RODM', 'RSP', 'RSX', 'SCHA', 'SCHB', 'SCHD', 'SCHE', 'SCHF', 'SCHG', 'SCHV', 'SCHX', 'SCZ', 'SDY', 'SIL', 'SILJ', 'SKYY', 'SMH', 'SOXX', 'SPDW', 'SPEM', 'SPHD', 'SPHQ', 'SPLG', 'SPLV', 'SPMD', 'SPSM', 'SPTM', 'SPY', 'SPYD', 'SPYG', 'SPYV', 'TAN', 'USMV', 'VB', 'VBR', 'VDE', 'VEA', 'VEU', 'VFH', 'VGK', 'VGT', 'VIG', 'VLUE', 'VO', 'VOO', 'VPL', 'VT', 'VTI', 'VTV', 'VUG', 'VWO', 'VXUS', 'VYM', 'WCLD', 'XBI', 'XHB', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XME', 'XOP', 'XRT', 'XSLV']
tickers = ['AAXJ', 'ACWI', 'ACWX', 'ASHR', 'BBEU', 'BBJP', 'BOTZ', 'CLOU', 'DBEF', 'DGRO', 'DIA', 'DON', 'DVY', 'DXJ', 'EEM', 'EFA', 'EFAV', 'EFG', 'EFV', 'EIDO', 'EPP', 'ESGE', 'ESGU', 'EUFN', 'EWA', 'EWC', 'EWG', 'EWH', 'EWI', 'EWJ', 'EWL', 'EWP', 'EWQ', 'EWS', 'EWT', 'EWU', 'EWW', 'EWY', 'EWZ', 'EZU', 'FCG', 'FDN', 'FENY', 'FEZ', 'FNDA', 'FNDE', 'FNDF', 'FNDX', 'FVD', 'FXD', 'FXI', 'FXN', 'FXU', 'GDX', 'GDXJ', 'GUNR', 'HEFA', 'HEZU', 'IBB', 'ICLN', 'IDV', 'IEFA', 'IEMG', 'IEZ', 'IGV', 'IJH', 'IJR', 'ILF', 'INDA', 'ITB', 'ITOT', 'IUSG', 'IUSV', 'IVE', 'IVV', 'IVW', 'IWB', 'IWD', 'IWF', 'IWM', 'IWN', 'IWR', 'IWS', 'IXC', 'IXUS', 'IYE', 'JETS', 'KBE', 'KBWB', 'KRE', 'KWEB', 'MCHI', 'MDY', 'MDYG', 'MDYV', 'MJ', 'MTUM', 'NOBL', 'OIH', 'PXH', 'QQQ', 'QUAL', 'QYLD', 'RODM', 'RSP', 'RSX', 'SCHA', 'SCHB', 'SCHD', 'SCHE', 'SCHF', 'SCHG', 'SCHV', 'SCHX', 'SCZ', 'SDY', 'SIL', 'SILJ', 'SKYY', 'SMH', 'SOXX', 'SPDW', 'SPEM', 'SPHD', 'SPHQ', 'SPLG', 'SPLV', 'SPMD', 'SPSM', 'SPTM', 'SPY', 'SPYD', 'SPYG', 'SPYV', 'TAN', 'USMV', 'VB', 'VBR', 'VDE', 'VEA', 'VEU', 'VFH', 'VGK', 'VGT', 'VIG', 'VLUE', 'VO', 'VOO', 'VPL', 'VT', 'VTI', 'VTV', 'VUG', 'VWO', 'VXUS', 'VYM', 'WCLD', 'XBI', 'XHB', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XME', 'XOP', 'XRT', 'XSLV']

# Remove ETF's which have high correlation
frame = {}
for ticker in tickers:
    d = get_pricing(ticker, '2019-08-01', '2020-08-25')['price']
    frame[ticker] = d
result = pd.DataFrame(frame) 
cor = result.corr()
nrow, ncol = cor.shape
for i in range(nrow):
    row = cor.iloc[i]
    ticker1 = cor.index[i]
    for j in range(i+1,ncol):
        ticker2 = cor.columns[j]
        if row[j]>0.99:
            #print ticker1, ticker2, row[j]
            if result[ticker1].pct_change(126)[-10] > result[ticker2].pct_change(126)[-10]:
                remove_ticker = ticker2
            else:
                remove_ticker = ticker1
            if remove_ticker in tickers:
                tickers.remove(remove_ticker)

assets = []
for t in tickers:
    assets.append(symbols(t))
return_6m = PercentChange(inputs=[EquityPricing.close], window_length=126)
return_1m = PercentChange(inputs=[EquityPricing.close], window_length=21)
return_2w = PercentChange(inputs=[EquityPricing.close], window_length=10)
stoch_k = FastStochasticOscillator([EquityPricing.close,EquityPricing.low,EquityPricing.high])
stoch_d = SimpleMovingAverage(inputs=[stoch_k], window_length=4) 
rsi = RSI(inputs=[EquityPricing.close])
base_universe = StaticAssets(assets) 
pipe = Pipeline(
    columns={
        'return_6m': return_6m,
        'return_1m': return_1m,
        'return_2w': return_2w,
        'stoch_d': stoch_d,
        'rsi': rsi
    },
    screen= base_universe & (return_2w >= return_1m) #& (return_2w > 0) & (stoch_d > 0)
)

pipeline_result = run_pipeline(pipe, '2020-08-25', '2020-08-25')
m = np.median(pipeline_result['return_6m'])
candidates = pipeline_result[pipeline_result['return_6m'] > m]
m = np.median(candidates['return_1m'])
candidates = candidates[candidates['return_1m'] > m]
m = np.median(candidates['return_2w'])
candidates = candidates[candidates['return_2w'] > m]
candidates['rsi'].nsmallest(5)

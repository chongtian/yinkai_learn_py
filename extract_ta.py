# Extract TA data from tradingview.com

import requests
import json
from datetime import date

# the file path to save TA data
save_path = '.'

# Complete List of columns
# {"symbols": {
#     "tickers": ["NASDAQ:IBUY"],
#     "query": {"types": []}},
#     "columns": [
#         "Recommend.Other", "Recommend.All", "Recommend.MA",
#         "RSI", "RSI[1]",
#         "Stoch.K", "Stoch.D", "Stoch.K[1]", "Stoch.D[1]",
#         "CCI20", "CCI20[1]",
#         "ADX", "ADX+DI", "ADX-DI", "ADX+DI[1]", "ADX-DI[1]",
#         "AO", "AO[1]",
#         "Mom", "Mom[1]",
#         "MACD.macd", "MACD.signal",
#         "Rec.Stoch.RSI",
#         "Stoch.RSI.K", "Rec.WR", "W.R", "Rec.BBPower", "BBPower", "Rec.UO", "UO",
#         "EMA5", "close", "SMA5", "EMA10", "SMA10", "EMA20", "SMA20", "EMA30", "SMA30", "EMA50", "SMA50", "EMA100", "SMA100", "EMA200", "SMA200",
#         "Rec.Ichimoku", "Ichimoku.BLine", "Rec.VWMA", "VWMA", "Rec.HullMA9", "HullMA9",
#         "Pivot.M.Classic.S3", "Pivot.M.Classic.S2", "Pivot.M.Classic.S1", "Pivot.M.Classic.Middle", "Pivot.M.Classic.R1",
#         "Pivot.M.Classic.R2", "Pivot.M.Classic.R3", "Pivot.M.Fibonacci.S3", "Pivot.M.Fibonacci.S2", "Pivot.M.Fibonacci.S1",
#         "Pivot.M.Fibonacci.Middle", "Pivot.M.Fibonacci.R1", "Pivot.M.Fibonacci.R2", "Pivot.M.Fibonacci.R3", "Pivot.M.Camarilla.S3",
#         "Pivot.M.Camarilla.S2", "Pivot.M.Camarilla.S1", "Pivot.M.Camarilla.Middle", "Pivot.M.Camarilla.R1", "Pivot.M.Camarilla.R2",
#         "Pivot.M.Camarilla.R3", "Pivot.M.Woodie.S3", "Pivot.M.Woodie.S2", "Pivot.M.Woodie.S1", "Pivot.M.Woodie.Middle", "Pivot.M.Woodie.R1",
#         "Pivot.M.Woodie.R2", "Pivot.M.Woodie.R3", "Pivot.M.Demark.S1", "Pivot.M.Demark.Middle", "Pivot.M.Demark.R1"]}


def extract_data(full_ticker):
    url = 'https://scanner.tradingview.com/america/scan'
    headers = {'Content-Type': 'application/json'}
    # Get JSON from tradingview.com web api
    payload = {
        "symbols": {"tickers": [full_ticker],
                    "query": {"types": []}},
        "columns": ["RSI", "Stoch.K", "SMA10", "EMA20"]}
    try:
        response = requests.request(
            'POST', url, headers=headers, data=json.dumps(payload))
        raw_response = response.text
        # print(raw_response)
        data_returns = json.loads(raw_response)
        return data_returns["data"][0]["d"]
    except:
        print('Failed to extract data.')


tickers = ['NASDAQ:IBUY', 'NASDAQ:ADMA', 'AMEX:ARKF', 'AMEX:ARKG', 'AMEX:ARKW', 'NASDAQ:CHNG',
           'NASDAQ:ESPO', 'NYSE:IRT', 'NYSE:STAG', 'AMEX:TAN', 'AMEX:VGT']
data = {}
for t in tickers:
    ta = extract_data(t)
    data[t] = ta

ta_csv_file = save_path + '\\_' + date.today().strftime('%Y%m%d')+'.csv'
with open(ta_csv_file, 'w', encoding='utf-8') as f:
    f.write('RSI,Stoch.K,SMA10,EMA20,Ticker\n')
    for t in tickers:
        for ta in data[t]:
            f.write(str(ta)+',')
        f.write(t+'\n')
    f.close()

print(data)
print('Extract ta data and save to ' + ta_csv_file)

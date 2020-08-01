# This function can parse Vanguard fund holding.
# For example, visit url https://investor.vanguard.com/mutual-funds/profile/portfolio/VWUSX/portfolio-holdings
# Use Chrome developer console to capture JS network flow
# Filter stock.jsonp
# And save it to a local file

import json

jsonp_file = 'd:\\temp\\stock.jsonp'
output_csv_file = 'd:\\temp\\vanguard.csv' 

def extract_json_from_jsonp(jsonp_text):
    i = jsonp_text.find('{')
    return jsonp_text[i:-1]

def parse_json(json_text):
    funds = json.loads(json_text)
    return funds['fund']['entity']

jsonp_text = ''
with open(jsonp_file, 'r', encoding='utf-8') as f:
    jsonp_text = f.read()
    f.close()
funds = parse_json(extract_json_from_jsonp(jsonp_text))

# calculate weight
total_market_value = 0
for fund in funds:
    total_market_value += float(fund['marketValue'])
for fund in funds:
    fund['weight']=float(fund['marketValue'])/total_market_value

with open(output_csv_file, 'w', encoding='utf-8') as f:
    f.write('Long Name,Short Name,Ticker,Shares,Market Value,Weight\n')
    for fund in funds:
        f.write(fund['longName']+',')
        f.write(fund['shortName']+',')
        f.write(fund['ticker']+',')
        f.write(fund['sharesHeld']+',')
        f.write(fund['marketValue']+',')
        f.write(str(fund['weight']))
        f.write('\n')
    f.close()

print('Extract vanguard fund data and save to ' + output_csv_file)

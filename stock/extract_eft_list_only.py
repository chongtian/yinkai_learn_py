# Extract data from etddb.com

import requests
import json
# import statistics

# the file path to save ETF data
etf_csv_file = 'etf_screen_result.txt'

url = 'https://etfdb.com/api/screener/'
headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/json'
}
page = 1
max_page = 100000
count = 0
total_count = 0
data = []

# Get JSON from etfdb.com web api
while(page <= max_page):
    payload = {
        "page": 1,
        "tab": "returns",
        "sort_by": "fifty_two_week",
        "sort_direction": "desc",
        "asset_class": "equity",
        "leveraged": "false",
        "inverse": "false",
        "expense_ratio_end": "0.8",
        "average_volume_start": "500000",
        #"fifty_two_week_start": "1",
        #"ytd_start": "1",
        # "four_week_ff_start": "0",
        # "one_year_ff_start": "0",
        "only": [
            "meta",
            "data"
        ]
    }

    error_count = 0
    try:
        payload['page'] = page
        payload['tab'] = 'returns'
        response = requests.request(
            'POST', url, headers=headers, data=json.dumps(payload))
        raw_response = response.text.encode('utf8')
        data_returns = json.loads(raw_response)
        # only get/assign max_page and total_count once
        if(max_page == 100000):
            max_page = data_returns['meta']['total_pages']
        if(total_count == 0):
            total_count = data_returns['meta']['total_records']

        for i in range(len(data_returns['data'])):
            etf = data_returns['data'][i]
            data.append(etf)
            count = count + 1

        print('Download date from page ' + str(page))
        page = page + 1
    except:
        # page = max_page + 1
        error_count = error_count + 1
        if error_count > 2:
            page = max_page + 1
            print('Failed to download data from page ' + str(page))
print('Get ' + str(count) + ' of ' + str(total_count) + ' records from etfdb.')


lines = ['Symbol,Name']
py_tickers = 'tickers = ['
for d in data:
    line = d['symbol']['text'] + ',' + d['name']['text']
    lines.append(line)
    py_tickers += "symbol('{0}'),".format(d['symbol']['text'])
py_tickers += ']'

with open(etf_csv_file, 'w', encoding='utf-8') as f:
    f.write(py_tickers)
    f.write('\n')
    for line in lines:
        f.write(line)
        f.write('\n')
    f.close()

print(py_tickers)
print('Extract eft data and save to ' + etf_csv_file)

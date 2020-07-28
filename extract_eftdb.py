# Extract data from etddb.com

import requests
import json
# import statistics

# the file path to save ETF data
etf_csv_file = 'c:\\temp\\etf_screen_result.csv'


def replace_na_with_mean(data, tag):
    total = 0
    count = 0
    indexes = []
    for i in range(len(data)):
        if(data[i][tag] == 'N/A'):
            indexes.append(i)
        else:
            count = count + 1
            total = total + float(data[i][tag].strip('%'))
    avg = total / count
    for i in indexes:
        data[i][tag] = str(avg) + '%'
    return data


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
        "average_volume_start": "100000",
        #"fifty_two_week_start": "1",
        "ytd_start": "1",
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

        payload['tab'] = 'risk'
        response = requests.request(
            'POST', url, headers=headers, data=json.dumps(payload))
        raw_response = response.text.encode('utf8')
        data_risk = json.loads(raw_response)

        for i in range(len(data_returns['data'])):
            # remove etf which has negative returns
            etf = data_returns['data'][i]
            if(etf['four_week_return'][0] == '-' or etf['three_ytd'][0] == '-' or etf['five_ytd'][0] == '-' or etf['fifty_two_week'][0] == '-' or etf['ytd'][0] == '-'):
                print('Removed ' + etf['symbol']['text'] + ' from the list.')
                continue
            etf_risk = data_risk['data'][i]
            etf.update(etf_risk)
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

# Process and clean up data
# get means of 1y, 3y, 5y returns. The means will be used to replace N/A values
data = replace_na_with_mean(data, 'fifty_two_week')
data = replace_na_with_mean(data, 'three_ytd')
data = replace_na_with_mean(data, 'five_ytd')

lines = ['Symbol,Name,4 Weeks,1 Year,3 Year,5 Year,YTD,std,20d v,50d v,200d v']
for d in data:
    line = d['symbol']['text'] + ',' + d['name']['text'] + ',' + d['four_week_return'] + ',' + d['fifty_two_week'] + ',' + d['three_ytd'] + ',' + d['five_ytd'] + \
        ',' + d['ytd'] + ',' + d["standard_deviation"] + ',' + d['twenty_day_volatility'] + \
        ',' + d['fifty_day_volatility'] + ',' + d['two_hundred_day_volatility']
    lines.append(line)

with open(etf_csv_file, 'w', encoding='utf-8') as f:
    for line in lines:
        f.write(line)
        f.write('\n')
    f.close()

print('Extract eft data and save to ' + etf_csv_file)

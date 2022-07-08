import json
import pandas as pd

import requests

#
# url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"
#
# headers = {
#     "X-RapidAPI-Key": "381e829054mshcf52bd8c38ec006p1d3f54jsn09797f52766e",
#     "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
# }
#
# response = requests.request("GET", url, headers=headers)
#
# data = response.json()
#
# with open("/Users/gauravkothyari/Documents/Sigmoid/sparkRestAPI/Data_files/stocks_list.json", "w") as dt:
#     json.dump(data['stocks'][:25], dt)

with open('/Data_files/stocks_list.json', 'r') as dt:
    json_object = json.load(dt)
# for x in json_object:
#     print(x)

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
for x in json_object:
    url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

    querystring = {"ticker_symbol": x, "years": "1", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "89115e2256mshdaf8dadb13f9b7fp143b12jsn6293d9c451b1",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    # print(response.text)
    data = response.json()
    # print(f"{x} {data['historical prices']}")
    # f1 = open('/Users/gauravkothyari/Documents/Sigmoid/sparkRestAPI/Data_files/stocks_data.json', 'w')
    # f1.write(str(data['historical prices']))
    # data['historical prices']['company'] = x
    # filename = f"/Users/gauravkothyari/Documents/Sigmoid/sparkRestAPI/csv_files/{x}.json"
    json_data = json.dumps(data['historical prices'], indent=4)

    df = pd.read_json(json_data)
    df['company'] = x
    filename = f'/Users/gauravkothyari/Documents/Sigmoid/sparkRestAPI/csv_files/{x}.csv'
    df.to_csv(filename, encoding='utf-8', index=False)

    # # with open(filename, "w") as dt:
    # #     json.dump(data['historical prices'], dt)
    print(f'Data for {x} inserted')

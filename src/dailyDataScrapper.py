# import required library and required constants
import time
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from utils.status import getStatus
from constants.url import dailyPriceUrl

html = requests.get(dailyPriceUrl).text
bs = BeautifulSoup(html, "lxml")

# today date in yyyy-mm-dd format
today = bs.find("span", {"class": "text-org"}).text

# get html tables
tables = pd.read_html(html)

# select the first table i.e. the stock price table
dataTable = tables[0]

fileDir = Path("../data/company-wise/")
import pandas.errors

for file in fileDir.glob("*.csv"):
    if file.stat().st_size == 0:
        print(f"[EMPTY] {file}")
        continue
    try:
        existingDf = pd.read_csv(file)
    except pandas.errors.EmptyDataError:
        print(f"[EMPTY DATA] {file}")
        continue
    # first check if data already exist for this date
    lastRow = existingDf.iloc[-1]
    lastDate = lastRow["published_date"]
    if str(lastDate) != str(today):
        symbol = file.stem
        data = dataTable.loc[dataTable["Symbol"] == symbol]
        print(f"Checking symbol: {symbol}")
        print(f"Available symbols in table: {list(dataTable['Symbol'])[:10]}")
        if len(data) == 1:
            status = getStatus(float(data["Open"].iloc[0]), float(data["Close"].iloc[0]))
            dataRow = [
                [
                    today,
                    float(data["Open"].iloc[0]),
                    float(data["High"].iloc[0]),
                    float(data["Low"].iloc[0]),
                    float(data["Close"].iloc[0]),
                    float(data["Diff %"].iloc[0]),
                    float(data["Vol"].iloc[0]),
                    float(data["Turnover"].iloc[0]),
                    status,
                ]
            ]
            dataframe = pd.DataFrame(dataRow)
            dataframe.to_csv(file, mode="a", header=False, index=False)
            print(f"[UPDATED] {file} with today's data.")

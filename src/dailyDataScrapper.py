# import required library and required constants
import time
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from utils.status import getStatus
from constants.url import dailyPriceUrl



import re


html = requests.get(dailyPriceUrl).text
bs = BeautifulSoup(html, "lxml")

# today date in yyyy-mm-dd format (use span.text-org if available, else fallback to regex)
span = bs.find("span", {"class": "text-org"})
if span and span.text:
    today = span.text.strip()
else:
    import re
    date_match = re.search(r"As of\s*:\s*(\d{4}-\d{2}-\d{2})", html)
    if date_match:
        today = date_match.group(1)
    else:
        today = None
        print("[WARNING] Could not extract today's date from HTML!")

# get html tables
tables = pd.read_html(html)

# select the first table i.e. the stock price table
dataTable = tables[0]

fileDir = Path("../data/company-wise/")
from constants.columns import columns as required_columns

for file in fileDir.glob("*.csv"):
    # Check if the CSV is empty
    try:
        existingDf = pd.read_csv(file)
        is_empty = existingDf.empty
    except Exception:
        is_empty = True

    # If not empty, check if data already exists for this date
    lastDate = None
    if not is_empty:
        lastRow = existingDf.iloc[-1]
        lastDate = lastRow["published_date"]

    # Extract symbol from filename (without extension)
    symbol = file.stem
    print(f"\n[DEBUG] Processing file: {file}")
    print(f"[DEBUG] Last date in CSV: {lastDate}")
    print(f"[DEBUG] Today's date: {today}")
    print(f"[DEBUG] Symbol to match: {symbol}")
    data = dataTable.loc[dataTable["Symbol"] == symbol]
    print(f"[DEBUG] Data found for symbol: {not data.empty}, Rows: {len(data)}")
    if len(data) == 1:
        open_ = float(data["Open"].iloc[0])
        close_ = float(data["Close"].iloc[0])
        high_ = float(data["High"].iloc[0])
        low_ = float(data["Low"].iloc[0])
        traded_quantity = float(data["Vol"].iloc[0])
        traded_amount = float(data["Turnover"].iloc[0])
        # Calculate per_change as (close - open) / open * 100
        if open_ != 0:
            per_change = round((close_ - open_) / open_ * 100, 2)
        else:
            per_change = 0.0
        status = getStatus(open_, close_)
        # Build row in the order matching the CSV structure (without DT_Row_Index)
        dataRow = [
            today,           # published_date
            open_,           # open
            high_,           # high
            low_,            # low
            close_,          # close
            per_change,      # per_change
            traded_quantity, # traded_quantity
            traded_amount,   # traded_amount
            status,          # status
        ]
        print(f"[DEBUG] Data row to append: {dataRow}")
        # Use only the first 9 columns (exclude DT_Row_Index if it exists in required_columns)
        csv_columns = required_columns[:9] if len(required_columns) > 9 else required_columns
        dataframe = pd.DataFrame([dataRow], columns=csv_columns)
        # If file is empty, write header; else, only append if date is new
        if is_empty:
            print(f"[DEBUG] CSV is empty. Writing header and row.")
            dataframe.to_csv(file, mode="w", header=True, index=False)
        elif str(lastDate) != str(today):
            print(f"[DEBUG] Appending new row for date {today}.")
            dataframe.to_csv(file, mode="a", header=False, index=False)
        else:
            print(f"[DEBUG] Row for date {today} already exists. Skipping.")
    else:
        print(f"[DEBUG] No data found for symbol {symbol} in today's table.")

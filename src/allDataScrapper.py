# import required lib and configs
import requests
import pandas as pd
from pathlib import Path
from config.headers import headers
from constants.url import historyUrl
from constants.companyIdMap import companyIdMap
from utils.params import getParams
from utils.flatten import flatten
import urllib.parse




def getData(company, session, base_headers):
    # get company symbol
    companySymbol = companyIdMap[company]

    # data collection start from here
    print(f"Collecting data of {company}...")
    print(".........")

    # set params for API, (set size = 1, for start to get the total size of data)
    params = getParams(1, 1, companySymbol)

    # Get fresh CSRF token from session cookies
    csrf_token = session.cookies.get('XSRF-TOKEN')
    csrf_token = urllib.parse.unquote(csrf_token)
    local_headers = base_headers.copy()
    if csrf_token:
        local_headers['X-XSRF-TOKEN'] = csrf_token

    # request API to get data (POST instead of GET)
    response = session.post(historyUrl, headers=local_headers, data=params)
    print(f"[DEBUG] Response status code for {company}: {response.status_code}")
    print(f"[DEBUG] Response text for {company}: {response.text}")

    # get total number of data available
    response_json = response.json()
    if "recordsTotal" in response_json:
        totalRecords = response_json["recordsTotal"]
    else:
        print(f"Key 'recordsTotal' not found in response for {company}:", response_json)
        totalRecords = 0  # or handle as needed

    # set the start to 1 and total size of data to 50
    start = 1
    size = 50

    # totalLoop = total number of iteration we have to do to get full data;
    totalLoop = (totalRecords // 50) + 1

    # intialized an empty array to store data that we got in the loop
    data = []

    # loop
    for i in range(1, totalLoop):
        dataParams = getParams(start, size, companySymbol)
        response = session.post(historyUrl, headers=local_headers, data=dataParams)
        data.append(response.json().get("data", []))
        start = start + 50

    # flat the 2d data to 1d array
    dataArray = flatten(data)

    # convert to dataframe and save to csv in reverse order
    df = pd.DataFrame.from_dict(dataArray)[::-1]

    # remove unwanted column DT_Row_Index if present
    if "DT_Row_Index" in df.columns:
        df = df.drop(columns="DT_Row_Index")

    # save to file
    fileName = f"../data/company-wise/{company}.csv"
    filepath = Path(fileName)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)

    # print after the data collection is completed
    print(".........")
    print(f"Collection completed of {company}...")


if __name__ == "__main__":
    import time
    session = requests.Session()
    # GET request to main page to get fresh cookies and CSRF token
    session.get('https://www.sharesansar.com', headers=headers)
    for company in companyIdMap:
        getData(company, session, headers)
        time.sleep(1)  # Add a small delay to avoid rate limiting

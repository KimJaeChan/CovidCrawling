import logging
import datetime
import csv
import json
import sys
import time
import pandas as pd
import bs4
from bs4 import BeautifulSoup as BS

import azure.functions as func
import requests
from azure.data.tables import TableServiceClient

sys.path.append('./')

def main(mytimer: func.TimerRequest, tablePath:func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    tags1 = covid()
    
    data_covid1 = []

    for i,tag in enumerate(tags1) :
        data_covid = {
            "지역" : tag[0],
            "누적확진자" : int(tag[1].replace(',','')),
            "신규확진자" : int(tag[2].replace(',','')),
            "PartitionKey" : f"지역{i}",
            "RowKey": time.time()
        }
        data_covid1.append(data_covid)
    
    print(data_covid1)
    tablePath.set(json.dumps(data_covid1))


def covid() :
   
    url = "https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query=%EC%BD%94%EB%A1%9C%EB%82%98+%ED%99%95%EC%A7%84%EC%9E%90"
    req = requests.get(url)
    req.raise_for_status() #요청/응답 코드가 200이 아니면 예외 발생

    soup = BS(req.text,"lxml")
    
    filename = "covid_index.csv"
    f = open(filename, "w", encoding="utf_8_sig",newline= "")
    writer = csv.writer(f)
    
    covid_list = []
    try:
        covid_data = soup.find("table",attrs={"class" :"table"}).find("tbody").find_all("tr")
        for data in covid_data:
            columns = data.find_all("td")
            if len(columns) <= 1 :
                    continue
            d_data = [column.get_text().strip() for column in columns]
            covid_list.append(d_data)
            writer.writerow(d_data)
    except IndexError : 
        pass
    return covid_list



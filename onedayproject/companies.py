import requests
import csv
import os
from dotenv import load_dotenv
load_dotenv()

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
CompanyName1=["AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "ALGN", "AMAT", "AMD", "AMGN", "AMZN", "ANSS","ASML"]
for company in CompanyName1:
   querystring = {"ticker_symbol": company, "years": "5", "format": "json"}

   headers = {
	   "X-RapidAPI-Key": os.environ.get("rapid_api_key"),
	   "X-RapidAPI-Host": os.environ.get("rapid_api_host")
   }

   response = requests.request("GET", url, headers=headers, params=querystring)

   # print(response.text)
   myjson = response.json()
   # print(myjson)
   ourdata = []
   headersName = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Date", "CompanyName"]
   for i in myjson['historical prices']:
      listing = [i["Open"], i["High"], i["Low"], i["Close"], i["Adj Close"], i["Volume"], i["Date"], company]
      ourdata.append(listing)
   with open(f"{company}.csv", 'w', encoding='UTF8', newline='') as f:
      writer = csv.writer(f)
      writer.writerow(headersName)
      for i in ourdata:
         writer.writerow(i)

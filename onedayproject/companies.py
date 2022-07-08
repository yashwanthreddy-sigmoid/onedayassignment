import requests
import csv

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
CompanyName=["AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "ALGN", "AMAT", "AMD", "AMGN", "AMZN", "ANSS", "ASML", "ATVI", "AVGO", "AZN", "BIDU", "BIIB", "BKNG", "CDNS", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO", "CSX", "CTAS", "CTSH", "DDOG", "DLTR", "DOCU", "DXCM", "EA", "EBAY", "EXC", "FAST", "FISV", "FTNT", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN", "INTC", "INTU", "ISRG", "JD", "KDP", "KHC", "KLAC", "LCID", "LRCX", "LULU", "MAR", "MCHP", "MDLZ", "MELI", "META", "MNST", "MRNA", "MRVL", "MSFT", "MTCH", "MU", "NFLX", "NTES", "NVDA", "NXPI", "ODFL", "OKTA", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", "PYPL", "QCOM", "REGN", "ROST", "SBUX", "SGEN", "SIRI", "SNPS", "SPLK", "SWKS", "TEAM", "TMUS", "TSLA", "TXN", "VRSK", "VRSN", "VRTX", "WBA", "WDAY", "XEL", "ZM", "ZS"]

CompanyName1=["AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "ALGN", "AMAT", "AMD", "AMGN", "AMZN", "ANSS","ASML"]
for company in CompanyName1:
   querystring = {"ticker_symbol": company, "years": "5", "format": "json"}

   headers = {
	   "X-RapidAPI-Key": "8fc012ff2emsh5bcc96214cd468dp1cb17ajsn2d6033c574ac",
	   "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
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
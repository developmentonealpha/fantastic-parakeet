import requests

def get_nse_symbols():
    url = "https://scanner.tradingview.com/india/scan"
    payload = {
        "filter": [],
        "options": {"lang": "en"},
        "markets": ["nse"],
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name"]
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    data = response.json()
    symbols = [item['d'][0] for item in data.get('data', [])]
    return symbols

symbols = get_nse_symbols()
print(symbols)

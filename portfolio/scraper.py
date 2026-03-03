import requests
from bs4 import BeautifulSoup
import time, random
import pandas as pd

# Load CSV
report_df = pd.read_csv('Holdings_Dec_25/12_25_report.csv')

# Create empty columns if they don't exist
for col in ["Current Price", "52wk High", "52wk Low"]:
    if col not in report_df.columns:
        report_df[col] = None

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.screener.in/"
}

# Use session for faster requests
session = requests.Session()
session.headers.update(headers)

for idx, ticker in enumerate(report_df["Ticker"]):
    print(f"Fetching {ticker}...")

    url = f"https://www.screener.in/company/{ticker}/"
    try:
        response = session.get(url, timeout=(5, 10))
        if response.status_code == 429:
            print("Rate limited. Cooling down...")
            time.sleep(60)
            continue
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract all li items with class flex flex-space-between
        li_items = soup.find_all("li", class_="flex flex-space-between")

        for li in li_items:
            name_span = li.find("span", class_="name")
            number_spans = li.find_all("span", class_="number")

            if not name_span or not number_spans:
                print(f"Failed Fetching {ticker}...")
                continue

            name_text = name_span.text.strip()

            if "Current Price" in name_text:
                report_df.at[idx, "Current Price"] = number_spans[0].text.replace(",", "")

            elif "High / Low" in name_text:
                report_df.at[idx, "52wk High"] = number_spans[0].text.replace(",", "")
                report_df.at[idx, "52wk Low"] = number_spans[1].text.replace(",", "")
                break  # No need to continue once High/Low found
    except requests.exceptions.Timeout:
        print(f"Timeout for {ticker}, skipping...")
    except requests.exceptions.ConnectionError:
        print(f"Connection error for {ticker}, skipping...")
    
    time.sleep(random.uniform(3, 6))

report_df.to_csv('Holdings_Dec_25/12_25_report_updated.csv', index=False)

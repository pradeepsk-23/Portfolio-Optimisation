import os
import re
from collections import defaultdict

import pandas as pd

# ---------------------------
# Configuration
# ---------------------------
EXCLUDED_STOCKS = [
    'SK Hynix Inc', 'Amazon.com Inc', 'Alibaba Group Holding Limited',
    'Contemporary Amperex Technology Co Limited', 'NIKE Inc', 'NVIDIA CORP',
    'Sony group', 'Sony Financial Holdings INC NPV',
    'Innoventive Industries Limited**', 'Epam Systems Inc',
    'Cognizant Technology Solutions Corporation', 'Disa India Ltd.',
    '0.00%  FCDW -  BETA NAPTHOL LT', 'EQ - ELBEE SERVICES LTD.(ELBEE EXPR',
    'EQ - GPI TEXTILES LTD.'
]

MAX_PREFIX_LENGTH = 20
TICKER_CSV_PATH = "nse_ticker.csv"

# ---------------------------
# Utility Functions
# ---------------------------

def clean_stock_name(stock_name):
    """Cleans and standardizes the stock name."""
    if isinstance(stock_name, str):
        stock_name = re.sub(r"^EQ(?=\s|-)", "", stock_name, flags=re.IGNORECASE)
        stock_name = re.sub(r"^EQ\s*\(RTS-PP\)", "", stock_name, flags=re.IGNORECASE)
        stock_name = re.sub(r"&", "and", stock_name, flags=re.IGNORECASE)
        stock_name = re.sub(r"[^A-Za-z0-9]", "", stock_name)
    return stock_name

def safe_float(value):
    """Safely converts value to float."""
    try:
        return float(value)
    except ValueError:
        return None
    
# ---------------------------
# CSV Processing
# ---------------------------

def process_csv_files(main_csv_path):
    # Read the main CSV file
    main_df = pd.read_csv(main_csv_path)
    
    # Folder paths
    folder_path = os.path.dirname(main_csv_path)
    
    # Initialize output DataFrames
    Holdings = {}
    Movement = {}
    Report = {}

    ticker_df = pd.read_csv("nse_ticker.csv")
    ticker_df["CompanyName_clean"] = (
        ticker_df["CompanyName"]
        .astype(str)  
        .apply(clean_stock_name) 
        .str.upper()
        .str.strip()
    )
    # Remove empty / invalid names
    ticker_df = ticker_df[
        ticker_df["CompanyName_clean"].notna() &
        (ticker_df["CompanyName_clean"] != "") &
        (ticker_df["CompanyName_clean"] != "NAN")
    ]
    company_names = ticker_df["CompanyName_clean"].tolist()
    ticker_lookup = dict(
        zip(ticker_df["CompanyName_clean"], ticker_df["Symbol"])
    )

    prefix_index = defaultdict(list)

    for name in company_names:
        for i in range(2, min(len(name), MAX_PREFIX_LENGTH) + 1):
            prefix_index[name[:i]].append(name)

    def prefix_match_ticker(stock_name):
        if not isinstance(stock_name, str):
            return None

        stock = clean_stock_name(stock_name).upper().strip()

        if len(stock) < 2:
            return None

        for i in range(2, min(len(stock), MAX_PREFIX_LENGTH) + 1):
            prefix = stock[:i]
            matches = prefix_index.get(prefix)

            if not matches:
                return None

            if len(matches) == 1:
                return ticker_lookup[matches[0]]

        return None

    # Process each fund
    for fund_name in main_df['Funds']:
        file_path = os.path.join(folder_path, f"{fund_name.replace(' ', '_').replace('/', '_')}.csv")

        if not os.path.exists(file_path):
            continue
        
        # Read raw CSV with no header
        header_row = 2
        preview = pd.read_csv(file_path, header=header_row)
        if preview.iloc[1].astype(str).str.contains("% of AUM", case=False, na=False).any():
            header_row = header_row + 1
        fund_df = pd.read_csv(file_path, header=header_row)

        for _, col in fund_df.iterrows():
            if col.isnull().any():
                print(f"Empty row found in {fund_name}, skipping the rest of the file.")
                break

            stock_name, stock_holding, col3, col5 = col.iloc[0], col.iloc[1], safe_float(col.iloc[2]), safe_float(col.iloc[4])
            if stock_name not in EXCLUDED_STOCKS:    
                if col3 is None:
                    continue
                change = col3 if col5 is None else col3 - col5

                Ticker = prefix_match_ticker(stock_name)
                company_name = ticker_df.loc[ticker_df['Symbol'] == Ticker, 'CompanyName'].values[0]

                # -------- Holdings / Changes --------
                if Ticker not in Holdings:
                    Holdings[Ticker] = {
                        'Ticker': Ticker,
                        'Stock': company_name
                    }
                    Movement[Ticker] = {
                        'Ticker': Ticker,
                        'Stock': company_name
                    }
                    Report[Ticker] = {
                        'Ticker': Ticker,
                        'Stock': company_name
                    }
                
                Holdings[Ticker][fund_name] = stock_holding
                Movement[Ticker][fund_name] = change

    for ticker, funds in Holdings.items():
        total_holdings = sum(float(value) for key, value in funds.items()
                             if key not in ['Ticker', 'Stock', 'Total Holdings'])
        Report[ticker]["Total holdings"] = total_holdings
    
    for ticker, funds in Movement.items():
        total_movement = sum(float(value) for key, value in funds.items()
                            if key not in ['Ticker', 'Stock', 'Total Holdings'])
        Report[ticker]["Total Movement"] = total_movement
    
    grand_total = sum(data["Total holdings"] for data in Report.values())
    for ticker in Report:
        Report[ticker]["Normalised holdings"] = (
            Report[ticker]["Total holdings"] / grand_total) * 100

    # Save output files
    output_df_holdings = pd.DataFrame.from_dict(Holdings, orient='index')
    output_df_movement = pd.DataFrame.from_dict(Movement, orient='index')
    output_df_report = pd.DataFrame.from_dict(Report, orient='index')

    output_df_holdings.to_csv(os.path.join(folder_path, '12_25_holdings.csv'), index=False)
    print(f"Output saved to 12-25.csv")

    output_df_movement.to_csv(os.path.join(folder_path, '12_25_movement.csv'), index=False)
    print(f"Output saved to 12-25_change.csv")

    output_df_report.to_csv(os.path.join(folder_path, '12_25_report.csv'), index=False)
    print(f"Output saved to 12-25_change.csv")

# Example usage
process_csv_files("Holdings_Dec_25\screener.csv")
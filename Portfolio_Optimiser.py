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
    'Sony group', 'Sony Financial Holdings INC NPV', 'Viatris Inc.',
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
    return stock_name.upper().strip()

def safe_float(value):
    """Safely converts value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

# ---------------------------
# Ticker Matching
# ---------------------------

def build_prefix_index(ticker_df: pd.DataFrame):
    """Build a prefix index for fast ticker lookup."""
    prefix_index = defaultdict(list)
    ticker_lookup = dict(zip(ticker_df['CompanyName_clean'], ticker_df['Symbol']))

    for name in ticker_df['CompanyName_clean']:
        for i in range(2, min(len(name), MAX_PREFIX_LENGTH) + 1):
            prefix_index[name[:i]].append(name)

    def prefix_match(stock_name: str):
        if not stock_name or len(stock_name) < 2:
            return None
        stock = clean_stock_name(stock_name)
        for i in range(2, min(len(stock), MAX_PREFIX_LENGTH) + 1):
            prefix = stock[:i]
            matches = prefix_index.get(prefix)
            if matches is None:
                return None
            if len(matches) == 1:
                return ticker_lookup[matches[0]]
        return None

    return prefix_match
    
# ---------------------------
# CSV Processing
# ---------------------------

def process_csv_files(main_csv_path):
    folder_path = os.path.dirname(main_csv_path)
    main_df = pd.read_csv(main_csv_path)
    
    # Load and clean ticker data
    ticker_df = pd.read_csv(TICKER_CSV_PATH)
    ticker_df["CompanyName_clean"] = ticker_df["CompanyName"].astype(str).apply(clean_stock_name)
    ticker_df = ticker_df[ticker_df["CompanyName_clean"] != ""]
    prefix_match_ticker = build_prefix_index(ticker_df)

    # Initialize dict for all stock data
    stock_data = {}

    for fund_name in main_df['Funds']:
        file_path = os.path.join(folder_path, f"{fund_name.replace(' ', '_').replace('/', '_')}.csv")
        if not os.path.exists(file_path):
            continue
        
        # Detect correct header row
        header_row = 2
        preview = pd.read_csv(file_path, header=header_row)
        if preview.iloc[1].astype(str).str.contains("% of AUM", case=False, na=False).any():
            header_row += 1
        fund_df = pd.read_csv(file_path, header=header_row)

        for _, row in fund_df.iterrows():
            if row.isnull().any():
                print(f"Empty row found in {fund_name}, skipping the rest of the file.")
                break

            stock_name = row.iloc[0]
            if stock_name in EXCLUDED_STOCKS:
                continue

            stock_holding, col3, col5 = safe_float(row.iloc[1]), safe_float(row.iloc[2]), safe_float(row.iloc[4])
            if col3 == 0 and col5 == 0:
                continue
            
            # Compute stock score and movement
            stock_score = 0
            change = 0
            if col5 == 0 and col3 > 0:
                stock_score, change = 1.0, col3
            elif col3 == 0 and col5 > 0:
                stock_score, change = -1.0, -col5
            elif col5 > 0 and col3 > col5:
                stock_score, change = 0.5, col3 - col5
            elif col5 > 0 and 0 < col3 < col5:
                stock_score, change = -0.5, col3 - col5

            Ticker = prefix_match_ticker(stock_name)
            company_name = ticker_df.loc[ticker_df['Symbol'] == Ticker, 'CompanyName'].values[0]

            if Ticker not in stock_data:
                stock_data[Ticker] = {
                    'Ticker': Ticker,
                    'Stock': company_name,
                    'Holdings': {},
                    'Movement': {},
                    'Score': {}
                }
            
            stock_data[Ticker]['Holdings'][fund_name] = stock_holding
            stock_data[Ticker]['Movement'][fund_name] = change
            stock_data[Ticker]['Score'][fund_name] = stock_score

    # Build final report
    report_rows = []
    grand_total = 0.0

    for Ticker, data in stock_data.items():
        total_holdings = sum(data['Holdings'].values())
        total_movement = sum(data['Movement'].values())
        total_score = sum(data['Score'].values())
        grand_total += total_holdings
        report_rows.append({
            'Ticker': Ticker,
            'Stock': data['Stock'],
            'Total Score': total_score,
            'Total Movement': total_movement,
            'Total Holdings': total_holdings,
            'Holdings': data['Holdings'],
            'Movement': data['Movement'],
            'Score': data['Score']
        })

    for row in report_rows:
        row['Normalised Holdings'] = (row['Total Holdings'] / grand_total) * 100 if grand_total else 0

    # Convert to DataFrames
    df_holdings = pd.DataFrame([
        {'Ticker': r['Ticker'], 'Stock': r['Stock'], **r['Holdings']}
        for r in report_rows])
    df_movement = pd.DataFrame([
        {'Ticker': r['Ticker'], 'Stock': r['Stock'], **r['Movement']}
        for r in report_rows])
    df_report = pd.DataFrame([
        {
            'Ticker': r['Ticker'], 
            'Stock': r['Stock'],
            'Total Score': r['Total Score'],
            'Total Movement': r['Total Movement'], 
            'Total Holdings': r['Total Holdings'], 
            'Normalised Holdings': r['Normalised Holdings']
        } for r in report_rows
    ])

    # Save CSVs
    df_holdings.to_csv(os.path.join(folder_path, '12_25_holdings.csv'), index=False)
    df_movement.to_csv(os.path.join(folder_path, '12_25_movement.csv'), index=False)
    df_report.to_csv(os.path.join(folder_path, '12_25_report.csv'), index=False)

    print("All CSVs saved successfully.")

process_csv_files("Holdings_Dec_25\screener.csv")
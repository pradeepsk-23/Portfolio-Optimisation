import os
import pandas as pd
import re
from rapidfuzz import process, fuzz
from collections import defaultdict

Excluded_Stocks = ['SK Hynix Inc',
                   'Amazon.com Inc',
                   'Alibaba Group Holding Limited',
                   'Contemporary Amperex Technology Co Limited',
                   'NIKE Inc',
                   'NVIDIA CORP',
                   'Sony group',
                   'Sony Financial Holdings INC NPV',
                   'Innoventive Industries Limited**',
                   'Epam Systems Inc',
                   'Cognizant Technology Solutions Corporation',
                   'Disa India Ltd.',
                   '0.00%  FCDW -  BETA NAPTHOL LT',
                   'EQ - ELBEE SERVICES LTD.(ELBEE EXPR',
                   'EQ - GPI TEXTILES LTD.']

def process_csv_files(main_csv_path):
    # Read the main CSV file
    main_df = pd.read_csv(main_csv_path)
    
    # Folder paths
    folder_path = os.path.dirname(main_csv_path)
    # small_folder_path = os.path.join(folder_path, "Small")
    
    # Initialize output DataFrames
    output_df = pd.DataFrame(columns=['Stocks'])
    output_df_change = pd.DataFrame(columns=['Stocks'])

    def clean_stock_name(stock_name):
        """Cleans and standardizes the stock name."""
        if isinstance(stock_name, str):
            stock_name = re.sub(r"^EQ(?=\s|-)", "", stock_name, flags=re.IGNORECASE)
            stock_name = re.sub(r"^EQ\s*\(RTS-PP\)", "", stock_name, flags=re.IGNORECASE)
            stock_name = re.sub(r"&", "and", stock_name, flags=re.IGNORECASE)
            stock_name = re.sub(r"[^A-Za-z0-9]", "", stock_name)
        return stock_name

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

    # Build index for prefixes up to reasonable length (say 10)
    MAX_PREFIX = 100

    for name in company_names:
        for i in range(2, min(len(name), MAX_PREFIX) + 1):
            prefix_index[name[:i]].append(name)

    def prefix_match_ticker(stock_name):
        if not isinstance(stock_name, str):
            return None

        stock = clean_stock_name(stock_name).upper().strip()

        if len(stock) < 2:
            return None

        for i in range(2, min(len(stock), MAX_PREFIX) + 1):
            prefix = stock[:i]
            matches = prefix_index.get(prefix)

            if not matches:
                return None

            if len(matches) == 1:
                return ticker_lookup[matches[0]]

        return None

    def safe_float(value):
        """Safely converts value to float."""
        try:
            return float(value)
        except ValueError:
            return None

    # Process each fund
    for fund_name in main_df['Funds']:
        file_path = os.path.join(folder_path, f"{fund_name.replace(' ', '_').replace('/', '_')}.csv")

        if not os.path.exists(file_path):
            continue

        fund_df = pd.read_csv(file_path, header=None, skiprows=4)
        if fund_name not in output_df.columns:
            output_df[fund_name] = None
            output_df_change[fund_name] = None

        processed_stocks = set()
        processed_stocks_change = set()

        for _, row in fund_df.iterrows():
            if row.isnull().any():
                print(f"Empty row found in {fund_name}, skipping the rest of the file.")
                break

            stock_name, stock_value, col3, col5 = row[0], row[1], safe_float(row[2]), safe_float(row[4])
            if stock_name not in Excluded_Stocks:    
                if col3 is None:
                    continue
                change = col3 if col5 is None else col3 - col5

                stock_name = prefix_match_ticker(stock_name)

                if stock_name not in processed_stocks:
                    if stock_name.lower() not in output_df['Stocks'].str.lower().tolist():
                        output_df = pd.concat([output_df, pd.DataFrame({'Stocks': [stock_name], fund_name: [stock_value]})], ignore_index=True)
                    else:
                        output_df.loc[output_df['Stocks'].str.lower() == stock_name.lower(), fund_name] = stock_value
                    processed_stocks.add(stock_name)

                if stock_name not in processed_stocks_change:
                    if stock_name.lower() not in output_df_change['Stocks'].str.lower().tolist():
                        output_df_change = pd.concat([output_df_change, pd.DataFrame({'Stocks': [stock_name], fund_name: [change]})], ignore_index=True)
                    else:
                        output_df_change.loc[output_df_change['Stocks'].str.lower() == stock_name.lower(), fund_name] = change
                    processed_stocks_change.add(stock_name)

    # Save output files
    output_df.to_csv(os.path.join(folder_path, '12-25.csv'), index=False)
    print(f"Output saved to 12-25.csv")

    output_df_change.to_csv(os.path.join(folder_path, '12-25_change.csv'), index=False)
    print(f"Output saved to 12-25_change.csv")

# Example usage
process_csv_files("Holdings_Dec_25\screener.csv")
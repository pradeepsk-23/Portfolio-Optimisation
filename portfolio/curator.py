import os
import pandas as pd
import re
from rapidfuzz import process, fuzz

def process_csv_files(main_csv_path):
    # Read the main CSV file
    main_df = pd.read_csv(main_csv_path)
    
    # Folder paths
    folder_path = os.path.dirname(main_csv_path)
    # small_folder_path = os.path.join(folder_path, "Small")
    
    # Initialize output DataFrames
    output_df = pd.DataFrame(columns=['Stocks'])
    output_df_change = pd.DataFrame(columns=['Stocks'])

    # Keywords to remove
    keywords = ['Ltd', 'Ltd.', 'Ltd*', 'Limited']
    keyword_pattern = re.compile(rf"\b({'|'.join(keywords)})\b.*", flags=re.IGNORECASE)

    # String replacements map
    replacements = {
        ' and ': ' & ', ' AND ': ' & ', ' And ': ' & ', 'Co.': 'Co', 'CO.': 'Co', '(INDIA)': 'India', 'Company': 'Co', '(India)': 'India',
        'Corp.': 'Corporation', 'CONSUMER ELEC': 'Consumer Electricals', 'DR REDDYS': "Dr. Reddy's", 'COMPANY': 'Co',
        'Cons Electrical': 'Consumer Electricals', 'ESCORTS': 'Escorts Kubota',
        'D B Corp': 'D.B.Corp', 'Healthcare India': 'Healthcare',
        'Data Patterns India': 'Data Patterns (India)', 'Infra projects': 'Infraprojects',
        'Deepak Fertilizers& Petrochemicals Corporation': 'Deepak Fertilizers & Petrochemicals Corporation',
        'GE T&D': 'GE Vernova T&D', 'TandD': 'Vernova T&D', 'EID Parry India': 'EID Parry (India)',
        'GENERAL INSURANC': 'General Insurance Co', 'LIFE INSURA L': 'Life Insurance Co',
        'INFO-EDGE': 'Info Edge', 'Anand Rathi': 'AnandRathi', 'EID Parry (India)': 'EID Parry India',
        'Inter Globe': 'InterGlobe', 'C.E.Info': 'C.E. Info', 'Five-Star Business Finance': 'Five Star Business Finance',
        'J B Chemicals & Pharma': 'JB Chemicals & Pharmaceuticals', 'Go Fashion India': 'Go Fashion (India)',
        'MAHINDRA FINANCIAL': 'Mahindra Financial Services', 'Grindwell Norton Limted': 'Grindwell Norton',
        'India L': 'India', 'K.P.R. Mill': 'KPR Mill', 'HeidelbergCement': 'Heidelberg Cement',
        'P I INDUSTRIES': 'PI Industries', 'CANFIN': 'Can Fin', 'INTL': 'International',
        'INDIA.': 'India', 'India*': 'India', 'Gujarat State Fert & Chemicals': 'Gujarat State Fertilizers and Chemicals',
        'PHARMACEUTICALS INDUSTRIES': 'Pharmaceutical Industries', 'Ctrl Sys Ind': 'Control Systems India',
        'Century Textile & Industries': 'Century Textiles & Industries',
        'INDIA SHELTER FIN CORP': 'India Shelter Finance Corporation', 'Ingersoll - Rand (India)': 'Ingersoll Rand (India)',
        'ION Exchange India': 'Ion Exchange (India)', 'JOHNSON CONTROLS - HITACHI AIR': 'Johnson Controls - Hitachi Air Conditioning India',
        'Mold Tek Packaging': 'Mold-Tek Packaging', 'Mrs Bectors Food Specialities': 'Mrs. Bectors Food Specialities',
        'NATIONAL THERMAL POWER CORPORATION': 'NTPC', 'INDUSRIES': 'Industries', 'REDINGTON (INDIA)': 'Redington', 'The Federal Bank': 'Federal Bank',
        'AMARA RAJA ENERGY MOB': 'Amara Raja Energy & Mobility', 'Carraro India Private': 'Carraro India', '(I)': 'India',
        'Computer Age Management Services': 'Computer Age Management Serv', 'E.I.D-Parry': 'EID Parry',
        'Fine Organic Industries': 'Fine Organic Ind', 'Green Panel Industries': 'Greenpanel Industries',
        'Hawkins Cookers': 'Hawkins Cooker', 'Heidleberg': 'Heidelberg', 'Housing & Urban Devlopment Company': 'Housing & Urban Development Corp',
        'Housing & Urban Development Corporation': 'Housing & Urban Development Corp', 'International Gemmological Inst Ind': 'International Gemmological Institute India Pvt',
        'J.B.': 'JB', 'J.K.CEMENT': 'JK Cement', 'Johnson Controls-Hitachi AC India': 'Johnson Controls - Hitachi Air Conditioning India',
        'JUPITER LIFELINE HOSPITALS': 'Jupiter Life Line Hospitals', 'Jyothy Laboratories': 'Jyothy Labs',
        'Krishna Inst of Medical Sciences': 'Krishna Institute of Medical', 'KRISHNA INSTITUTE OF MEDI SCIE': 'Krishna Institute of Medical', 'Krishna Institute of Medical Sciences': 'Krishna Institute of Medical',
        'LA-OPALA RG': 'La Opala RG', 'Mrs.': 'Mrs', 'EXCHANGE OF IN': 'Exchange of India',
        'NAVIN FLUORINE INTERNATIONAL L': 'Navin Fluorine International', 'Net web Technologies India': 'Netweb Technologies India',
        'NIPPON LIFE INDIA  ASSET MANAGEMENT': 'Nippon Life India Asset Management', 'Onesource Specialty Pharma': 'One Source Specialty Pharma',
        'ORIENT ELECTIC': 'Orient Electric', 'PVR INOX': 'PVR', "Rainbow Children's Medicare": 'Rainbow Childrens Medicare', 'Rate Gain Travel Technologies': 'Rategain Travel Technologies', 'S.J.S.': 'S J S', 'SJS Enterprises Pvt': 'S J S', 
        'S. P.': 'S P', '(KALAMANDIR)': 'Kalamandir', 'Shopper Stop': 'Shoppers Stop', 'CARBORANDUM UNIVERSAL': 'Carborundum Universal', 'The Great': 'Great',
        'Techno Electric & Engineering Company': 'Techno Electric & Engineering', 'Techno Electric & Engineering Co': 'Techno Electric & Engineering',
        'The Indian Hotels Co': 'Indian Hotels Co', 'The Karnataka Bank': 'Karnataka Bank', 'Thomas Cook  India': 'Thomas Cook India',
        'THOMAS COOK [I]': 'Thomas Cook India', 'T. V.': 'TV', 'VIJAYA DIAGNOSTIC CENTRE PVT': 'Vijaya Diagnostic Centre', 'Moil Limtied': 'MOIL'

    }

    def clean_stock_name(stock_name):
        """Cleans and standardizes the stock name."""
        if isinstance(stock_name, str):
            stock_name = re.sub(r"^EQ\s*-*\s*", "", stock_name)
            stock_name = re.sub(r"^EQ \(RTS-PP\)", "", stock_name)
            stock_name = re.sub(r"[^A-Za-z0-9]", "", stock_name)
            stock_name = keyword_pattern.sub("", stock_name).strip()
            for old, new in replacements.items():
                stock_name = stock_name.replace(old, new)
        return stock_name

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
            if col3 is None:
                continue
            change = col3 if col5 is None else col3 - col5

            stock_name = clean_stock_name(stock_name)

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
    
    ticker_df = pd.read_csv("nse_ticker.csv")
    ticker_df["CompanyName_clean"] = ticker_df["CompanyName"].apply(clean_stock_name)
    ticker_lookup = dict(
        zip(ticker_df["CompanyName_clean"], ticker_df["Symbol"])
    )
    ticker_names = list(ticker_lookup.keys())

    output_df["NSE_Ticker"] = output_df["Stocks"].map(ticker_lookup)
    output_df_change["NSE_Ticker"] = output_df_change["Stocks"].map(ticker_lookup)

    def get_best_ticker(stock_name, threshold=85):
        match, score, _ = process.extractOne(
            stock_name,
            ticker_names,
            scorer=fuzz.token_sort_ratio
        )
        return match, score, ticker_lookup.get(match) if score >= threshold else None
    
    mask = output_df["NSE_Ticker"].isna()
    output_df.loc[mask, "NSE_Ticker"] = (
        output_df.loc[mask, "Stocks"]
            .apply(get_best_ticker)
    )

    matched = output_df["NSE_Ticker"].notna().sum()
    total = len(output_df)
    print(f"Matched {matched}/{total} holdings")

    # Save output files
    output_df.to_csv(os.path.join(folder_path, '01-25_Small.csv'), index=False)
    print(f"Output saved to 01-25_Small.csv")

    output_df_change.to_csv(os.path.join(folder_path, '01-25_Small_change.csv'), index=False)
    print(f"Output saved to 01-25_Small_change.csv")

# Example usage
process_csv_files("Holdings_Dec_25\screener.csv")
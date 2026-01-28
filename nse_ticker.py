import pandas as pd
import yfinance as yf
from nsepython import nse_eq_symbols
from concurrent.futures import ProcessPoolExecutor
import multiprocessing


def fetch_company_name(sym: str) -> dict:
    """
    Fetch company name for a single NSE symbol using yfinance.
    Must be top-level for ProcessPoolExecutor.
    """
    try:
        ticker = yf.Ticker(f"{sym}.NS")
        info = ticker.info
        company_name = info.get("longName") or info.get("shortName") or ""
        print(f"Fetched {sym}")
        return {"Symbol": sym, "CompanyName": company_name}
    except Exception:
        print(f"Error fetching {sym}")
        return {"Symbol": sym, "CompanyName": ""}


def main():
    # Get all NSE symbols
    symbols = nse_eq_symbols()

    # Keep only symbols starting from N to Z
    symbols = [
        s for s in symbols
        if s and s[0].upper() >= "N"
    ]
    
    print(f"Filtered symbols count: {len(symbols)}")

    # Use available CPU cores (cap if you want to be gentle on Yahoo)
    workers = min(8, multiprocessing.cpu_count())

    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(fetch_company_name, symbols))

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Save to CSV
    df.to_csv("nse_company_names.csv", index=False)

    print("CSV saved as nse_company_names.csv")


if __name__ == "__main__":
    main()

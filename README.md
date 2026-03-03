# 📈 Portfolio Optimisation

A production-ready NSE (National Stock Exchange of India) portfolio optimisation engine built on **Modern Portfolio Theory (MPT)**. It scrapes live market data, curates historical price series, and computes the mathematically optimal asset allocation by maximising the **Sharpe ratio** on the efficient frontier.

---

## 🧠 How It Works

```
ticker.py        →   curator.py      →   scraper.py    →    optimiser.py
  Fetch valid              Clean & curate           Scrape live daily        Compute optimal weights
  NSE ticker list          OHLCV data               price updates            via Markowitz MPT
```

1. **`ticker.py`** – Retrieves and validates the full list of NSE-listed stock tickers.
2. **`curator.py`** – Cleans raw price data, handles missing values, aligns time series, and computes log returns.
3. **`scraper.py`** – Automates daily OHLCV data ingestion for a user-defined watchlist.
4. **`optimiser.py`** – Runs mean-variance optimisation (Markowitz) to find the maximum Sharpe ratio portfolio on the efficient frontier.

---

## 🚀 Quickstart

### 1. Clone the repo

```bash
git clone https://github.com/pradeepsk-23/Portfolio-Optimisation.git
cd Portfolio-Optimisation
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Curate your data

```bash
python data_curator.py
```

### 4. Run the optimiser

```bash
python Portfolio_Optimiser.py
```

---

## 📦 Project Structure

```
Portfolio-Optimisation/
├── portfolio/
|    ├── optimiser.py       # Core MPT optimisation engine
|    ├── scraper.py         # Live NSE data scraper
|    ├── curator.py         # Data cleaning & preprocessing
|    ├── ticker.py          # NSE ticker utilities
├── tests/
│   ├── __init__.py
│   ├── test_optimiser.py    # Unit tests for optimiser
│   └── test_curator.py      # Unit tests for data curator
├── data/
│   └── .gitkeep             # Placeholder for local data files
├── .github/
│   └── workflows/
│       └── ci.yml           # GitHub Actions CI pipeline
├── requirements.txt

```

---

## 📊 Sample Output

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Optimal Portfolio (Max Sharpe Ratio)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Ticker       Weight
 ──────────────────
 RELIANCE     23.4%
 INFY         18.7%
 TCS          17.2%
 HDFCBANK     15.9%
 WIPRO        14.3%
 ICICIBANK    10.5%

 Expected Annual Return :  18.6%
 Annual Volatility      :  12.3%
 Sharpe Ratio           :  1.51
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 🔬 Methodology

| Concept | Details |
|---|---|
| **Returns** | Log returns computed from adjusted closing prices |
| **Covariance** | Sample covariance matrix of historical log returns |
| **Objective** | Maximise Sharpe ratio (return per unit of risk) |
| **Solver** | `scipy.optimize.minimize` with SLSQP method |
| **Constraints** | Weights sum to 1; long-only (no short selling) |
| **Risk-free rate** | 6.5% (approximate Indian 10Y G-Sec yield) |
| **Data source** | NSE via `nsepy` / `yfinance` |

---
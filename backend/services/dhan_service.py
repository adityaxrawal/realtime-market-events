# backend/services/dhan_service.py
# Service layer for interacting with the DhanHQ API and data sources.

import logging
import requests
import pandas as pd
import io
from typing import List, Dict, Any, Optional
from datetime import date

from core.config import settings
from models.stock import StockListItem, HistoricalDataPoint

logger = logging.getLogger(__name__)

# --- Constants ---
DHAN_INSTRUMENT_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv" # Compact CSV URL
DHAN_HISTORICAL_API_URL = "https://api.dhan.co/v2/charts/historical"
DHAN_INTRADAY_API_URL = "https://api.dhan.co/v2/charts/intraday"

# Cache for instrument data to avoid repeated downloads/parsing
instrument_data_cache: Optional[pd.DataFrame] = None
nifty50_list_cache: Optional[List[StockListItem]] = None

# --- Helper Functions ---

def get_dhan_headers() -> Dict[str, str]:
    """Returns standard headers required for Dhan REST API calls."""
    if not settings.DHAN_ACCESS_TOKEN or settings.DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN_HERE":
        raise ValueError("DHAN_ACCESS_TOKEN is not configured.")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": settings.DHAN_ACCESS_TOKEN
    }

async def load_instrument_data() -> pd.DataFrame:
    """Loads the Dhan instrument master CSV into a pandas DataFrame."""
    global instrument_data_cache
    if instrument_data_cache is not None:
        return instrument_data_cache

    logger.info(f"Downloading instrument master CSV from {DHAN_INSTRUMENT_CSV_URL}")
    try:
        response = requests.get(DHAN_INSTRUMENT_CSV_URL, timeout=30)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # Use StringIO to read the CSV content directly into pandas
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data)

        # Basic validation (check for essential columns)
        required_cols = ['SEM_TRADING_SYMBOL', 'SEM_CUSTOM_SYMBOL', 'SECURITY_ID', 'SEM_EXCH_NAME', 'SEM_SEGMENT', 'SEM_INSTRUMENT_NAME']
        if not all(col in df.columns for col in required_cols):
             raise ValueError(f"Instrument CSV is missing one or more required columns: {required_cols}")

        # Convert SECURITY_ID to string for consistent key usage
        df['SECURITY_ID'] = df['SECURITY_ID'].astype(str)
        instrument_data_cache = df
        logger.info(f"Successfully loaded {len(df)} instruments from CSV.")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading instrument CSV: {e}", exc_info=True)
        raise ConnectionError(f"Failed to download instrument data: {e}") from e
    except Exception as e:
        logger.error(f"Error processing instrument CSV: {e}", exc_info=True)
        raise ValueError(f"Failed to process instrument data: {e}") from e


async def get_nifty50_constituent_list() -> List[StockListItem]:
    """
    Fetches the list of Nifty 50 constituents by filtering the instrument master data.
    Uses a simple cache.
    """
    global nifty50_list_cache
    if nifty50_list_cache is not None:
        return nifty50_list_cache

    logger.info("Fetching Nifty 50 constituent list...")
    try:
        df = await load_instrument_data()

        # Filter criteria (adjust based on actual CSV column values for Nifty 50)
        # This assumes 'SEM_INSTRUMENT_NAME' column contains 'NIFTY 50' for its constituents
        # and we only want NSE Equity segment. Verify this assumption with the actual CSV data.
        nifty50_df = df[
            (df['SEM_INSTRUMENT_NAME'] == 'NIFTY 50') & # Check if this column identifies constituents
            (df['SEM_SEGMENT'] == 'EQUITY') &
            (df['SEM_EXCH_NAME'] == 'NSE')
        ].copy() # Use .copy() to avoid SettingWithCopyWarning

        if nifty50_df.empty:
             logger.warning("No Nifty 50 constituents found using filter criteria on SEM_INSTRUMENT_NAME='NIFTY 50'. Check CSV data/filter logic.")
             # Fallback or alternative filtering might be needed.
             # Maybe filter by a known list of Nifty 50 symbols if the above fails?
             # known_n50_symbols = ["RELIANCE", "TCS", ...]
             # nifty50_df = df[df['SEM_TRADING_SYMBOL'].isin(known_n50_symbols) & (df['SEM_SEGMENT'] == 'EQUITY')]

        stock_list = [
            StockListItem(
                symbol=row['SEM_TRADING_SYMBOL'],
                name=row['SEM_CUSTOM_SYMBOL'], # Or another name column if available
                security_id=str(row['SECURITY_ID']),
                exchange=row['SEM_EXCH_NAME'],
                segment=row['SEM_SEGMENT'],
                instrument_type=row['SEM_INSTRUMENT_NAME'] # Or a more specific type column
            ) for index, row in nifty50_df.iterrows()
        ]
        nifty50_list_cache = stock_list
        logger.info(f"Found {len(stock_list)} potential Nifty 50 constituents.")
        return stock_list

    except Exception as e:
        logger.error(f"An error occurred while fetching/processing Nifty 50 list: {e}", exc_info=True)
        return [] # Return empty list on error

# --- Service Functions ---

async def get_historical_data(
    security_id: str,
    exchange_segment: str,
    instrument_type: str,
    from_date: date,
    to_date: date,
    interval: str
) -> List[HistoricalDataPoint]:
    """
    Fetches historical OHLCV data from Dhan REST API.
    """
    logger.info(f"Fetching historical data for {security_id} ({interval}) from {from_date} to {to_date}")

    try:
        headers = get_dhan_headers()
    except ValueError as e:
        logger.error(f"Cannot fetch historical data: {e}")
        return [] # Return empty on auth error

    payload = {
        "securityId": security_id,
        "exchangeSegment": exchange_segment,
        "instrument": instrument_type,
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
    }

    if interval == 'D':
        api_url = DHAN_HISTORICAL_API_URL
    else:
        # Assume interval is minutes ('1', '5', etc.)
        api_url = DHAN_INTRADAY_API_URL
        payload["interval"] = interval
        # Note: Intraday API might have different date range limits (e.g., 90 days)
        # Add logic here to handle requests spanning > 90 days if needed (make multiple calls)

    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        # --- Process Response ---
        # The API returns separate arrays for open, high, low, close, volume, timestamp
        if not data or 'open' not in data or not isinstance(data['open'], list):
            logger.warning(f"Unexpected historical data format received for {security_id}: {data}")
            return []

        num_candles = len(data['open'])
        if not all(len(data.get(key, [])) == num_candles for key in ['high', 'low', 'close', 'volume', 'timestamp']):
             logger.warning(f"Length mismatch in OHLCV arrays for {security_id}")
             # Attempt to process based on shortest array length? Or return empty?
             min_len = min(len(data.get(key, [])) for key in ['open', 'high', 'low', 'close', 'volume', 'timestamp'])
             if min_len == 0: return []
             num_candles = min_len
             # Slicing arrays - potential data loss if lengths mismatch
             data = {k: v[:num_candles] for k, v in data.items() if isinstance(v, list)}


        historical_points = [
            HistoricalDataPoint(
                timestamp=int(data['timestamp'][i]), # Ensure timestamp is int
                open=float(data['open'][i]),
                high=float(data['high'][i]),
                low=float(data['low'][i]),
                close=float(data['close'][i]),
                volume=int(data['volume'][i]) # Ensure volume is int
            ) for i in range(num_candles)
        ]
        logger.info(f"Successfully processed {len(historical_points)} historical data points for {security_id}")
        return historical_points

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical data for {security_id} from {api_url}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching historical data for {security_id}: {e}", exc_info=True)
        # Log response body if available and not too large
        if e.response is not None:
             try:
                  logger.error(f"Response status: {e.response.status_code}, Body: {e.response.text[:500]}") # Log first 500 chars
             except Exception:
                  pass # Ignore errors during error logging
        return []
    except Exception as e:
        logger.error(f"Error processing historical data response for {security_id}: {e}", exc_info=True)
        return []


# backend/services/dhan_service.py
# Service layer for interacting with the DhanHQ API and local data.
# Added function to query analysis results from database.

import logging
import requests
import json
import os
import psycopg2 # Added for database connection
import psycopg2.extras # Added for DictCursor
from urllib.parse import urlparse # Added for parsing DATABASE_URL
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta

from core.config import settings
# Import models including the new AnalysisOutcome
from models.stock import StockListItem, HistoricalDataPoint, AnalysisOutcome

logger = logging.getLogger(__name__)

# --- Constants ---
DHAN_HISTORICAL_API_URL = "https://api.dhan.co/v2/charts/historical"
DHAN_INTRADAY_API_URL = "https://api.dhan.co/v2/charts/intraday"
LOCAL_NIFTY50_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nifty50_list.json")

# Cache for the processed Nifty 50 list
nifty50_list_cache: Optional[List[StockListItem]] = None

# --- Helper Functions ---

def get_dhan_headers() -> Dict[str, str]:
    """Returns standard headers required for Dhan REST API calls."""
    if not settings.DHAN_ACCESS_TOKEN or settings.DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN_HERE":
        raise ValueError("DHAN_ACCESS_TOKEN is not configured.")
    return {"Accept": "application/json", "Content-Type": "application/json", "access-token": settings.DHAN_ACCESS_TOKEN}

def load_local_nifty50_data() -> List[Dict[str, Any]]:
    """Loads the Nifty 50 stock list from the local JSON file."""
    logger.info(f"Loading local Nifty 50 list from: {LOCAL_NIFTY50_JSON_PATH}")
    try:
        with open(LOCAL_NIFTY50_JSON_PATH, 'r', encoding='utf-8') as f: data = json.load(f)
        if not isinstance(data, list): raise ValueError("Local Nifty 50 JSON is not a list.")
        logger.info(f"Loaded {len(data)} entries from local Nifty 50 JSON.")
        return data
    except FileNotFoundError:
        logger.error(f"Local Nifty 50 JSON file not found at: {LOCAL_NIFTY50_JSON_PATH}")
        raise FileNotFoundError("Nifty 50 data file is missing.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding local Nifty 50 JSON file: {e}")
        raise ValueError("Nifty 50 data file contains invalid JSON.")
    except Exception as e:
        logger.error(f"Error reading local Nifty 50 JSON file: {e}", exc_info=True)
        raise IOError("Could not read Nifty 50 data file.")

async def get_nifty50_constituent_list() -> List[StockListItem]:
    """
    Loads Nifty 50 list from local JSON and converts it to StockListItem models.
    Uses cache. Now includes Security ID directly from the JSON.
    """
    global nifty50_list_cache
    if nifty50_list_cache is not None:
        return nifty50_list_cache

    logger.info("Generating Nifty 50 constituent list from local JSON...")
    try:
        local_data = load_local_nifty50_data() # Load directly from JSON

        stock_list = []
        skipped_count = 0
        for item in local_data:
            symbol = item.get("Symbol")
            security_id_raw = item.get("SEM_SMST_SECURITY_ID") # Get the ID

            # Basic validation
            if not symbol or security_id_raw is None:
                logger.warning(f"Skipping item in local JSON due to missing 'Symbol' or 'SEM_SMST_SECURITY_ID': {item}")
                skipped_count += 1
                continue

            # Convert security ID to string
            security_id = str(security_id_raw)

            stock_list.append(StockListItem(
                symbol=symbol,
                name=item.get("Company Name"),
                industry=item.get("Industry"),
                isin_code=item.get("ISIN Code"),
                series=item.get("Series"),
                security_id=security_id,
                # Add default/assumed values for other fields if needed, or leave as None
                exchange="NSE", # Assuming all are NSE from context
                segment="EQUITY", # Assuming all are Equity
                instrument_type="EQUITY" # Assuming
            ))

        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} items from local JSON due to missing data.")

        if not stock_list:
             logger.error("Failed to create Nifty 50 list from local JSON. No valid entries found.")
             raise ValueError("Could not process any valid entries from local Nifty 50 data.")

        nifty50_list_cache = stock_list
        logger.info(f"Generated Nifty 50 list with {len(stock_list)} constituents from local JSON.")
        return stock_list

    except Exception as e:
        logger.error(f"An error occurred while generating Nifty 50 list from local JSON: {e}", exc_info=True)
        nifty50_list_cache = None # Clear cache on error
        return [] # Return empty list on error


async def get_historical_data(
    security_id: str,
    exchange_segment: str,
    instrument_type: str,
    from_date: date,
    to_date: date,
    interval: str
) -> List[HistoricalDataPoint]:
    """Fetches historical OHLCV data from Dhan REST API."""
    logger.info(f"Fetching historical data for SecID {security_id} ({interval}) from {from_date} to {to_date}")
    try: headers = get_dhan_headers()
    except ValueError as e: logger.error(f"Cannot fetch historical data: {e}"); return []

    # Map backend segment format (e.g., NSE_EQ) to Dhan format (e.g., NSE EQ)
    dhan_exchange_segment = exchange_segment.replace("_", " ") if exchange_segment else "NSE EQ"
    dhan_instrument = instrument_type or "EQUITY" # Default to EQUITY if not provided

    from_date_str = from_date.strftime("%Y-%m-%d"); to_date_str = to_date.strftime("%Y-%m-%d")

    payload = {
        "securityId": security_id,
        "exchangeSegment": dhan_exchange_segment,
        "instrument": dhan_instrument,
        "fromDate": from_date_str,
        "toDate": to_date_str
    }

    # Determine API URL and add interval if needed
    if interval == 'D':
        api_url = DHAN_HISTORICAL_API_URL
    elif interval.isdigit():
        api_url = DHAN_INTRADAY_API_URL
        payload["interval"] = interval
        # Apply 90-day limit for intraday data as per Dhan docs
        date_diff = to_date - from_date
        if date_diff > timedelta(days=90):
             logger.warning(f"Intraday request for {security_id} exceeds 90 days. Fetching only first 90 days from {from_date_str}.")
             payload["toDate"] = (from_date + timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Invalid interval provided: {interval}")

    logger.debug(f"Calling Dhan Historical/Intraday API: {api_url} with payload: {payload}")
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30) # Increased timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        # Validate response structure (adjust based on actual Dhan response)
        if not data or 'open' not in data or not isinstance(data['open'], list):
            logger.warning(f"Unexpected historical data format for {security_id}. Response: {str(data)[:500]}")
            return []

        required_keys = ['open', 'high', 'low', 'close', 'volume', 'start_Time'] # Use start_Time based on docs
        if not all(key in data and isinstance(data[key], list) for key in required_keys):
            logger.warning(f"Missing arrays in historical data for {security_id}. Keys: {list(data.keys())}")
            return []

        # Check for length consistency
        num_candles = len(data['open'])
        if not all(len(data[key]) == num_candles for key in required_keys):
            min_len = min(len(data.get(key, [])) for key in required_keys)
            logger.warning(f"OHLCVT array length mismatch for {security_id}. Processing {min_len} candles.")
            if min_len == 0: return []
            # Trim arrays to the minimum length found
            data = {k: v[:min_len] for k, v in data.items() if k in required_keys}
            num_candles = min_len

        # Convert to HistoricalDataPoint model
        historical_points = []
        for i in range(num_candles):
             try:
                  # Use 'start_Time' for timestamp based on Dhan docs
                  ts = int(data['start_Time'][i])
                  o = float(data['open'][i])
                  h = float(data['high'][i])
                  l = float(data['low'][i])
                  c = float(data['close'][i])
                  v = int(data['volume'][i])
                  historical_points.append(HistoricalDataPoint(timestamp=ts, open=o, high=h, low=l, close=c, volume=v))
             except (ValueError, TypeError, IndexError) as item_err:
                  logger.warning(f"Skipping invalid data point {i} for {security_id}: {item_err}. Data: { {k: data[k][i] for k in required_keys if i < len(data.get(k,[]))} }")
                  continue

        logger.info(f"Processed {len(historical_points)} historical points for {security_id}")
        return historical_points

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching historical data for {security_id} from {api_url}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical data for {security_id} from {api_url}: {e}", exc_info=False)
        if e.response is not None:
            logger.error(f"Response status: {e.response.status_code}, Body: {e.response.text[:500]}")
        return []
    except Exception as e:
        logger.error(f"Error processing historical data for {security_id}: {e}", exc_info=True)
        return []


# --- ADDED: Function to get latest analysis results ---
async def get_latest_analysis(stock_symbol: str) -> Optional[AnalysisOutcome]:
    """
    Fetches the latest analysis results from the TimescaleDB database
    for a given stock symbol.
    """
    logger.info(f"Fetching latest analysis for symbol: {stock_symbol}")
    conn = None
    cursor = None # Initialize cursor to None
    try:
        # Parse DATABASE_URL (basic example, consider more robust parsing)
        result = urlparse(settings.DATABASE_URL)
        username = result.username
        password = result.password
        database = result.path[1:] # Remove leading '/'
        hostname = result.hostname
        port = result.port

        conn = psycopg2.connect(
            dbname=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )
        # Use DictCursor to get results as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Query for the most recent entry for the given stock symbol
        # Assumes the table name is 'market_analysis_results' as per updated schema
        query = """
            SELECT * FROM market_analysis_results
            WHERE stock_symbol = %s
            ORDER BY time DESC
            LIMIT 1;
        """
        cursor.execute(query, (stock_symbol,))
        result_row = cursor.fetchone()

        if result_row:
            logger.info(f"Found analysis data for {stock_symbol}")
            # Convert row dictionary to AnalysisOutcome model
            # Pydantic v2 uses from_attributes=True in model Config
            analysis_data = AnalysisOutcome.model_validate(result_row) # Use model_validate for Pydantic v2
            return analysis_data
        else:
            logger.info(f"No analysis data found for {stock_symbol}")
            return None

    except psycopg2.Error as db_err:
        logger.error(f"Database error fetching analysis for {stock_symbol}: {db_err}")
        return None # Or raise a custom exception
    except Exception as e:
        logger.error(f"Unexpected error fetching analysis for {stock_symbol}: {e}", exc_info=True)
        return None # Or raise
    finally:
        # Ensure cursor and connection are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()


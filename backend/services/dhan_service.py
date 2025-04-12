# backend/services/dhan_service.py
# Service layer for interacting with the DhanHQ API and local data.

import logging
import requests
# import pandas as pd # No longer needed
# import io # No longer needed
import json # Import json
import os # Import os
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta

from core.config import settings
# Import StockListItem with updated fields if necessary
from models.stock import StockListItem, HistoricalDataPoint

logger = logging.getLogger(__name__)

# --- Constants ---
# DHAN_INSTRUMENT_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv" # Removed
DHAN_HISTORICAL_API_URL = "https://api.dhan.co/v2/charts/historical"
DHAN_INTRADAY_API_URL = "https://api.dhan.co/v2/charts/intraday"
# --- FIX: Correct path assuming 'data' folder is sibling to 'core', 'services' etc. ---
LOCAL_NIFTY50_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nifty50_list.json")

# Cache for the processed Nifty 50 list
nifty50_list_cache: Optional[List[StockListItem]] = None

# --- Helper Functions ---

def get_dhan_headers() -> Dict[str, str]:
    """Returns standard headers required for Dhan REST API calls."""
    if not settings.DHAN_ACCESS_TOKEN or settings.DHAN_ACCESS_TOKEN == "YOUR_DHAN_ACCESS_TOKEN_HERE":
        raise ValueError("DHAN_ACCESS_TOKEN is not configured.")
    return {"Accept": "application/json", "Content-Type": "application/json", "access-token": settings.DHAN_ACCESS_TOKEN}

# --- Removed load_instrument_data() function ---

def load_local_nifty50_data() -> List[Dict[str, Any]]:
    """Loads the Nifty 50 stock list from the local JSON file."""
    # (Function remains the same as before)
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
            # --- FIX: Use keys from the provided JSON data ---
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
    # (Function body remains the same as previous version)
    logger.info(f"Fetching historical data for SecID {security_id} ({interval}) from {from_date} to {to_date}")
    try: headers = get_dhan_headers()
    except ValueError as e: logger.error(f"Cannot fetch historical data: {e}"); return []
    dhan_exchange_segment = exchange_segment.replace("_", " ")
    dhan_instrument = instrument_type
    from_date_str = from_date.strftime("%Y-%m-%d"); to_date_str = to_date.strftime("%Y-%m-%d")
    payload = {"securityId": security_id, "exchangeSegment": dhan_exchange_segment, "instrument": dhan_instrument, "fromDate": from_date_str, "toDate": to_date_str}
    if interval == 'D': api_url = DHAN_HISTORICAL_API_URL
    elif interval.isdigit():
        api_url = DHAN_INTRADAY_API_URL; payload["interval"] = interval
        date_diff = to_date - from_date
        if date_diff > timedelta(days=90):
             logger.warning(f"Intraday request for {security_id} > 90 days. Fetching only first 90.")
             payload["toDate"] = (from_date + timedelta(days=90)).strftime("%Y-%m-%d")
    else: raise ValueError(f"Invalid interval: {interval}")
    logger.debug(f"Calling Dhan Historical API: {api_url} with payload: {payload}")
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status(); data = response.json()
        if not data or 'open' not in data or not isinstance(data['open'], list): logger.warning(f"Unexpected historical data format for {security_id}. Response: {str(data)[:500]}"); return []
        required_keys = ['open', 'high', 'low', 'close', 'volume', 'timestamp']
        if not all(key in data and isinstance(data[key], list) for key in required_keys): logger.warning(f"Missing arrays in historical data for {security_id}. Keys: {list(data.keys())}"); return []
        try:
             num_candles = len(data['open'])
             if not all(len(data[key]) == num_candles for key in required_keys):
                  min_len = min(len(data[key]) for key in required_keys); logger.warning(f"OHLCVT array length mismatch for {security_id}. Processing {min_len} candles.")
                  if min_len == 0: return []
                  num_candles = min_len; data = {k: v[:num_candles] for k, v in data.items() if k in required_keys}
        except Exception as len_err: logger.error(f"Error checking array lengths for {security_id}: {len_err}"); return []
        historical_points = []
        for i in range(num_candles):
             try:
                  ts=int(data['timestamp'][i]);o=float(data['open'][i]);h=float(data['high'][i]);l=float(data['low'][i]);c=float(data['close'][i]);v=int(data['volume'][i])
                  historical_points.append(HistoricalDataPoint(timestamp=ts, open=o, high=h, low=l, close=c, volume=v))
             except (ValueError, TypeError, IndexError) as item_err: logger.warning(f"Skipping invalid data point {i} for {security_id}: {item_err}."); continue
        logger.info(f"Processed {len(historical_points)} historical points for {security_id}"); return historical_points
    except requests.exceptions.Timeout: logger.error(f"Timeout fetching historical data for {security_id}"); return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching historical data for {security_id}: {e}", exc_info=False)
        if e.response is not None: logger.error(f"Response status: {e.response.status_code}, Body: {e.response.text[:500]}")
        return []
    except Exception as e: logger.error(f"Error processing historical data for {security_id}: {e}", exc_info=True); return []


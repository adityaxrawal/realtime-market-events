# backend/routes/stocks.py
# API routes for stock-related operations.

import logging
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List
import datetime

# Import models and services
from models.stock import StockListItem, Nifty50ListResponse, HistoricalDataResponse, HistoricalDataRequestParams
from services import dhan_service # Import the service module
from core.config import settings # Import settings if needed

logger = logging.getLogger(__name__)

# Create an API router
router = APIRouter()

@router.get(
    "/nifty50-list",
    response_model=Nifty50ListResponse,
    summary="Get Nifty 50 Constituents",
    description="Fetches the list of stocks currently part of the Nifty 50 index using the instrument master data."
)
async def get_nifty50_stocks():
    """Endpoint to retrieve the list of Nifty 50 stocks."""
    logger.info("Received request for /nifty50-list")
    try:
        stock_list: List[StockListItem] = await dhan_service.get_nifty50_constituent_list()
        if not stock_list:
             logger.warning("Nifty 50 list is empty. Check service logic or instrument data source.")
             # Return empty list, frontend should handle this state
        logger.info(f"Returning {len(stock_list)} Nifty 50 stocks.")
        return Nifty50ListResponse(stocks=stock_list)

    except Exception as e:
        logger.error(f"Error retrieving Nifty 50 list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching Nifty 50 list.")


@router.get(
    "/historical/{security_id}",
    response_model=HistoricalDataResponse,
    summary="Get Historical OHLCV Data",
    description="Fetches historical candle data (OHLCV) for a given security ID."
)
async def get_stock_historical_data(
    security_id: str = Path(..., description="Dhan Security ID of the instrument"),
    # Use Depends for query parameters to group them using Pydantic model
    params: HistoricalDataRequestParams = Depends()
):
    """
    Endpoint to retrieve historical OHLCV data.
    Use query parameters like:
    ?exchange_segment=NSE_EQ&instrument_type=EQUITY&from_date=2024-01-01&to_date=2024-04-10&interval=D
    ?exchange_segment=NSE_EQ&instrument_type=EQUITY&from_date=2024-04-10&to_date=2024-04-11&interval=5
    """
    logger.info(f"Received request for historical data: security_id={security_id}, params={params}")
    try:
        data_points = await dhan_service.get_historical_data(
            security_id=security_id,
            exchange_segment=params.exchange_segment,
            instrument_type=params.instrument_type,
            from_date=params.from_date,
            to_date=params.to_date,
            interval=params.interval
        )
        if not data_points:
            # Decide if empty data is a 404 or just an empty list response
            logger.warning(f"No historical data found for security_id={security_id} with given parameters.")
            # Returning empty list in the response model structure
            # raise HTTPException(status_code=404, detail="No historical data found for the given parameters.")

        # Attempt to get symbol from cache for response (optional nice-to-have)
        symbol = f"ID_{security_id}" # Default if symbol not found
        if dhan_service.instrument_data_cache is not None:
             try:
                  # Find symbol in the cached DataFrame
                  cached_symbol = dhan_service.instrument_data_cache.loc[
                       dhan_service.instrument_data_cache['SECURITY_ID'] == security_id,
                       'SEM_TRADING_SYMBOL'
                  ].iloc[0]
                  if cached_symbol: symbol = cached_symbol
             except (IndexError, KeyError):
                  logger.warning(f"Could not find symbol for security_id {security_id} in cache.")


        return HistoricalDataResponse(
            symbol=symbol,
            security_id=security_id,
            data=data_points
        )

    except ValueError as ve: # Catch specific errors like missing API key
         logger.error(f"Value error fetching historical data for {security_id}: {ve}")
         raise HTTPException(status_code=400, detail=str(ve))
    except ConnectionError as ce:
         logger.error(f"Connection error fetching historical data for {security_id}: {ce}")
         raise HTTPException(status_code=503, detail="Could not connect to data source.")
    except Exception as e:
        logger.error(f"Error retrieving historical data for {security_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching historical data.")

# Add other stock-related routes here later (e.g., snapshot quote using REST)


# backend/routes/stocks.py
# API routes for stock-related operations.
# Added /analysis/{stock_symbol} endpoint.

import logging
from fastapi import APIRouter, HTTPException, Depends, Query, Path
from typing import List, Optional # Added Optional
import datetime

# Added AnalysisResponse model and datetime
from models.stock import (
    StockListItem, Nifty50ListResponse, HistoricalDataResponse,
    HistoricalDataRequestParams, AnalysisResponse, AnalysisOutcome
)
from services import dhan_service
from core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get(
    "/nifty50-list",
    response_model=Nifty50ListResponse,
    summary="Get Nifty 50 Constituents",
    description="Fetches the list of Nifty 50 stocks from the local data file, enriched with Security IDs."
)
async def get_nifty50_stocks():
    """Endpoint to retrieve the list of Nifty 50 stocks."""
    logger.info("Received request for /nifty50-list")
    try:
        stock_list: List[StockListItem] = await dhan_service.get_nifty50_constituent_list()
        if not stock_list:
             logger.warning("Nifty 50 list is empty. Check local data file and mapping logic.")
        logger.info(f"Returning {len(stock_list)} Nifty 50 stocks.")
        return Nifty50ListResponse(stocks=stock_list)
    except (FileNotFoundError, ValueError, IOError) as data_err: # Removed ConnectionError as it's less likely here
         logger.error(f"Error loading Nifty 50 data: {data_err}")
         raise HTTPException(status_code=500, detail=f"Error loading Nifty 50 data: {data_err}")
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
    security_id: str = Path(..., description="Dhan Security ID of the instrument", examples=["11536"]),
    params: HistoricalDataRequestParams = Depends()
):
    """Endpoint to retrieve historical OHLCV data."""
    logger.info(f"Received request for historical data: security_id={security_id}, params={params}")
    try:
        data_points = await dhan_service.get_historical_data(
            security_id=security_id, exchange_segment=params.exchange_segment,
            instrument_type=params.instrument_type, from_date=params.from_date,
            to_date=params.to_date, interval=params.interval
        )

        # --- Get symbol from the cached nifty50 list ---
        symbol = f"ID_{security_id}" # Default
        try:
             n50_list = await dhan_service.get_nifty50_constituent_list() # Get cached list
             found_stock = next((s for s in n50_list if s.security_id == security_id), None)
             if found_stock:
                  symbol = found_stock.symbol
             else:
                  logger.warning(f"Security ID {security_id} not found in cached Nifty 50 list for symbol lookup.")
        except Exception as lookup_err:
             logger.warning(f"Could not retrieve symbol for security_id {security_id} from Nifty 50 list cache: {lookup_err}")

        return HistoricalDataResponse(symbol=symbol, security_id=security_id, data=data_points)

    except ValueError as ve:
        logger.error(f"Value error fetching historical data for {security_id}: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except requests.exceptions.RequestException as req_err: # More specific error for requests
        logger.error(f"API request error fetching historical data for {security_id}: {req_err}")
        raise HTTPException(status_code=503, detail="Could not connect to external data source.")
    except Exception as e:
        logger.error(f"Error retrieving historical data for {security_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error.")


# --- ADDED: Endpoint for Analysis Results ---
@router.get(
    "/analysis/{stock_symbol}",
    response_model=AnalysisResponse, # Use the new response model
    summary="Get Latest Stock Analysis",
    description="Fetches the latest calculated analysis outcomes (sentiment, volatility, etc.) for a given stock symbol from the database."
)
async def get_stock_analysis(
    stock_symbol: str = Path(..., description="Stock symbol (e.g., RELIANCE)", examples=["RELIANCE"])
):
    """
    Endpoint to retrieve the latest analysis results stored in the database
    by the Flink processing pipeline.
    """
    logger.info(f"Received request for analysis data: symbol={stock_symbol}")
    try:
        # Call the service function to query the database
        analysis_result: Optional[AnalysisOutcome] = await dhan_service.get_latest_analysis(stock_symbol.upper()) # Ensure uppercase symbol

        if analysis_result:
            # Return the found analysis data
            return AnalysisResponse(analysis=analysis_result, status="found")
        else:
            # Return not found if no data exists yet for the symbol
            return AnalysisResponse(analysis=None, status="not_found", message="No analysis data available yet for this symbol.")

    except Exception as e:
        # Log any unexpected errors during database query or processing
        logger.error(f"Error retrieving analysis for {stock_symbol}: {e}", exc_info=True)
        # Return a generic 500 error to the client
        raise HTTPException(status_code=500, detail="Internal server error fetching analysis data.")

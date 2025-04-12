# backend/models/stock.py
# Pydantic models for stock-related data structures

from pydantic import BaseModel, Field, validator, field_validator
from typing import List, Optional, Dict, Any
import datetime

# === API Request/Response Models ===

class StockListItem(BaseModel):
    """Represents a single stock item in a list (e.g., from instrument master)."""
    symbol: str = Field(..., description="Trading symbol (e.g., 'RELIANCE')")
    name: str = Field(..., description="Company name")
    security_id: Optional[str] = Field(None, description="Unique Security ID from the broker")
    exchange: Optional[str] = Field(None, description="Exchange code (e.g., 'NSE')")
    segment: Optional[str] = Field(None, description="Segment code (e.g., 'EQUITY')")
    instrument_type: Optional[str] = Field(None, description="Instrument type (e.g., 'EQUITY')") # Or SEM_INSTRUMENT_NAME?

    class Config:
        from_attributes = True

class Nifty50ListResponse(BaseModel):
    """Response model for the Nifty 50 stock list endpoint."""
    stocks: List[StockListItem]

class HistoricalDataRequestParams(BaseModel):
    """Query parameters model for the historical data endpoint."""
    exchange_segment: str = Field(..., description="Exchange Segment (e.g., NSE_EQ, NSE_FNO)", examples=["NSE_EQ"])
    instrument_type: str = Field(..., description="Instrument Type (e.g., EQUITY, INDEX, FUTURE)", examples=["EQUITY"])
    from_date: datetime.date = Field(..., description="Start date (YYYY-MM-DD)")
    to_date: datetime.date = Field(..., description="End date (YYYY-MM-DD)")
    interval: str = Field(..., description="Candle interval ('D' for daily, '1', '5', '60' etc. for minutes)", examples=["D", "5"])

    @field_validator('interval')
    @classmethod
    def check_interval(cls, v: str):
        valid_intervals = ['D', '1', '5', '15', '25', '60'] # Add others if needed
        if v not in valid_intervals:
            raise ValueError(f"Invalid interval. Must be one of: {valid_intervals}")
        return v

class HistoricalDataPoint(BaseModel):
    """Represents a single OHLCV data point."""
    timestamp: int # Assuming epoch timestamp in seconds from Dhan API
    open: float
    high: float
    low: float
    close: float
    volume: int

class HistoricalDataResponse(BaseModel):
    """Response model for the historical data endpoint."""
    symbol: str
    security_id: str
    data: List[HistoricalDataPoint]

# === WebSocket Message Models ===

class WebSocketRequestParams(BaseModel):
    """Parameters for WebSocket requests from frontend."""
    security_id: str = Field(..., description="Dhan Security ID")
    exchange_segment: str = Field(..., description="e.g., NSE_EQ, NSE_FNO")

class WebSocketRequest(BaseModel):
    """Model for messages received FROM frontend clients via WebSocket."""
    action: str # e.g., 'subscribe_ohlc', 'unsubscribe_ohlc'
    params: WebSocketRequestParams # Nested parameters model

class WebSocketResponsePayload(BaseModel):
    """Base model for WebSocket response payloads."""
    pass # Specific payloads will inherit from this

class WebSocketErrorPayload(WebSocketResponsePayload):
    message: str

class WebSocketTickerPayload(WebSocketResponsePayload): # DEFINED HERE
    symbol: str
    ltp: float
    ltt: int

class WebSocketQuotePayload(WebSocketResponsePayload): # DEFINED HERE
    symbol: str
    ltp: float
    ltt: int
    volume: int
    open: float
    high: float
    low: float
    close: Optional[float] = None # Close might be null/0 intraday
    atp: float
    total_buy_qty: int
    total_sell_qty: int

class WebSocketSubscribedPayload(WebSocketResponsePayload): # DEFINED HERE
    security_id: str
    dataType: str

class WebSocketUnsubscribedPayload(WebSocketResponsePayload): # DEFINED HERE
    security_id: str
    dataType: str

class WebSocketResponse(BaseModel): # DEFINED HERE
    """Generic structure for messages sent TO frontend clients via WebSocket."""
    type: str # e.g., 'ticker_update', 'quote_update', 'error', 'subscribed', 'unsubscribed'
    payload: Dict[str, Any] # Allow flexible payload, or use Union[...] for specific types


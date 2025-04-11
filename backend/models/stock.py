# backend/models/stock.py
# Pydantic models for stock-related data structures

from pydantic import BaseModel, Field, validator
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
    instrument_type: Optional[str] = Field(None, description="Instrument type (e.g., 'EQUITY')")

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

class WebSocketRequest(BaseModel):
    """Model for messages received FROM frontend clients via WebSocket."""
    action: str # e.g., 'subscribe_ohlc', 'unsubscribe_ohlc'
    params: Dict[str, Any] # e.g., {"security_id": "1234", "exchange_segment": "NSE_EQ"}

class WebSocketResponse(BaseModel):
    """Generic structure for messages sent TO frontend clients via WebSocket."""
    type: str # e.g., 'nifty50_tick', 'selected_stock_ohlc', 'error'
    payload: Dict[str, Any] # The actual data payload


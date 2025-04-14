# backend/models/stock.py
# Pydantic models for stock-related data structures
# Added AnalysisOutcome and AnalysisResponse models.

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
import datetime

# === API Request/Response Models ===

class StockListItem(BaseModel):
    """Represents a single stock item in a list (e.g., from instrument master)."""
    symbol: str = Field(..., description="Trading symbol (e.g., 'RELIANCE')")
    name: Optional[str] = Field(None, description="Company name") # Made optional as it might not always be present
    security_id: Optional[str] = Field(None, description="Unique Security ID from the broker")
    exchange: Optional[str] = Field(None, description="Exchange code (e.g., 'NSE')")
    segment: Optional[str] = Field(None, description="Segment code (e.g., 'EQUITY')")
    instrument_type: Optional[str] = Field(None, description="Instrument type (e.g., 'EQUITY')")
    # Added fields from nifty50_list.json for completeness
    industry: Optional[str] = Field(None, description="Industry sector")
    isin_code: Optional[str] = Field(None, description="ISIN code")
    series: Optional[str] = Field(None, description="Trading series (e.g., 'EQ')")


    class Config:
        from_attributes = True # Enable ORM mode for Pydantic v2+

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
        # Allow numeric strings for minute intervals
        if v == 'D' or (v.isdigit() and int(v) > 0):
            return v
        raise ValueError(f"Invalid interval. Must be 'D' or a positive integer string (e.g., '1', '5').")

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

class WebSocketTickerPayload(WebSocketResponsePayload):
    symbol: str
    ltp: float
    ltt: int
    change: Optional[float] = None
    percent_change: Optional[float] = None

class WebSocketQuotePayload(WebSocketResponsePayload):
    symbol: str
    ltp: float
    ltt: int
    volume: int
    open: float
    high: float
    low: float
    close: Optional[float] = None # Close might be null/0 intraday
    atp: Optional[float] = None # Make optional if not always present
    total_buy_qty: Optional[int] = None # Make optional
    total_sell_qty: Optional[int] = None # Make optional
    prev_close: Optional[float] = None # Add previous close

class WebSocketSubscribedPayload(WebSocketResponsePayload):
    security_id: str
    dataType: str # e.g., TICKER, QUOTE, DEPTH

class WebSocketUnsubscribedPayload(WebSocketResponsePayload):
    security_id: str
    dataType: str

# Optional: Add if needed for depth data broadcasting (currently not broadcasted directly)
# class WebSocketDepthLevel(BaseModel):
#     price: float
#     quantity: int
#     orders: int
#
# class WebSocketDepthPayload(WebSocketResponsePayload):
#     symbol: str
#     timestamp: int
#     bids: List[WebSocketDepthLevel]
#     asks: List[WebSocketDepthLevel]

class WebSocketResponse(BaseModel):
    """Generic structure for messages sent TO frontend clients via WebSocket."""
    type: str # e.g., 'ticker_update', 'quote_update', 'error', 'subscribed', 'unsubscribed'
    payload: Dict[str, Any] # Allow flexible payload, or use Union[...] for specific types


# === ADDED: Models for Analysis Endpoint ===

class AnalysisOutcome(BaseModel):
    """Represents the combined analysis results for a stock."""
    time: Optional[datetime.datetime] = Field(None, description="Timestamp of the latest analysis calculation")
    stock_symbol: str = Field(..., description="The stock symbol")
    sentiment_label: Optional[str] = Field(None, description="Overall sentiment (e.g., Very Bearish, Bullish)")
    short_term_label: Optional[str] = Field(None, description="Short-term trend (e.g., Bearish, Neutral)")
    volatility_label: Optional[str] = Field(None, description="Volatility level (e.g., Low, High)")
    risk_label: Optional[str] = Field(None, description="Risk level (e.g., Low, High)")
    liquidity_crunch: Optional[bool] = Field(None, description="Flag indicating potential liquidity issues")
    mood_index_label: Optional[str] = Field(None, description="Overall mood index (e.g., Fear, Greed)")
    # Optional: Include underlying scores if calculated and stored
    sentiment_score: Optional[float] = Field(None, description="Underlying sentiment score")
    volatility_score: Optional[float] = Field(None, description="Underlying volatility score")
    risk_score: Optional[float] = Field(None, description="Underlying risk score (e.g., Beta)")
    mood_index_score: Optional[float] = Field(None, description="Underlying mood index score")

    class Config:
        from_attributes = True # Enable ORM mode for Pydantic v2+

class AnalysisResponse(BaseModel):
    """Response model for the /analysis/{symbol} endpoint."""
    analysis: Optional[AnalysisOutcome] = Field(None, description="The latest analysis outcome for the stock")
    status: str = Field(..., description="Status of the request ('found', 'not_found', 'error')")
    message: Optional[str] = Field(None, description="Optional message, e.g., explaining status")


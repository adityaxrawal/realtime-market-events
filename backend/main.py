# backend/main.py
# Basic FastAPI application structure as per Phase 0 roadmap.

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware # Import CORS middleware
import logging
from typing import List, Set

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Connection Manager for WebSockets ---
# (Basic structure as outlined in Phase 1, ready for Phase 2 expansion)
class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        # Use a set to store active WebSocket connections for efficient add/remove
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        """Accepts a new WebSocket connection and adds it to the active list."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"New WebSocket connection accepted: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        """Removes a WebSocket connection from the active list."""
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket connection closed: {websocket.client}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Sends a message to a specific WebSocket connection."""
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        """Sends a message to all active WebSocket connections."""
        # Iterate over a copy of the set in case connections change during broadcast
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                # Handle disconnection if sending fails
                self.disconnect(connection)
            except Exception as e:
                # Log other potential errors during broadcast
                logger.error(f"Error sending message to {connection.client}: {e}")

# Instantiate the connection manager globally
manager = ConnectionManager()

# --- FastAPI Application Instance ---
app = FastAPI(
    title="Financial Analytics Dashboard API",
    description="API for serving financial data, sentiment analysis, and real-time updates.",
    version="0.1.0",
)

# --- CORS (Cross-Origin Resource Sharing) Middleware ---
# Allow requests from your frontend development server (e.g., http://localhost:3000)
# Adjust origins as needed for your deployment environment.
origins = [
    "http://localhost",       # Allow requests from localhost (common for development)
    "http://localhost:3000",  # Default port for create-react-app
    "http://localhost:5173",  # Default port for Vite React app
    # Add any other origins if necessary (e.g., your deployed frontend URL)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,           # List of origins allowed to make requests
    allow_credentials=True,        # Allow cookies to be included in requests
    allow_methods=["*"],             # Allow all standard HTTP methods (GET, POST, etc.)
    allow_headers=["*"],             # Allow all headers
)


# --- API Endpoints ---

@app.get("/", tags=["Root"])
async def read_root():
    """Root endpoint providing a welcome message."""
    logger.info("Root endpoint '/' accessed.")
    return {"message": "Welcome to the Financial Analytics Dashboard API!"}

@app.get("/health", tags=["Health Check"])
async def health_check():
    """Health check endpoint to verify API status."""
    logger.info("Health check endpoint '/health' accessed.")
    return {"status": "ok"}

# Placeholder endpoint for Nifty 50 list (to be implemented in Phase 1)
@app.get("/api/nifty50-list", tags=["Stocks"])
async def get_nifty50_list():
    """
    Placeholder: Fetches the list of Nifty 50 stocks.
    (Implementation requires Dhan API integration in Phase 1)
    """
    logger.info("Placeholder endpoint '/api/nifty50-list' accessed.")
    # In Phase 1, this will call a service to fetch data from Dhan API
    return [{"symbol": "RELIANCE", "name": "Reliance Industries"}, {"symbol": "TCS", "name": "Tata Consultancy Services"}] # Example mock data

# Placeholder endpoints for sentiment and regime data (to be implemented in Phase 2/6)
@app.get("/api/sentiment/{stock_symbol}", tags=["Analytics"])
async def get_sentiment(stock_symbol: str):
    """
    Placeholder: Fetches sentiment data for a given stock symbol.
    (Implementation requires data pipeline integration in Phase 6)
    """
    logger.info(f"Placeholder endpoint '/api/sentiment/{stock_symbol}' accessed.")
    # In Phase 6, this will query TimescaleDB or consume from Kafka
    return {"stock_symbol": stock_symbol, "sentiment": "neutral", "score": 0.5, "status": "mock_data"}

@app.get("/api/regime/{stock_symbol}", tags=["Analytics"])
async def get_market_regime(stock_symbol: str):
    """
    Placeholder: Fetches market regime data for a given stock symbol.
    (Implementation requires data pipeline integration in Phase 6)
    """
    logger.info(f"Placeholder endpoint '/api/regime/{stock_symbol}' accessed.")
    # In Phase 6, this will query TimescaleDB or consume from Kafka
    return {"stock_symbol": stock_symbol, "regime_type": "Volatility", "regime_label": "Low Volatility", "status": "mock_data"}


# --- WebSocket Endpoint ---
# (Basic setup as per Phase 1, ready for Phase 2 expansion)
@app.websocket("/ws/stocks")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time stock data communication.
    (Broadcasting logic to be fully implemented in Phase 2)
    """
    await manager.connect(websocket)
    try:
        # Send a welcome message upon connection
        await manager.send_personal_message("Welcome to the Real-time Stock Feed!", websocket)
        # Keep the connection alive and listen for messages (if needed for client->server comms)
        while True:
            # You might receive messages from the client here if needed
            # data = await websocket.receive_text()
            # logger.info(f"Received message from {websocket.client}: {data}")
            # await manager.send_personal_message(f"You wrote: {data}", websocket)

            # This loop primarily keeps the connection open on the server side.
            # Broadcasting happens independently when new data arrives from Dhan (Phase 2).
            # For basic keep-alive, you might implement ping/pong or rely on underlying mechanisms.
            # Example: Send a ping periodically if needed (check WebSocket library features)
            # await asyncio.sleep(30) # Example delay
            # await websocket.ping()

            # For now, just wait indefinitely for disconnection or server shutdown
            await websocket.receive_text() # Waits for a message or disconnect

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"WebSocket disconnected: {websocket.client}")
    except Exception as e:
        # Log unexpected errors and ensure disconnection
        logger.error(f"WebSocket error for {websocket.client}: {e}")
        manager.disconnect(websocket)


# --- Running the Application (for local development) ---
# This block allows running the script directly using `python main.py`
# However, `uvicorn main:app --reload` is the recommended way for development.
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server directly from main.py")
    # Note: --reload is not effective when running programmatically like this.
    # Use the command line: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run(app, host="0.0.0.0", port=8000)


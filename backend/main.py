# backend/main.py
# Main FastAPI application entry point

import logging
import asyncio
import json # Import json for parsing client messages
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends # Import Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Set, Optional

# Import configuration, routers, and models
from core.config import settings
from routes import stocks as stock_routes
# Import the Dhan feed client runner AND the subscription update function
from core.dhan_feed import run_dhan_feed_client, update_subscriptions
from models.stock import WebSocketRequest # Import request model

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Connection Manager for WebSockets ---
class ConnectionManager:
    """Manages active WebSocket connections to frontend clients."""
    def __init__(self):
        # Store active connections mapped to their subscribed security IDs (for OHLC etc.)
        self.active_connections: Dict[WebSocket, Set[str]] = {} # WebSocket -> {security_id1, security_id2}
        logger.info("Frontend ConnectionManager initialized.")

    async def connect(self, websocket: WebSocket):
        """Accepts a new frontend WebSocket connection."""
        if len(self.active_connections) >= settings.WEBSOCKET_MAX_CONNECTIONS:
             logger.warning(f"Rejecting new frontend connection: Max connections ({settings.WEBSOCKET_MAX_CONNECTIONS}) reached.")
             await websocket.close(code=1008)
             return
        await websocket.accept()
        self.active_connections[websocket] = set() # Initialize empty set of subscriptions for this client
        logger.info(f"New frontend connection accepted: {websocket.client} (Total: {len(self.active_connections)})")

    async def disconnect(self, websocket: WebSocket):
        """Removes a frontend WebSocket connection and cleans up its subscriptions."""
        if websocket in self.active_connections:
            subscribed_ids = self.active_connections.pop(websocket)
            logger.info(f"Frontend connection closed: {websocket.client} (Total: {len(self.active_connections)})")
            # Trigger unsubscribe for instruments this client was watching (optional, depends on desired logic)
            # This prevents holding unnecessary subscriptions if no clients are watching
            # for sec_id in subscribed_ids:
            #     # Check if any OTHER client is still subscribed before unsubscribing from Dhan feed
            #     is_still_needed = any(sec_id in client_subs for client_subs in self.active_connections.values())
            #     if not is_still_needed:
            #         logger.info(f"Client disconnected, initiating unsubscribe for {sec_id} from Dhan feed.")
            #         # Assuming QUOTE/FULL was used for selected stock OHLC
            #         await update_subscriptions(sec_id, "NSE_EQ", "QUOTE", subscribe=False) # Need segment info too
        # else:
        #      logger.warning(f"Attempted to disconnect an unknown frontend websocket: {websocket.client}")


    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Sends a message to a specific frontend WebSocket connection."""
        if websocket not in self.active_connections:
             # logger.warning(f"Attempted to send personal message to disconnected client: {websocket.client}")
             return # Silently ignore if client disconnected
        try:
            await websocket.send_text(message)
        except WebSocketDisconnect:
            await self.disconnect(websocket) # Use await here
        except Exception as e:
            logger.error(f"Error sending personal message to {websocket.client}: {e}")
            await self.disconnect(websocket) # Use await here

    async def broadcast(self, message: str):
        """Sends a message to all active frontend WebSocket connections."""
        if not self.active_connections:
             return

        # Create list copy of websockets to iterate over
        connections_list = list(self.active_connections.keys())
        tasks = [self.send_personal_message(message, connection) for connection in connections_list]
        if tasks:
             await asyncio.gather(*tasks, return_exceptions=True) # Errors handled in send_personal_message

    async def add_client_subscription(self, websocket: WebSocket, security_id: str):
         """Adds a security ID to a client's subscription set."""
         if websocket in self.active_connections:
              self.active_connections[websocket].add(security_id)

    async def remove_client_subscription(self, websocket: WebSocket, security_id: str):
         """Removes a security ID from a client's subscription set."""
         if websocket in self.active_connections:
              self.active_connections[websocket].discard(security_id) # Use discard to avoid error if not present

# Instantiate the frontend connection manager globally
manager = ConnectionManager()

# --- Lifespan Management ---
# (Remains the same as previous version - starts/stops dhan_feed_task)
dhan_feed_task: Optional[asyncio.Task] = None
@asynccontextmanager
async def lifespan(app: FastAPI):
    global dhan_feed_task
    logger.info("Application startup...")
    logger.info(f"CORS Origins Allowed: {settings.BACKEND_CORS_ORIGINS}")
    if settings.DHAN_ACCESS_TOKEN and settings.DHAN_ACCESS_TOKEN != "YOUR_DHAN_ACCESS_TOKEN_HERE":
        logger.info("Starting Dhan WebSocket feed client background task...")
        dhan_feed_task = asyncio.create_task(run_dhan_feed_client())
    else:
        logger.warning("DHAN_ACCESS_TOKEN not set. Dhan WebSocket feed client will not start.")
    yield
    logger.info("Application shutdown...")
    if dhan_feed_task and not dhan_feed_task.done():
        logger.info("Cancelling Dhan WebSocket feed client task...")
        dhan_feed_task.cancel()
        try:
            await asyncio.wait_for(dhan_feed_task, timeout=5.0)
        except asyncio.CancelledError:
            logger.info("Dhan feed task successfully cancelled.")
        except asyncio.TimeoutError:
            logger.warning("Dhan feed task did not cancel within timeout.")
        except Exception as e:
             logger.error(f"Error during Dhan feed task cancellation: {e}")
    elif dhan_feed_task and dhan_feed_task.done(): logger.info("Dhan feed task was already done.")
    else: logger.info("No active Dhan feed task to cancel.")

# --- FastAPI Application Instance ---
app = FastAPI(
    title=settings.APP_NAME,
    description="API for serving financial data, sentiment analysis, and real-time updates.",
    version="0.1.0",
    lifespan=lifespan
)

# --- CORS Middleware ---
# (Remains the same as before)
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(CORSMiddleware, allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
else:
     logger.warning("No CORS origins configured. Allowing all origins.")
     app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- API Routers ---
# (Remains the same as before)
app.include_router(stock_routes.router, prefix=settings.API_V1_STR + "/stocks", tags=["Stocks"])

# --- Root and Health Check Endpoints ---
# (Remains the same as before)
@app.get("/", tags=["Root"])
async def read_root():
    return {"message": f"Welcome to the {settings.APP_NAME}!"}
@app.get("/health", tags=["Health Check"])
async def health_check():
    return {"status": "ok"}

# --- Frontend WebSocket Endpoint (Updated) ---
@app.websocket("/ws/stocks")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for frontend clients to receive updates and send requests."""
    await manager.connect(websocket)
    if websocket not in manager.active_connections:
         return # Connection rejected

    client_host = websocket.client.host if websocket.client else "Unknown"
    client_port = websocket.client.port if websocket.client else "N/A"
    client_info = f"{client_host}:{client_port}"

    try:
        await manager.send_personal_message(f"Welcome {client_info}! You are connected.", websocket)

        while True:
            # Wait for messages from the frontend client
            data = await websocket.receive_text()
            logger.info(f"Received message from {client_info}: {data}")

            try:
                # Parse the incoming JSON message
                request_data = json.loads(data)
                # Validate using Pydantic model
                client_request = WebSocketRequest(**request_data)

                # --- Handle Client Actions ---
                action = client_request.action
                params = client_request.params
                security_id = params.get("security_id")
                exchange_segment = params.get("exchange_segment", "NSE_EQ") # Default segment if needed

                if not security_id:
                     await manager.send_personal_message('{"type": "error", "payload": {"message": "Missing security_id in params"}}', websocket)
                     continue

                if action == "subscribe_ohlc":
                    # Subscribe to Quote data for the requested stock ID
                    logger.info(f"Client {client_info} requested OHLC subscription for {security_id}")
                    # Use "QUOTE" data type for OHLC updates (or "FULL" if depth needed too) - VERIFY WITH DHAN DOCS
                    await update_subscriptions(security_id, exchange_segment, "QUOTE", subscribe=True)
                    await manager.add_client_subscription(websocket, security_id) # Track client interest
                    await manager.send_personal_message(f'{{"type": "subscribed", "payload": {{"security_id": "{security_id}", "dataType": "QUOTE"}}}}', websocket)

                elif action == "unsubscribe_ohlc":
                    # Unsubscribe from Quote data
                    logger.info(f"Client {client_info} requested OHLC unsubscription for {security_id}")
                    await manager.remove_client_subscription(websocket, security_id) # Remove client interest
                    # Check if any other client needs this subscription before unsubscribing from Dhan
                    is_still_needed = any(security_id in client_subs for client_subs in manager.active_connections.values())
                    if not is_still_needed:
                         logger.info(f"No clients watching {security_id} QUOTE anymore, unsubscribing from Dhan feed.")
                         await update_subscriptions(security_id, exchange_segment, "QUOTE", subscribe=False)
                    else:
                         logger.info(f"Other clients still watching {security_id} QUOTE, keeping Dhan subscription active.")
                    await manager.send_personal_message(f'{{"type": "unsubscribed", "payload": {{"security_id": "{security_id}", "dataType": "QUOTE"}}}}', websocket)

                else:
                    logger.warning(f"Received unknown action from {client_info}: {action}")
                    await manager.send_personal_message(f'{{"type": "error", "payload": {{"message": "Unknown action: {action}"}}}}', websocket)

            except json.JSONDecodeError:
                logger.error(f"Received invalid JSON from {client_info}: {data}")
                await manager.send_personal_message('{"type": "error", "payload": {"message": "Invalid JSON format"}}', websocket)
            except Exception as e: # Catch Pydantic validation errors etc.
                logger.error(f"Error processing client request from {client_info}: {e}", exc_info=True)
                await manager.send_personal_message(f'{{"type": "error", "payload": {{"message": "Error processing request: {e}"}}}}', websocket)


    except WebSocketDisconnect:
        logger.info(f"Frontend client {client_info} disconnected.")
        # disconnect() handles cleanup
    except Exception as e:
        client_info = websocket.client if websocket else "Unknown Client"
        logger.error(f"WebSocket error for frontend client {client_info}: {e}", exc_info=True)
    finally:
         # Ensure disconnect is called if connection exists in manager
         if websocket in manager.active_connections:
              await manager.disconnect(websocket)


# --- Uvicorn Runner ---
# (Remains the same as before)
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server directly from main.py for debugging...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


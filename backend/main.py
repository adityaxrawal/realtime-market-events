# backend/main.py
# Main FastAPI application entry point

import logging
import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Set, Optional, Dict

# Import configuration, routers, and models
from core.config import settings
from routes import stocks as stock_routes
# Import the Dhan feed client runner AND the subscription update function
from core.dhan_feed import run_dhan_feed_client, update_subscriptions
# Import the ConnectionManager instance from its new file
from core.connection_manager import manager # Import the instance
from models.stock import WebSocketRequest, WebSocketResponse, WebSocketErrorPayload, WebSocketSubscribedPayload, WebSocketUnsubscribedPayload

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ConnectionManager class is now defined in core.connection_manager.py ---
# --- manager instance is imported from core.connection_manager.py ---

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
        try: await asyncio.wait_for(dhan_feed_task, timeout=5.0)
        except asyncio.CancelledError: logger.info("Dhan feed task successfully cancelled.")
        except asyncio.TimeoutError: logger.warning("Dhan feed task did not cancel within timeout.")
        except Exception as e: logger.error(f"Error during Dhan feed task cancellation: {e}")
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
async def read_root(): return {"message": f"Welcome to the {settings.APP_NAME}!"}
@app.get("/health", tags=["Health Check"])
async def health_check(): return {"status": "ok"}

# --- Frontend WebSocket Endpoint ---
# Uses the imported 'manager' instance
@app.websocket("/ws/stocks")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for frontend clients to receive updates and send requests."""
    await manager.connect(websocket) # Use imported manager
    if websocket not in manager.active_connections: return

    client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "Unknown"

    try:
        await manager.send_personal_message(f"Welcome {client_info}! You are connected.", websocket)

        while True:
            data = await websocket.receive_text()
            async def send_error_to_client(error_message: str):
                 err_payload = WebSocketErrorPayload(message=error_message)
                 err_response = WebSocketResponse(type="error", payload=err_payload.model_dump()).model_dump_json()
                 await manager.send_personal_message(err_response, websocket) # Use imported manager

            try:
                request_data = json.loads(data)
                client_request = WebSocketRequest(**request_data)

                action = client_request.action
                params = client_request.params
                security_id = params.security_id
                exchange_segment = params.exchange_segment
                target_data_type = "QUOTE" # Assuming OHLC actions use QUOTE feed

                if action == "subscribe_ohlc":
                    logger.info(f"Client {client_info} requested {target_data_type} subscription for {security_id} ({exchange_segment})")
                    success = await update_subscriptions(security_id, exchange_segment, target_data_type, subscribe=True)
                    if success:
                         await manager.add_client_subscription(websocket, security_id) # Use imported manager
                         resp_payload = WebSocketSubscribedPayload(security_id=security_id, dataType=target_data_type).model_dump()
                         resp = WebSocketResponse(type="subscribed", payload=resp_payload).model_dump_json()
                         await manager.send_personal_message(resp, websocket) # Use imported manager
                    else:
                         await send_error_to_client(f"Failed to subscribe to {security_id} ({target_data_type}) on feed.")

                elif action == "unsubscribe_ohlc":
                    logger.info(f"Client {client_info} requested {target_data_type} unsubscription for {security_id} ({exchange_segment})")
                    await manager.remove_client_subscription(websocket, security_id) # Use imported manager
                    is_still_needed = any(security_id in client_subs for client_subs in manager.active_connections.values())
                    if not is_still_needed:
                         logger.info(f"No clients watching {security_id} {target_data_type} anymore, requesting unsubscribe from Dhan feed.")
                         asyncio.create_task(update_subscriptions(security_id, exchange_segment, target_data_type, subscribe=False))
                    else:
                         logger.info(f"Other clients still watching {security_id} {target_data_type}, keeping Dhan subscription active.")
                    resp_payload = WebSocketUnsubscribedPayload(security_id=security_id, dataType=target_data_type).model_dump()
                    resp = WebSocketResponse(type="unsubscribed", payload=resp_payload).model_dump_json()
                    await manager.send_personal_message(resp, websocket) # Use imported manager

                else:
                    logger.warning(f"Received unknown action from {client_info}: {action}")
                    await send_error_to_client(f"Unknown action: {action}")

            except json.JSONDecodeError:
                logger.error(f"Received invalid JSON from {client_info}: {data}")
                await send_error_to_client("Invalid JSON format")
            except Exception as e:
                logger.error(f"Error processing client request from {client_info}: {e}", exc_info=True)
                await send_error_to_client(f"Error processing request: {str(e)}")

    except WebSocketDisconnect:
        logger.info(f"Frontend client {client_info} disconnected.")
    except Exception as e:
        client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "Unknown"
        logger.error(f"Unhandled WebSocket error for frontend client {client_info}: {e}", exc_info=True)
    finally:
         if websocket in manager.active_connections:
              await manager.disconnect(websocket) # Use imported manager


# --- Uvicorn Runner ---
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server directly from main.py for debugging...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


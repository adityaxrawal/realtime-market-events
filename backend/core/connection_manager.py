# backend/core/connection_manager.py
# Defines and instantiates the ConnectionManager for frontend WebSockets.

import logging
from typing import Set, Dict, Any
from fastapi import WebSocket, WebSocketDisconnect # Import WebSocket types here
import asyncio

# Import settings if needed for configuration within the manager
from core.config import settings

logger = logging.getLogger(__name__)

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
        """Removes a frontend WebSocket connection and potentially triggers unsubscriptions."""
        if websocket in self.active_connections:
            subscribed_ids = self.active_connections.pop(websocket)
            client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "Unknown"
            logger.info(f"Frontend connection closed: {client_info} (Total: {len(self.active_connections)})")

            # Trigger unsubscribe for instruments this client was watching (only if no other clients need it)
            # This requires importing the update_subscriptions function, creating another potential cycle.
            # A better approach might be for disconnect to just remove the client,
            # and have a separate periodic task check for orphaned Dhan subscriptions.
            # For now, we comment out the direct unsubscribe call from here to avoid new cycles.

            # from core.dhan_feed import update_subscriptions # <-- Avoid importing here if possible

            # default_segment = "NSE_EQ"
            # default_data_type = "QUOTE"
            # for sec_id in subscribed_ids:
            #     is_still_needed = any(sec_id in client_subs for client_subs in self.active_connections.values())
            #     if not is_still_needed:
            #         logger.info(f"Client disconnected, initiating unsubscribe for {sec_id} ({default_data_type}) from Dhan feed.")
            #         try:
            #              # asyncio.create_task(update_subscriptions(sec_id, default_segment, default_data_type, subscribe=False)) # Creates cycle
            #              pass # Handle orphan cleanup elsewhere
            #         except Exception as unsub_err:
            #              logger.error(f"Error creating unsubscribe task for {sec_id}: {unsub_err}")


    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Sends a message to a specific frontend WebSocket connection."""
        if websocket not in self.active_connections: return
        try:
            await websocket.send_text(message)
        except (WebSocketDisconnect, websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
            # Use await when calling async disconnect
            await self.disconnect(websocket)
        except Exception as e:
            logger.error(f"Error sending personal message to {websocket.client}: {e}")
            # Use await when calling async disconnect
            await self.disconnect(websocket)

    async def broadcast(self, message: str):
        """Sends a message to all active frontend WebSocket connections."""
        if not self.active_connections: return
        connections_list = list(self.active_connections.keys())
        # Exceptions are handled within send_personal_message
        await asyncio.gather(*[self.send_personal_message(message, connection) for connection in connections_list], return_exceptions=False)

    async def add_client_subscription(self, websocket: WebSocket, security_id: str):
         """Adds a security ID to a client's subscription set."""
         if websocket in self.active_connections:
              self.active_connections[websocket].add(security_id)

    async def remove_client_subscription(self, websocket: WebSocket, security_id: str):
         """Removes a security ID from a client's subscription set."""
         if websocket in self.active_connections:
              self.active_connections[websocket].discard(security_id)

# Instantiate the ConnectionManager globally *once* here
manager = ConnectionManager()

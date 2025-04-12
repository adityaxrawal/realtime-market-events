# backend/core/dhan_feed.py
# Handles connection to DhanHQ WebSocket Market Feed (v2)

import asyncio
import websockets
import struct
import json
import logging
from typing import Dict, List, Any, Set, Optional
# import pandas as pd # No longer needed

from core.config import settings
from core.connection_manager import manager as frontend_manager
# Import only the necessary service function
from services.dhan_service import get_nifty50_constituent_list
from models.stock import StockListItem, WebSocketResponse, WebSocketTickerPayload, WebSocketQuotePayload, WebSocketErrorPayload

logger = logging.getLogger(__name__)

# --- Constants (Based on DhanHQ v2 Docs - !!! VERIFY THESE !!!) ---
DHAN_FEED_WS_URL_FORMAT = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
REQUEST_CODE_SUBSCRIBE = 15
REQUEST_CODE_UNSUBSCRIBE = 16 # Assumed, VERIFY
REQUEST_CODE_DISCONNECT = 12
RESPONSE_CODE_TICKER = 11
RESPONSE_CODE_QUOTE = 12
RESPONSE_CODE_FULL = 13
RESPONSE_CODE_PREV_CLOSE = 15
RESPONSE_CODE_OI = 5
RESPONSE_CODE_SERVER_DISCONNECT = 99
RESPONSE_CODE_PING = 100

# --- Global State ---
security_id_to_symbol_map: Dict[str, str] = {}
security_id_to_prev_close_map: Dict[str, float] = {}
dhan_subscribed_instruments: Dict[str, Set[str]] = {}
dhan_websocket: Optional[websockets.WebSocketClientProtocol] = None
subscription_lock = asyncio.Lock()

# --- Helper Functions ---
async def populate_instrument_maps():
    """Populates mappings using the enriched Nifty 50 list from the service."""
    global security_id_to_symbol_map
    if security_id_to_symbol_map: return # Already populated
    try:
        # Get the enriched list (which now loads local JSON + maps IDs if needed)
        nifty50_list: List[StockListItem] = await get_nifty50_constituent_list()
        # --- FIX: Build map directly from the StockListItem list ---
        security_id_to_symbol_map = {
            stock.security_id: stock.symbol
            for stock in nifty50_list if stock.security_id and stock.symbol
        }
        logger.info(f"Populated security_id_to_symbol map with {len(security_id_to_symbol_map)} entries from local list.")
        if not security_id_to_symbol_map:
            logger.error("Security ID to Symbol map is empty after processing local list. WebSocket parsing will fail.")
    except Exception as e:
        logger.error(f"Failed to populate instrument maps from local Nifty 50 list: {e}", exc_info=True)
        security_id_to_symbol_map = {} # Ensure map is empty on error

def get_symbol_from_id(security_id: str) -> Optional[str]:
    """Looks up symbol from security ID using the populated map."""
    return security_id_to_symbol_map.get(security_id)

# --- WebSocket Message Parsing ---
# (parse_dhan_message remains the same - relies on get_symbol_from_id which is now fixed)
async def parse_dhan_message(message: bytes):
    """Parses incoming binary messages from Dhan WebSocket feed."""
    global security_id_to_prev_close_map
    try:
        header_format = '<BHBI'; header_size = struct.calcsize(header_format)
        if len(message) < header_size: return
        message_code, message_length, segment_code, security_id_int = struct.unpack(header_format, message[:header_size])
        payload = message[header_size:]; actual_payload_len = len(payload); security_id = str(security_id_int)
        if actual_payload_len < message_length: logger.warning(f"Payload length mismatch for SecID {security_id}"); return
        symbol = get_symbol_from_id(security_id)
        if not symbol: return # Unknown ID

        if message_code == RESPONSE_CODE_TICKER:
            ticker_format = '<fi'; expected_size = struct.calcsize(ticker_format)
            if actual_payload_len >= expected_size:
                ltp, ltt = struct.unpack(ticker_format, payload[:expected_size])
                prev_close = security_id_to_prev_close_map.get(security_id)
                change = round(ltp - prev_close, 2) if prev_close is not None else None
                percent_change = round((change / prev_close) * 100, 2) if prev_close is not None and prev_close != 0 and change is not None else None
                payload_data = WebSocketTickerPayload(symbol=symbol, ltp=round(ltp, 2), ltt=ltt, change=change, percent_change=percent_change)
                frontend_message = WebSocketResponse(type="ticker_update", payload=payload_data.model_dump()).model_dump_json()
                await frontend_manager.broadcast(frontend_message)
            else: logger.warning(f"Ticker payload too short for {symbol}")

        elif message_code == RESPONSE_CODE_QUOTE:
            quote_format = '<fhifiiiifff'; expected_size = struct.calcsize(quote_format)
            if actual_payload_len >= expected_size:
                ltp, lq, ltt, atp, vol, tsq, tbq, o, c, h, l = struct.unpack(quote_format, payload[:expected_size])
                prev_close = security_id_to_prev_close_map.get(security_id)
                payload_data = WebSocketQuotePayload(
                    symbol=symbol, ltp=round(ltp, 2), ltt=ltt, volume=vol, open=round(o, 2), high=round(h, 2), low=round(l, 2),
                    close=round(c, 2) if c != 0 else None, atp=round(atp, 2), total_buy_qty=tbq, total_sell_qty=tsq, prev_close=prev_close)
                frontend_message = WebSocketResponse(type="quote_update", payload=payload_data.model_dump()).model_dump_json()
                await frontend_manager.broadcast(frontend_message)
            else: logger.warning(f"Quote payload too short for {symbol}")

        elif message_code == RESPONSE_CODE_PREV_CLOSE:
            prev_close_format = '<fi' # !!! VERIFY !!! prev_close(f4), prev_oi(i4) = 8 bytes
            expected_size = struct.calcsize(prev_close_format)
            if actual_payload_len >= expected_size:
                prev_close_price, prev_oi = struct.unpack(prev_close_format, payload[:expected_size])
                logger.debug(f"PrevClose Packet: Symbol={symbol}, PrevClose={prev_close_price:.2f}")
                security_id_to_prev_close_map[security_id] = prev_close_price # Store it
            else: logger.warning(f"PrevClose payload too short for {symbol}")

        elif message_code == RESPONSE_CODE_PING: pass
        elif message_code == RESPONSE_CODE_SERVER_DISCONNECT:
             disconnect_format = '<H'; reason_code = "N/A"
             if actual_payload_len >= struct.calcsize(disconnect_format): reason_code = struct.unpack(disconnect_format, payload[:struct.calcsize(disconnect_format)])[0]
             logger.warning(f"Received Server Disconnect from Dhan for {symbol}. Reason: {reason_code}")
        else: pass
    except struct.error as e: logger.error(f"Struct error unpacking Dhan message: {e}", exc_info=True)
    except Exception as e: logger.error(f"Error processing Dhan message: {e}", exc_info=True)

# --- WebSocket Subscription Management ---
# (send_subscription_request remains the same)
async def send_subscription_request(requests_to_make: List[Dict]):
    global dhan_websocket
    if not dhan_websocket: logger.warning("Cannot send request, Dhan WS not connected."); return False
    if not requests_to_make: return True
    success = True
    for req in requests_to_make:
        try:
            message_str = json.dumps(req); await dhan_websocket.send(message_str)
            rc = req.get('RequestCode'); ic = req.get('InstrumentCount')
            logger.info(f"Sent WS request code {rc} for {ic} instruments.")
            await asyncio.sleep(0.1)
        except websockets.exceptions.ConnectionClosed: logger.error(f"Failed to send WS request ({req}): Connection closed."); success = False; break
        except Exception as e: logger.error(f"Failed to send WS request ({req}): {e}"); success = False
    return success

# (format_subscription_requests remains the same)
async def format_subscription_requests(instruments_details: List[Dict], subscribe: bool) -> List[Dict]:
    requests_list = []; chunk_size = 100
    request_code = REQUEST_CODE_SUBSCRIBE if subscribe else REQUEST_CODE_UNSUBSCRIBE
    subs_by_type: Dict[str, List[Dict]] = {}
    for details in instruments_details:
        dtype = details['dataType']
        instrument = {"exchangeSegment": details['exchangeSegment'], "securityId": details['securityId']}
        if dtype not in subs_by_type: subs_by_type[dtype] = []
        subs_by_type[dtype].append(instrument)
    for dtype, instruments in subs_by_type.items():
        if not instruments: continue
        for i in range(0, len(instruments), chunk_size):
            chunk = instruments[i:i + chunk_size]
            request = {"RequestCode": request_code, "InstrumentCount": len(chunk), "InstrumentList": chunk}
            requests_list.append(request)
    return requests_list

# (update_subscriptions remains the same)
async def update_subscriptions(security_id: str, exchange_segment: str, data_type: str, subscribe: bool = True):
    global dhan_subscribed_instruments
    async with subscription_lock:
        current_types = dhan_subscribed_instruments.get(security_id, set())
        action_taken = False
        target_details = {"securityId": security_id, "exchangeSegment": exchange_segment, "dataType": data_type}
        requests_to_send = []
        if subscribe:
            if data_type not in current_types:
                logger.info(f"Requesting Dhan subscription: {security_id} - {data_type}")
                requests_to_send = await format_subscription_requests([target_details], subscribe=True)
                if await send_subscription_request(requests_to_send):
                    current_types.add(data_type); dhan_subscribed_instruments[security_id] = current_types; action_taken = True
                else: logger.error(f"Failed to send subscribe request for {security_id} - {data_type}")
        else: # Unsubscribe
            if data_type in current_types:
                logger.info(f"Requesting Dhan unsubscription: {security_id} - {data_type}")
                requests_to_send = await format_subscription_requests([target_details], subscribe=False)
                if await send_subscription_request(requests_to_send):
                    current_types.remove(data_type)
                    if not current_types: del dhan_subscribed_instruments[security_id]
                    else: dhan_subscribed_instruments[security_id] = current_types
                    action_taken = True
                else: logger.error(f"Failed to send unsubscribe request for {security_id} - {data_type}")
        return action_taken

# --- Main Connection Logic ---
async def connect_dhan_websocket():
    """Establishes connection, handles messages, and manages initial subscriptions."""
    global dhan_websocket, dhan_subscribed_instruments, security_id_to_prev_close_map
    client_id = settings.DHAN_CLIENT_ID; access_token = settings.DHAN_ACCESS_TOKEN
    if not access_token or access_token == "YOUR_DHAN_ACCESS_TOKEN_HERE" or not client_id:
        logger.error("Dhan Access Token or Client ID not configured."); await asyncio.sleep(60); return

    ws_url = DHAN_FEED_WS_URL_FORMAT.format(token=access_token, client_id=client_id)
    logger.info(f"Attempting to connect to Dhan WebSocket: {ws_url.split('token=')[0]}...")

    try:
        async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10, close_timeout=10) as websocket:
            logger.info("Connected to Dhan WebSocket."); dhan_websocket = websocket

            # --- Subscribe to Nifty 50 Tickers + PrevClose initially ---
            nifty50_list = await get_nifty50_constituent_list() # Get enriched list from service
            initial_subs_details = []
            # Add Nifty 50 Index itself (Ticker + PrevClose)
            initial_subs_details.append({"securityId": "13", "exchangeSegment": "IDX", "dataType": "TICKER"})
            initial_subs_details.append({"securityId": "13", "exchangeSegment": "IDX", "dataType": "PREV_CLOSE"})

            for stock in nifty50_list:
                 # Use attributes from StockListItem model
                 if stock.security_id and stock.exchange == 'NSE' and stock.segment == 'EQUITY':
                      # Subscribe to both Ticker and PrevClose for each stock
                      initial_subs_details.append({"securityId": stock.security_id, "exchangeSegment": "NSE_EQ", "dataType": "TICKER"})
                      initial_subs_details.append({"securityId": stock.security_id, "exchangeSegment": "NSE_EQ", "dataType": "PREV_CLOSE"})

            async with subscription_lock:
                 requests_to_send = await format_subscription_requests(initial_subs_details, subscribe=True)
                 success = await send_subscription_request(requests_to_send)
                 if success:
                      dhan_subscribed_instruments.clear(); security_id_to_prev_close_map.clear() # Clear state on successful connect/sub
                      for sub in initial_subs_details:
                           sid = sub['securityId']; dtype = sub['dataType']
                           if sid not in dhan_subscribed_instruments: dhan_subscribed_instruments[sid] = set()
                           dhan_subscribed_instruments[sid].add(dtype)
                      logger.info(f"Initial Nifty 50 TICKER+PREV_CLOSE subscriptions sent ({len(nifty50_list) + 1} instruments).")
                 else: logger.error("Failed to send initial Nifty 50 subscriptions.")

            logger.info("Starting to listen for Dhan feed messages...")
            while True:
                message = await websocket.recv()
                if isinstance(message, bytes): await parse_dhan_message(message)
                else: logger.warning(f"Received non-bytes message: {type(message)}")

    except Exception as e:
         if isinstance(e, websockets.exceptions.InvalidStatusCode) and e.status_code == 401: logger.error(f"Dhan WS connection failed: 401 Unauthorized. Check Token/ID.")
         elif isinstance(e, websockets.exceptions.WebSocketException): logger.error(f"Dhan WS exception: {e}")
         else: logger.error(f"Unhandled error in Dhan WS connection loop: {e}", exc_info=True)
    finally:
         logger.info("Cleaning up Dhan WebSocket connection state.")
         dhan_websocket = None; dhan_subscribed_instruments.clear(); security_id_to_prev_close_map.clear()

# (run_dhan_feed_client remains the same)
async def run_dhan_feed_client():
    """Runs the Dhan WebSocket client with reconnection logic."""
    await populate_instrument_maps() # Load maps once at startup
    retry_delay = 5; max_retry_delay = 60
    while True:
        await connect_dhan_websocket()
        logger.info(f"Dhan WebSocket client disconnected/stopped. Reconnecting in {retry_delay} seconds...")
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


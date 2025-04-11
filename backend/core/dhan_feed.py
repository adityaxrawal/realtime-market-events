# backend/core/dhan_feed.py
# Handles connection to DhanHQ WebSocket Market Feed (v2)

import asyncio
import websockets
import struct
import json
import logging
from typing import Dict, List, Any, Set, Optional

# Import necessary components from our application
from core.config import settings
from main import manager as frontend_manager # To broadcast to frontend clients
from services.dhan_service import get_nifty50_constituent_list, load_instrument_data # To get instrument list/map
from models.stock import StockListItem, WebSocketResponse # To use models

logger = logging.getLogger(__name__)

# --- Constants (Based on DhanHQ v2 Docs - !!! VERIFY THESE !!!) ---
# Main Market Feed (Ticker, Quote, Full)
DHAN_FEED_WS_URL_FORMAT = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
# Message Codes (!!! REFER TO DHAN V2 DOCS FOR EXACT CODES !!!)
# Request Codes (Client -> Server, JSON)
REQUEST_CODE_SUBSCRIBE = 15 # Example code, verify (Live market feed.pdf, p1)
REQUEST_CODE_UNSUBSCRIBE = 16 # Example code, verify
REQUEST_CODE_DISCONNECT = 12 # Example code, verify (Live market feed.pdf, p4)
# Response Codes (Server -> Client, Binary Header)
RESPONSE_CODE_TICKER = 11        # Example code, verify (Live market feed.pdf, p2 uses code 2?) -> Using 11 as more specific
RESPONSE_CODE_QUOTE = 12         # Example code, verify (Live market feed.pdf, p3 uses code 3?) -> Using 12
RESPONSE_CODE_FULL = 13          # Example code, verify (Live market feed.pdf, p3 uses code 6?) -> Using 13
RESPONSE_CODE_PREV_CLOSE = 15    # Example code, verify (Live market feed.pdf, p3 uses code 4?) -> Using 15
RESPONSE_CODE_OI = 5             # Example code, verify (Live market feed.pdf, p3)
RESPONSE_CODE_SERVER_DISCONNECT = 99 # Example code, verify (Live market feed.pdf, p4 uses code 50?) -> Using 99
RESPONSE_CODE_PING = 100         # Example code, verify

# --- Global State for Subscriptions & Mapping ---
# Store security_id to symbol mapping (populated at startup)
security_id_to_symbol_map: Dict[str, str] = {}
# Store currently subscribed instruments (Security ID -> DataType Set {'TICKER', 'QUOTE', 'FULL'})
subscribed_instruments: Dict[str, Set[str]] = {}
# Store the active Dhan WebSocket connection
dhan_websocket: Optional[websockets.WebSocketClientProtocol] = None
# Lock for modifying subscriptions safely
subscription_lock = asyncio.Lock()

# --- Helper Functions ---

async def populate_instrument_maps():
    """Loads instrument data and populates mappings."""
    global security_id_to_symbol_map
    try:
        df = await load_instrument_data() # Use the service function
        # Create mapping from Security ID to Trading Symbol
        security_id_to_symbol_map = pd.Series(
            df.SEM_TRADING_SYMBOL.values,
            index=df.SECURITY_ID.astype(str)
        ).to_dict()
        logger.info(f"Populated security_id_to_symbol map with {len(security_id_to_symbol_map)} entries.")
        if not security_id_to_symbol_map:
             logger.warning("Security ID to Symbol map is empty after loading data.")

    except Exception as e:
        logger.error(f"Failed to populate instrument maps: {e}", exc_info=True)

def get_symbol_from_id(security_id: str) -> Optional[str]:
    """Looks up symbol from security ID using the populated map."""
    return security_id_to_symbol_map.get(security_id)

# --- WebSocket Message Parsing ---

async def parse_dhan_message(message: bytes):
    """
    Parses incoming binary messages from Dhan WebSocket feed.
    *** THIS IS HIGHLY DEPENDENT ON DHAN V2 DOCUMENTATION - IMPLEMENT CAREFULLY ***
    *** THE FORMAT STRINGS AND OFFSETS BELOW ARE EXAMPLES AND MUST BE VERIFIED ***
    """
    try:
        # --- Parse Header (8 Bytes - Live market feed.pdf, p2) ---
        header_format = '<BHBI' # !!! VERIFY !!! Little-endian: code(uByte,1), len(uShort,2), seg(uByte,1), secId(uInt,4) = 8 bytes
        header_size = struct.calcsize(header_format)

        if len(message) < header_size:
            logger.warning(f"Received message shorter than header size: {len(message)} < {header_size}")
            return

        message_code, message_length, segment_code, security_id_int = struct.unpack(header_format, message[:header_size])
        payload = message[header_size:]
        actual_payload_len = len(payload)
        security_id = str(security_id_int) # Use string ID consistent with map

        # logger.debug(f"Rcvd: Code={message_code}, Len={message_length}, Seg={segment_code}, SecID={security_id}, PayloadLen={actual_payload_len}")

        if actual_payload_len < message_length:
            logger.warning(f"Payload length mismatch for SecID {security_id}: Expected {message_length}, Got {actual_payload_len}. Processing available.")
            # Continue processing, but be aware data might be truncated

        symbol = get_symbol_from_id(security_id)
        if not symbol:
             # logger.debug(f"Received packet for unknown security_id: {security_id}")
             return # Ignore if we don't know the symbol

        # --- Handle Message Based on Code ---
        if message_code == RESPONSE_CODE_TICKER:
            # --- Parse Ticker Packet (Payload: LTP(float32,4), LTT(int32,4) = 8 bytes - Live market feed.pdf, p2) ---
            ticker_format = '<fi' # !!! VERIFY !!! Little-endian: ltp(float,4), ltt(int,4) = 8 bytes
            expected_size = struct.calcsize(ticker_format)

            if actual_payload_len >= expected_size:
                ltp, ltt = struct.unpack(ticker_format, payload[:expected_size])
                # logger.info(f"Ticker: Symbol={symbol}, LTP={ltp:.2f}, LTT={ltt}")
                frontend_message = WebSocketResponse(
                    type="ticker_update",
                    payload={"symbol": symbol, "ltp": round(ltp, 2), "ltt": ltt}
                ).model_dump_json() # Use model_dump_json() in Pydantic v2
                await frontend_manager.broadcast(frontend_message)
            else:
                logger.warning(f"Ticker payload too short for {symbol}: Expected {expected_size}, Got {actual_payload_len}")

        elif message_code == RESPONSE_CODE_QUOTE:
            # --- Parse Quote Packet (Payload: See Live market feed.pdf, p3) ---
            # LTP(f4), LQty(h2), LTT(i4), ATP(f4), Vol(i4), TotSell(i4), TotBuy(i4), Open(f4), Close(f4), High(f4), Low(f4) = 42 bytes
            quote_format = '<fhifiiiifff' # !!! VERIFY !!! f(4)+h(2)+i(4)+f(4)+i(4)+i(4)+i(4)+f(4)+f(4)+f(4)+f(4) = 42 bytes
            expected_size = struct.calcsize(quote_format)

            if actual_payload_len >= expected_size:
                ltp, last_qty, ltt, atp, volume, total_sell_qty, total_buy_qty, day_open, day_close, day_high, day_low = struct.unpack(quote_format, payload[:expected_size])
                # logger.info(f"Quote: Symbol={symbol}, LTP={ltp:.2f}, O={day_open}, H={day_high}, L={day_low}, C={day_close}, V={volume}") # Close might be 0 intraday
                frontend_message = WebSocketResponse(
                    type="quote_update", # Use for selected stock OHLC updates
                    payload={
                        "symbol": symbol,
                        "ltp": round(ltp, 2),
                        "ltt": ltt,
                        "volume": volume,
                        "open": round(day_open, 2),
                        "high": round(day_high, 2),
                        "low": round(day_low, 2),
                        "close": round(day_close, 2) if day_close != 0 else None, # Send close only if available
                        "atp": round(atp, 2),
                        "total_buy_qty": total_buy_qty,
                        "total_sell_qty": total_sell_qty,
                    }
                ).model_dump_json()
                await frontend_manager.broadcast(frontend_message) # Broadcast for now, could filter later
            else:
                logger.warning(f"Quote payload too short for {symbol}: Expected {expected_size}, Got {actual_payload_len}")

        # --- Add parsing for other relevant codes (FULL, PREV_CLOSE, OI) if needed ---

        elif message_code == RESPONSE_CODE_PING:
            # logger.debug("Received Ping from Dhan")
            pass # Handled by websockets library

        elif message_code == RESPONSE_CODE_SERVER_DISCONNECT:
             disconnect_format = '<H' # Example: 2-byte reason code - VERIFY
             expected_size = struct.calcsize(disconnect_format)
             reason_code = "N/A"
             if actual_payload_len >= expected_size:
                  reason_code = struct.unpack(disconnect_format, payload[:expected_size])[0]
             logger.warning(f"Received Server Disconnect message from Dhan for {symbol}. Reason Code: {reason_code}")

        else:
            # logger.debug(f"Received unhandled message code {message_code} for {symbol}")
            pass

    except struct.error as e:
        logger.error(f"Error unpacking Dhan message structure: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error processing Dhan message: {e}", exc_info=True)

# --- WebSocket Subscription Management ---

async def send_subscription_request(instruments_to_sub: List[Dict], instruments_to_unsub: List[Dict]):
    """Sends subscribe/unsubscribe requests to the active Dhan WebSocket."""
    global dhan_websocket
    if not dhan_websocket or not dhan_websocket.open:
        logger.warning("Cannot send subscription request, Dhan WebSocket is not connected.")
        return False

    # Structure based on Live market feed.pdf, p1 (JSON Request)
    # Combine sub/unsub logic if API supports it, otherwise send separate messages
    requests_to_send = []

    if instruments_to_sub:
         # Group by data type
         subs_by_type = {}
         for sub in instruments_to_sub:
              dtype = sub['dataType']
              if dtype not in subs_by_type: subs_by_type[dtype] = []
              subs_by_type[dtype].append({"exchangeSegment": sub['exchangeSegment'], "securityId": sub['securityId']})

         for dtype, instruments in subs_by_type.items():
              if instruments:
                   # Split into chunks of 100 (VERIFY LIMIT)
                   chunk_size = 100
                   for i in range(0, len(instruments), chunk_size):
                        chunk = instruments[i:i + chunk_size]
                        request = {
                             "RequestCode": REQUEST_CODE_SUBSCRIBE, # Verify
                             "InstrumentCount": len(chunk),
                             "InstrumentList": chunk,
                             "SubscribeType": dtype # Assuming a field like this exists - VERIFY DHAN DOCS
                             # Or maybe dataType is part of InstrumentList items? VERIFY
                        }
                        requests_to_send.append(request)

    if instruments_to_unsub:
         # Similar chunking logic for unsubscribe
         chunk_size = 100
         for i in range(0, len(instruments_to_unsub), chunk_size):
              chunk = instruments_to_unsub[i:i + chunk_size]
              request = {
                   "RequestCode": REQUEST_CODE_UNSUBSCRIBE, # Verify
                   "InstrumentCount": len(chunk),
                   "InstrumentList": chunk
                   # Unsubscribe might not need data type? VERIFY
              }
              requests_to_send.append(request)


    if not requests_to_send:
         return True # Nothing to do

    success = True
    for req in requests_to_send:
        try:
            message_str = json.dumps(req)
            await dhan_websocket.send(message_str)
            logger.info(f"Sent WebSocket request: {req.get('RequestCode')} for {req.get('InstrumentCount')} instruments")
            await asyncio.sleep(0.1) # Small delay between requests
        except Exception as e:
            logger.error(f"Failed to send WebSocket request ({req}): {e}")
            success = False
            # Decide whether to break or try sending remaining requests

    return success


async def update_subscriptions(security_id: str, exchange_segment: str, data_type: str, subscribe: bool = True):
    """Adds or removes a subscription for a specific instrument and data type."""
    global subscribed_instruments
    async with subscription_lock:
        current_types = subscribed_instruments.get(security_id, set())
        needs_update = False
        target_instrument = {"securityId": security_id, "exchangeSegment": exchange_segment, "dataType": data_type} # Assuming dataType needed

        if subscribe:
            if data_type not in current_types:
                current_types.add(data_type)
                subscribed_instruments[security_id] = current_types
                needs_update = True
                logger.info(f"Adding subscription: {security_id} - {data_type}")
                await send_subscription_request(instruments_to_sub=[target_instrument], instruments_to_unsub=[])
            else:
                 logger.debug(f"Already subscribed to {security_id} - {data_type}")
        else: # Unsubscribe
            if data_type in current_types:
                current_types.remove(data_type)
                if not current_types: # Remove entry if no types left for this ID
                    del subscribed_instruments[security_id]
                else:
                    subscribed_instruments[security_id] = current_types
                needs_update = True # Need to send unsubscribe request
                logger.info(f"Removing subscription: {security_id} - {data_type}")
                await send_subscription_request(instruments_to_sub=[], instruments_to_unsub=[target_instrument])
            else:
                 logger.debug(f"Not subscribed to {security_id} - {data_type}, cannot unsubscribe.")

# --- Main Connection Logic ---

async def connect_dhan_websocket():
    """Establishes connection, handles messages, and manages initial subscriptions."""
    global dhan_websocket
    client_id = settings.DHAN_CLIENT_ID
    access_token = settings.DHAN_ACCESS_TOKEN

    if not access_token or access_token == "YOUR_DHAN_ACCESS_TOKEN_HERE":
        logger.error("Dhan Access Token not configured. Cannot connect.")
        await asyncio.sleep(60)
        return

    ws_url = DHAN_FEED_WS_URL_FORMAT.format(token=access_token, client_id=client_id)
    logger.info(f"Attempting to connect to Dhan WebSocket: {ws_url}")

    try:
        async with websockets.connect(
            ws_url,
            ping_interval=30, ping_timeout=10, close_timeout=10
            ) as websocket:
            logger.info("Connected to Dhan WebSocket.")
            dhan_websocket = websocket # Store active connection

            # --- Subscribe to Nifty 50 Tickers initially ---
            nifty50_list = await get_nifty50_constituent_list()
            nifty50_subs = [
                 {
                      "securityId": stock.security_id,
                      "exchangeSegment": "NSE_EQ", # Assuming Nifty 50 are NSE Equity
                      "dataType": "TICKER"
                 } for stock in nifty50_list if stock.security_id
            ]
            # Add Nifty 50 Index itself
            nifty50_subs.append({"securityId": "13", "exchangeSegment": "IDX", "dataType": "TICKER"})

            async with subscription_lock:
                 await send_subscription_request(instruments_to_sub=nifty50_subs, instruments_to_unsub=[])
                 # Update global state
                 for sub in nifty50_subs:
                      sid = sub['securityId']
                      dtype = sub['dataType']
                      if sid not in subscribed_instruments: subscribed_instruments[sid] = set()
                      subscribed_instruments[sid].add(dtype)

            # --- Receive and Process Messages ---
            logger.info("Starting to listen for Dhan feed messages...")
            while True:
                message = await websocket.recv()
                if isinstance(message, bytes):
                    await parse_dhan_message(message)
                else:
                    logger.warning(f"Received non-bytes message: {type(message)}")

    # Handle exceptions (same as before)
    except websockets.exceptions.ConnectionClosedOK:
         logger.info("Dhan WebSocket connection closed normally.")
    except websockets.exceptions.ConnectionClosedError as e:
        logger.error(f"Dhan WebSocket connection closed unexpectedly: Code={e.code}, Reason='{e.reason}'")
    except websockets.exceptions.InvalidHandshake as e:
         logger.error(f"Dhan WebSocket handshake failed: {e}")
    except ConnectionRefusedError:
         logger.error(f"Dhan WebSocket connection refused.")
    except OSError as e:
         logger.error(f"Dhan WebSocket connection OS error: {e}")
    except asyncio.TimeoutError:
         logger.error("Dhan WebSocket connection timed out.")
    except Exception as e:
        logger.error(f"Error in Dhan WebSocket connection loop: {e}", exc_info=True)
    finally:
         dhan_websocket = None # Clear active connection on exit
         subscribed_instruments.clear() # Clear subscriptions on disconnect


async def run_dhan_feed_client():
    """Runs the Dhan WebSocket client with reconnection logic and pre-populates maps."""
    await populate_instrument_maps() # Load maps once at startup

    retry_delay = 5 # Initial delay in seconds
    max_retry_delay = 60 # Maximum delay
    while True:
        await connect_dhan_websocket()
        logger.info(f"Dhan WebSocket client disconnected/stopped. Reconnecting in {retry_delay} seconds...")
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay) # Exponential backoff


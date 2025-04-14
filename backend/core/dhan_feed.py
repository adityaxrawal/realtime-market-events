# backend/core/dhan_feed.py
# Handles connection to DhanHQ WebSocket Market Feed (v2)
# Includes Kafka production for Quote and Depth data.

import asyncio
import websockets
import struct
import json
import logging
import time # Added for sleep
from typing import Dict, List, Any, Set, Optional

# Added Kafka imports
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from core.config import settings
from core.connection_manager import manager as frontend_manager
# Import only the necessary service function
from services.dhan_service import get_nifty50_constituent_list
from models.stock import (
    StockListItem, WebSocketResponse, WebSocketTickerPayload,
    WebSocketQuotePayload, WebSocketErrorPayload
) # Added WebSocketDepthPayload if defined in models

logger = logging.getLogger(__name__)

# --- Constants (Based on DhanHQ v2 Docs - !!! VERIFY THESE !!!) ---
DHAN_FEED_WS_URL_FORMAT = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
REQUEST_CODE_SUBSCRIBE = 15
REQUEST_CODE_UNSUBSCRIBE = 16 # Assumed, VERIFY
REQUEST_CODE_DISCONNECT = 12
RESPONSE_CODE_TICKER = 11
RESPONSE_CODE_QUOTE = 12
RESPONSE_CODE_FULL = 13 # Often includes depth
RESPONSE_CODE_DEPTH = 41 # From 20 Market Depth PDF - VERIFY if this is used for the feed
RESPONSE_CODE_PREV_CLOSE = 15
RESPONSE_CODE_OI = 5
RESPONSE_CODE_SERVER_DISCONNECT = 99
RESPONSE_CODE_PING = 100

# --- Kafka Configuration ---
KAFKA_MARKET_DATA_TOPIC = "stock_market_data"
KAFKA_DEPTH_DATA_TOPIC = "market_depth_data"

# --- Global State ---
security_id_to_symbol_map: Dict[str, str] = {}
security_id_to_prev_close_map: Dict[str, float] = {}
dhan_subscribed_instruments: Dict[str, Set[str]] = {}
dhan_websocket: Optional[websockets.WebSocketClientProtocol] = None
subscription_lock = asyncio.Lock()
# Kafka Producer instance
kafka_producer: Optional[KafkaProducer] = None

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

# --- Kafka Producer Management ---
def create_or_get_kafka_producer():
    """Creates or gets the KafkaProducer instance with retry logic."""
    global kafka_producer
    if kafka_producer:
        # Basic check if producer is still valid (might need more robust checks)
        try:
            if kafka_producer.bootstrap_connected():
                return kafka_producer
            else:
                logger.warning("Kafka producer seems disconnected, attempting to recreate.")
                try: kafka_producer.close()
                except Exception: pass
                kafka_producer = None
        except Exception:
            logger.warning("Error checking Kafka producer status, attempting to recreate.")
            try: kafka_producer.close()
            except Exception: pass
            kafka_producer = None


    retry_delay = 2
    max_retries = 5
    attempt = 0
    while kafka_producer is None and attempt < max_retries:
        attempt += 1
        try:
            # Use internal Kafka address defined in settings
            kafka_bootstrap_servers = settings.KAFKA_BROKER_INTERNAL
            if not kafka_bootstrap_servers:
                 logger.error("KAFKA_BROKER_INTERNAL not configured in settings.")
                 time.sleep(retry_delay)
                 retry_delay = min(retry_delay * 2, 30)
                 continue

            kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=500,
                request_timeout_ms=5000 # Add timeout
            )
            logger.info(f"KafkaProducer connected for Dhan Feed at {kafka_bootstrap_servers}")
            return kafka_producer
        except NoBrokersAvailable:
            logger.error(f"Kafka broker at {kafka_bootstrap_servers} not available for Dhan Feed (Attempt {attempt}/{max_retries}). Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)
        except Exception as e:
            logger.error(f"Failed to create Kafka Producer for Dhan Feed (Attempt {attempt}/{max_retries}): {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)

    if kafka_producer is None:
         logger.critical("Failed to create Kafka producer after multiple retries. Kafka production disabled.")
    return kafka_producer


# --- WebSocket Message Parsing ---
async def parse_dhan_message(message: bytes):
    """Parses incoming binary messages from Dhan WebSocket feed and sends to Kafka/Frontend."""
    global security_id_to_prev_close_map
    producer = create_or_get_kafka_producer() # Get producer instance

    try:
        # --- MODIFIED: Read header based on 20 Market Depth PDF (12 bytes + 1 feed code) ---
        # < Message Length (uint16), Feed Code (byte), Exchange Segment (byte), Security ID (int32), Sequence (uint32)
        header_format = '<HBBII'; header_size = struct.calcsize(header_format)
        if len(message) < header_size:
            logger.warning(f"Message too short for header: {len(message)} bytes")
            return

        message_length, message_code, segment_code, security_id_int, sequence = struct.unpack(header_format, message[:header_size])
        # Note: message_length in header might not include header itself, adjust payload slicing if needed
        payload = message[header_size:]
        actual_payload_len = len(payload)
        security_id = str(security_id_int)

        # Basic validation
        # if actual_payload_len < message_length:
        #     logger.warning(f"Payload length mismatch for SecID {security_id}. Header says {message_length}, got {actual_payload_len}")
        #     # Allow processing if slightly different, but log it. Strict check might discard valid data.
        #     # return

        symbol = get_symbol_from_id(security_id)
        if not symbol:
            # logger.debug(f"Ignoring message for unknown Security ID: {security_id}")
            return # Unknown ID

        # --- Process based on Message Code ---
        if message_code == RESPONSE_CODE_TICKER:
            ticker_format = '<fi'; expected_size = struct.calcsize(ticker_format)
            if actual_payload_len >= expected_size:
                ltp, ltt = struct.unpack_from(ticker_format, payload, 0) # Use unpack_from
                prev_close = security_id_to_prev_close_map.get(security_id)
                change = round(ltp - prev_close, 2) if prev_close is not None else None
                percent_change = round((change / prev_close) * 100, 2) if prev_close is not None and prev_close != 0 and change is not None else None

                # Send Ticker to Frontend
                payload_data = WebSocketTickerPayload(symbol=symbol, ltp=round(ltp, 2), ltt=ltt, change=change, percent_change=percent_change)
                frontend_message = WebSocketResponse(type="ticker_update", payload=payload_data.model_dump()).model_dump_json()
                await frontend_manager.broadcast(frontend_message)

                # --- ADDED: Send Ticker to Kafka ---
                if producer:
                    kafka_ticker_payload = {
                         "timestamp": ltt,
                         "symbol": symbol,
                         "ltp": round(ltp, 2),
                         "prev_close": prev_close # Include prev_close if available
                    }
                    try:
                        producer.send(KAFKA_MARKET_DATA_TOPIC, value=kafka_ticker_payload, key=symbol.encode('utf-8'))
                        # logger.debug(f"Sent Ticker data for {symbol} to Kafka topic '{KAFKA_MARKET_DATA_TOPIC}'")
                    except Exception as kafka_err:
                        logger.error(f"Failed to send Ticker data for {symbol} to Kafka: {kafka_err}")
                else:
                     logger.warning("Kafka producer not available, skipping Ticker send.")

            else: logger.warning(f"Ticker payload too short for {symbol}")

        elif message_code == RESPONSE_CODE_QUOTE:
            quote_format = '<fhifiiiifff'; expected_size = struct.calcsize(quote_format)
            if actual_payload_len >= expected_size:
                ltp, lq, ltt, atp, vol, tsq, tbq, o, c, h, l = struct.unpack_from(quote_format, payload, 0) # Use unpack_from
                prev_close = security_id_to_prev_close_map.get(security_id)

                # Prepare payload for both Kafka and Frontend
                common_payload = {
                    "timestamp": ltt,
                    "symbol": symbol,
                    "ltp": round(ltp, 2),
                    "last_traded_qty": lq,
                    "atp": round(atp, 2),
                    "volume": vol,
                    "total_sell_qty": tsq,
                    "total_buy_qty": tbq,
                    "open": round(o, 2),
                    "high": round(h, 2),
                    "low": round(l, 2),
                    "close": round(c, 2) if c != 0 else None, # Use None for intraday close
                    "prev_close": prev_close
                }

                # --- ADDED: Send Quote to Kafka ---
                if producer:
                    try:
                        producer.send(KAFKA_MARKET_DATA_TOPIC, value=common_payload, key=symbol.encode('utf-8'))
                        # logger.debug(f"Sent Quote data for {symbol} to Kafka topic '{KAFKA_MARKET_DATA_TOPIC}'")
                    except Exception as kafka_err:
                        logger.error(f"Failed to send Quote data for {symbol} to Kafka: {kafka_err}")
                else:
                     logger.warning("Kafka producer not available, skipping Quote send.")

                # Send Quote to Frontend
                payload_data = WebSocketQuotePayload(**common_payload)
                frontend_message = WebSocketResponse(type="quote_update", payload=payload_data.model_dump()).model_dump_json()
                await frontend_manager.broadcast(frontend_message)

            else: logger.warning(f"Quote payload too short for {symbol}")

        # --- ADDED: Depth Parsing and Kafka Sending ---
        # Verify the correct code for 20-level depth (using 41 from PDF)
        elif message_code == RESPONSE_CODE_DEPTH:
            # Structure based on Dhan 20 Market Depth PDF
            # Header (12 bytes) + Feed Code (1 byte) = 13 bytes offset? NO, header includes feed code. Offset is header_size.
            # Format: <price (float64), quantity (uint32), orders (uint32)> = 8+4+4 = 16 bytes per level
            depth_level_format = '<dII'
            level_size = struct.calcsize(depth_level_format)
            num_levels = 20 # 20 bids + 20 asks
            expected_payload_size = num_levels * level_size

            # The PDF implies separate packets for Bid (41) and Ask (42). Assuming one packet contains one side.
            # Let's assume code 41 is Bid and 42 is Ask (VERIFY THIS ASSUMPTION)

            if actual_payload_len >= expected_payload_size:
                levels = []
                offset = 0 # Start after the header
                for _ in range(num_levels):
                    # Check if enough bytes remain
                    if offset + level_size > actual_payload_len:
                        logger.warning(f"Depth payload truncated for {symbol} at level {_}. Expected {expected_payload_size}, got {actual_payload_len}")
                        break
                    price, quantity, orders = struct.unpack_from(depth_level_format, payload, offset)
                    levels.append({"price": price, "quantity": quantity, "orders": orders})
                    offset += level_size

                # Determine if it's bid or ask based on code (needs confirmation)
                depth_type = "bids" if message_code == 41 else "asks" if message_code == 42 else "unknown"

                if depth_type != "unknown" and producer:
                    depth_payload_for_kafka = {
                        "timestamp": int(time.time()), # Use current time as depth packets might not have LTT
                        "symbol": symbol,
                        depth_type: levels # Store either 'bids' or 'asks'
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_DEPTH_DATA_TOPIC, value=depth_payload_for_kafka, key=symbol.encode('utf-8'))
                        # logger.debug(f"Sent {depth_type} data for {symbol} to Kafka topic '{KAFKA_DEPTH_DATA_TOPIC}'")
                    except Exception as kafka_err:
                        logger.error(f"Failed to send {depth_type} data for {symbol} to Kafka: {kafka_err}")
                elif not producer:
                    logger.warning(f"Kafka producer not available, skipping {depth_type} send.")

            else:
                logger.warning(f"Depth payload too short for {symbol} (Code {message_code}). Expected {expected_payload_size}, got {actual_payload_len}")


        elif message_code == RESPONSE_CODE_PREV_CLOSE:
            # Format: <prev_close (float32), prev_oi (int32)> = 8 bytes
            prev_close_format = '<fi'; expected_size = struct.calcsize(prev_close_format)
            if actual_payload_len >= expected_size:
                prev_close_price, prev_oi = struct.unpack_from(prev_close_format, payload, 0) # Use unpack_from
                # logger.debug(f"PrevClose Packet: Symbol={symbol}, PrevClose={prev_close_price:.2f}")
                security_id_to_prev_close_map[security_id] = prev_close_price # Store it
            else: logger.warning(f"PrevClose payload too short for {symbol}")

        elif message_code == RESPONSE_CODE_PING:
            # logger.debug("Received Ping from Dhan")
            pass # Handled by websockets library ping_interval/timeout

        elif message_code == RESPONSE_CODE_SERVER_DISCONNECT:
             disconnect_format = '<H'; reason_code = "N/A"
             if actual_payload_len >= struct.calcsize(disconnect_format):
                 reason_code = struct.unpack_from(disconnect_format, payload, 0)[0] # Use unpack_from
             logger.warning(f"Received Server Disconnect from Dhan for {symbol}. Reason: {reason_code}")
        # else:
             # logger.debug(f"Received unhandled message code {message_code} for {symbol}")

    except struct.error as e:
        logger.error(f"Struct error unpacking Dhan message (Code: {message_code if 'message_code' in locals() else 'N/A'}): {e}", exc_info=False)
    except Exception as e:
        logger.error(f"Error processing Dhan message: {e}", exc_info=True)

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
            await asyncio.sleep(0.1) # Small delay between requests
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
    """Manages subscriptions to specific data types for an instrument on the Dhan feed."""
    global dhan_subscribed_instruments
    async with subscription_lock:
        current_types = dhan_subscribed_instruments.get(security_id, set())
        action_taken = False
        # Map frontend request type ('ohlc') to Dhan data type ('QUOTE')
        # Add mappings for 'depth' if needed
        dhan_data_type = "QUOTE" if data_type == "ohlc" else data_type.upper()

        target_details = {"securityId": security_id, "exchangeSegment": exchange_segment, "dataType": dhan_data_type}
        requests_to_send = []

        if subscribe:
            if dhan_data_type not in current_types:
                logger.info(f"Requesting Dhan subscription: {security_id} - {dhan_data_type}")
                requests_to_send = await format_subscription_requests([target_details], subscribe=True)
                if await send_subscription_request(requests_to_send):
                    current_types.add(dhan_data_type); dhan_subscribed_instruments[security_id] = current_types; action_taken = True
                else: logger.error(f"Failed to send subscribe request for {security_id} - {dhan_data_type}")
            else:
                logger.debug(f"Already subscribed to {dhan_data_type} for {security_id}")
        else: # Unsubscribe
            if dhan_data_type in current_types:
                logger.info(f"Requesting Dhan unsubscription: {security_id} - {dhan_data_type}")
                requests_to_send = await format_subscription_requests([target_details], subscribe=False)
                if await send_subscription_request(requests_to_send):
                    current_types.discard(dhan_data_type) # Use discard to avoid KeyError
                    if not current_types:
                        if security_id in dhan_subscribed_instruments:
                            del dhan_subscribed_instruments[security_id]
                    else:
                        dhan_subscribed_instruments[security_id] = current_types
                    action_taken = True
                else: logger.error(f"Failed to send unsubscribe request for {security_id} - {dhan_data_type}")
            else:
                logger.debug(f"Not subscribed to {dhan_data_type} for {security_id}, cannot unsubscribe.")
        return action_taken

# --- Main Connection Logic ---
async def connect_dhan_websocket():
    """Establishes connection, handles messages, and manages initial subscriptions."""
    global dhan_websocket, dhan_subscribed_instruments, security_id_to_prev_close_map, kafka_producer
    client_id = settings.DHAN_CLIENT_ID; access_token = settings.DHAN_ACCESS_TOKEN
    if not access_token or access_token == "YOUR_DHAN_ACCESS_TOKEN_HERE" or not client_id:
        logger.error("Dhan Access Token or Client ID not configured."); await asyncio.sleep(60); return

    ws_url = DHAN_FEED_WS_URL_FORMAT.format(token=access_token, client_id=client_id)
    logger.info(f"Attempting to connect to Dhan WebSocket: {ws_url.split('token=')[0]}...")

    try:
        # Ensure Kafka producer is ready before connecting to Dhan WS
        producer = create_or_get_kafka_producer()
        if not producer:
            logger.error("Failed to initialize Kafka producer. Aborting Dhan WS connection attempt.")
            await asyncio.sleep(30) # Wait before retrying Kafka connection
            return

        # Connect to Dhan WebSocket
        async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10, close_timeout=10) as websocket:
            logger.info("Connected to Dhan WebSocket."); dhan_websocket = websocket

            # --- Subscribe to Nifty 50 Tickers + PrevClose initially ---
            nifty50_list = await get_nifty50_constituent_list() # Get enriched list from service
            initial_subs_details = []
            # Add Nifty 50 Index itself (Ticker + PrevClose)
            # Use correct segment code 'IDX' for Index
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
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=45) # Add timeout to recv
                    if isinstance(message, bytes):
                         await parse_dhan_message(message)
                    else:
                         logger.warning(f"Received non-bytes message: {type(message)}")
                except asyncio.TimeoutError:
                    logger.warning("Dhan WS recv timeout. Sending ping.")
                    try:
                        # Send explicit ping if timeout occurs (websockets library might handle this)
                        await websocket.ping()
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("Dhan WS connection closed while sending ping.")
                        break # Exit inner loop to trigger reconnect
                except websockets.exceptions.ConnectionClosedOK:
                    logger.info("Dhan WS connection closed normally.")
                    break
                except websockets.exceptions.ConnectionClosedError as close_err:
                    logger.error(f"Dhan WS connection closed with error: {close_err.code} {close_err.reason}")
                    break # Exit inner loop to trigger reconnect

    except Exception as e:
         if isinstance(e, websockets.exceptions.InvalidStatusCode) and e.status_code == 401: logger.error(f"Dhan WS connection failed: 401 Unauthorized. Check Token/ID.")
         elif isinstance(e, websockets.exceptions.WebSocketException): logger.error(f"Dhan WS exception: {e}")
         else: logger.error(f"Unhandled error in Dhan WS connection loop: {e}", exc_info=True)
    finally:
         logger.info("Cleaning up Dhan WebSocket connection state.")
         dhan_websocket = None; dhan_subscribed_instruments.clear(); security_id_to_prev_close_map.clear()
         # Close Kafka producer if it exists
         if kafka_producer:
             try:
                 logger.info("Closing Kafka producer for Dhan feed.")
                 kafka_producer.close(timeout=5)
             except Exception as kp_close_err:
                 logger.error(f"Error closing Kafka producer: {kp_close_err}")
             finally:
                 kafka_producer = None


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


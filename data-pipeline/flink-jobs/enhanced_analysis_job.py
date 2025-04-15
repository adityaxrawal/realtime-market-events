# data-pipeline/flink-jobs/enhanced_analysis_job.py
# Apache Flink Job using Python DataStream API for real-time financial analysis.
# (Refined version with clearer parameters and enhanced error handling)

import os
import logging
import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# --- Flink Setup ---
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Time, Types, Row
from pyflink.common.typeinfo import RowTypeInfo, BasicTypeInfo
from pyflink.datastream import (
    StreamExecutionEnvironment, TimeCharacteristic, KeyedProcessFunction,
    ProcessWindowFunction, CoProcessFunction, KeyedCoProcessFunction
)
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

# --- Data Science / NLP ---
import pandas as pd
import numpy as np
import nltk
try:
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
except LookupError:
    print("NLTK VADER lexicon not found. Attempting download...")
    try:
        nltk.download('vader_lexicon', quiet=True)
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        print("VADER lexicon downloaded successfully.")
    except Exception as download_err:
        print(f"ERROR: Failed to download NLTK VADER lexicon: {download_err}")
        print("Sentiment analysis will default to neutral.")
        SentimentIntensityAnalyzer = None # Will cause sentiment to be neutral

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Kafka Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:9092")
KAFKA_CONSUMER_GROUP = "flink_enhanced_analysis_group_v2" # Incremented group id
KAFKA_SOURCE_NEWS_TOPIC = "raw_news"
KAFKA_SOURCE_REDDIT_TOPIC = "raw_reddit_posts"
KAFKA_SOURCE_MARKET_DATA_TOPIC = "stock_market_data"
KAFKA_SOURCE_DEPTH_TOPIC = "market_depth_data"

# --- TimescaleDB Sink Configuration ---
DB_URL = os.getenv("FLINK_DB_URL", "jdbc:postgresql://timescaledb:5432/postgres")
DB_TABLE = os.getenv("FLINK_DB_TABLE", "market_analysis_results") # Target table
DB_USER = os.getenv("FLINK_DB_USER", "postgres")
DB_PASSWORD = os.getenv("FLINK_DB_PASSWORD", "your_strong_password") # Load securely

# --- Analysis Parameters (Tunable) ---
# Windowing
SENTIMENT_WINDOW_MINUTES = 60
MARKET_DATA_WINDOW_MINUTES = 15 # For volatility, trend
LIQUIDITY_WINDOW_SECONDS = 30
# State History Size
MARKET_DATA_HISTORY_SIZE = 30 # Increased size for more stable calculations
DEPTH_DATA_HISTORY_SIZE = 60 # Keep more depth snapshots
# Technical Indicator Periods
SMA_SHORT_PERIOD = 5
SMA_LONG_PERIOD = 20
VOLATILITY_PERIOD = 14
# Thresholds & Labels (TUNE THESE)
VOLATILITY_THRESHOLDS = [0.5, 1.5, 3.0, 5.0] # Example annualized std dev %
VOLATILITY_LABELS = ["Very Low", "Low", "Neutral", "High", "Very High"]
RISK_THRESHOLDS = VOLATILITY_THRESHOLDS # Using volatility as proxy
RISK_LABELS = VOLATILITY_LABELS
MOOD_THRESHOLDS = [-0.5, -0.1, 0.1, 0.5] # Example mood index score range (-1 to 1)
MOOD_LABELS = ["Very Low", "Low", "Neutral", "High", "Very High"]
LIQUIDITY_SPREAD_THRESHOLD_PCT = 0.5 # % spread increase to flag crunch
# Mood Index Weights
MOOD_SENTIMENT_WEIGHT = 0.6
MOOD_VOLATILITY_WEIGHT = 0.4
# Processing Control
MARKET_ANALYSIS_OUTPUT_INTERVAL_MS = 60 * 1000 # Emit market analysis max once per minute

# --- Helper Functions & Classes ---

# NLTK VADER setup
vader_analyzer = SentimentIntensityAnalyzer() if SentimentIntensityAnalyzer else None

def clean_text(text):
    """Basic text cleaning."""
    if not isinstance(text, str): return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'[^\w\s.,$!?\'`%/-]', '', text) # Keep symbols like $
    text = text.lower()
    text = re.sub(r'\s+', ' ', text).strip()
    return text

NIFTY50_SYMBOLS = set([ # Keep this updated or load dynamically
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO",
    "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL", "CIPLA", "COALINDIA",
    "DRREDDY", "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK",
    "INFY", "JSWSTEEL", "JIOFIN", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC",
    "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "TRENT", "ULTRACEMCO", "WIPRO", "NIFTY 50" # Include index if needed
])

def extract_symbols(text):
    """Extracts potential stock symbols ($XXX or known Nifty 50 names)."""
    if not isinstance(text, str): return []
    symbols = set()
    text_upper = text.upper()
    cashtags = re.findall(r'\$[A-Z0-9\-&]{1,10}', text_upper)
    for tag in cashtags:
        symbols.add(tag[1:])
    for nifty_symbol in NIFTY50_SYMBOLS:
        pattern = r'\b' + re.escape(nifty_symbol.upper()) + r'\b'
        if re.search(pattern, text_upper):
            symbols.add(nifty_symbol)
    return list(symbols) if symbols else []

def get_vader_sentiment_score(text):
    """Calculates VADER compound sentiment score."""
    if not vader_analyzer or not isinstance(text, str) or not text:
        return 0.0
    try:
        vs = vader_analyzer.polarity_scores(text)
        return vs['compound']
    except Exception as e:
        logger.warning(f"VADER sentiment analysis failed: {e} for text: {text[:100]}...")
        return 0.0

def map_score_to_sentiment_label(score):
    """Maps a compound score (-1 to 1) to a sentiment label."""
    if score is None: return "Neutral"
    try:
        score = float(score)
        if score >= 0.6: return "Very Bullish"
        elif score >= 0.1: return "Bullish"
        elif score > -0.1: return "Neutral"
        elif score > -0.6: return "Bearish"
        else: return "Very Bearish"
    except (ValueError, TypeError):
        return "Neutral"

def map_value_to_category(value, thresholds, labels, default_label="Neutral"):
    """Maps a numeric value to a category label based on thresholds."""
    if value is None: return default_label
    try:
        val = float(value)
        for i, threshold in enumerate(thresholds):
            if val <= threshold:
                return labels[i]
        return labels[-1]
    except (ValueError, TypeError):
        return default_label

# --- Define Output Row Type ---
result_field_names = [
    "time", "stock_symbol", "sentiment_label", "short_term_label",
    "volatility_label", "risk_label", "liquidity_crunch", "mood_index_label",
    "sentiment_score", "volatility_score", "risk_score", "mood_index_score"
]
result_field_types = [
    Types.SQL_TIMESTAMP(), BasicTypeInfo.STRING_TYPE_INFO(), BasicTypeInfo.STRING_TYPE_INFO(), BasicTypeInfo.STRING_TYPE_INFO(),
    BasicTypeInfo.STRING_TYPE_INFO(), BasicTypeInfo.STRING_TYPE_INFO(), BasicTypeInfo.BOOLEAN_TYPE_INFO(), BasicTypeInfo.STRING_TYPE_INFO(),
    BasicTypeInfo.DOUBLE_TYPE_INFO(), BasicTypeInfo.DOUBLE_TYPE_INFO(), BasicTypeInfo.DOUBLE_TYPE_INFO(), BasicTypeInfo.DOUBLE_TYPE_INFO()
]
result_type_info = RowTypeInfo(result_field_types, result_field_names)


# --- Flink Process Functions ---

class SentimentProcessor(KeyedProcessFunction):
    """Aggregates sentiment scores over a time window using event time timers."""

    def __init__(self, window_minutes=SENTIMENT_WINDOW_MINUTES):
        self.window_duration_ms = window_minutes * 60 * 1000
        self.sentiment_sum = None
        self.sentiment_count = None
        self.window_end_timer = None

    def open(self, runtime_context):
        sum_desc = ValueStateDescriptor("sentiment_sum", BasicTypeInfo.DOUBLE_TYPE_INFO())
        self.sentiment_sum = runtime_context.get_state(sum_desc)
        count_desc = ValueStateDescriptor("sentiment_count", BasicTypeInfo.INT_TYPE_INFO())
        self.sentiment_count = runtime_context.get_state(count_desc)
        timer_desc = ValueStateDescriptor("window_end_timer", BasicTypeInfo.LONG_TYPE_INFO())
        self.window_end_timer = runtime_context.get_state(timer_desc)
        logger.info(f"SentimentProcessor opened for key.")

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # value: tuple (stock_symbol, sentiment_score, timestamp_ms)
        symbol, score, timestamp_ms = value
        try:
            # Ensure score is float
            score = float(score)

            current_sum = self.sentiment_sum.value() or 0.0
            current_count = self.sentiment_count.value() or 0

            self.sentiment_sum.update(current_sum + score)
            self.sentiment_count.update(current_count + 1)

            # Register a timer for the end of the window if not already set
            # Aligns window end based on the first element arriving in that conceptual window
            current_timer = self.window_end_timer.value()
            if current_timer is None:
                # Calculate window end based on the event time
                window_end = (int(timestamp_ms / self.window_duration_ms) + 1) * self.window_duration_ms
                ctx.timer_service().register_event_time_timer(window_end)
                self.window_end_timer.update(window_end)
                # logger.debug(f"[{symbol}] Registered sentiment timer for {window_end}")

        except Exception as e:
            logger.error(f"Error processing sentiment element for {symbol}: {value} - {e}", exc_info=True)


    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext', out):
        symbol = ctx.get_current_key()
        try:
            # Timer fires when the window ends
            current_sum = self.sentiment_sum.value() or 0.0
            current_count = self.sentiment_count.value() or 0

            avg_score = current_sum / current_count if current_count > 0 else 0.0
            sentiment_label = map_score_to_sentiment_label(avg_score)

            # Output: (symbol, timestamp_ms_window_end, avg_score, label)
            out.collect(Row(symbol, timestamp, avg_score, sentiment_label))
            # logger.debug(f"[{symbol}] Emitted sentiment timer for {timestamp}: Score={avg_score:.4f}, Count={current_count}")

        except Exception as e:
            logger.error(f"Error in sentiment on_timer for {symbol} at {timestamp}: {e}", exc_info=True)
        finally:
            # Clean up state for the next window
            self.sentiment_sum.clear()
            self.sentiment_count.clear()
            self.window_end_timer.clear()


class MarketAnalysisProcessor(KeyedProcessFunction):
    """Calculates Trend, Volatility, Risk, Liquidity based on market/depth data."""

    def __init__(self, history_size=MARKET_DATA_HISTORY_SIZE,
                 sma_short=SMA_SHORT_PERIOD, sma_long=SMA_LONG_PERIOD,
                 vol_period=VOLATILITY_PERIOD,
                 liquidity_window_sec=LIQUIDITY_WINDOW_SECONDS,
                 spread_threshold=LIQUIDITY_SPREAD_THRESHOLD_PCT,
                 output_interval_ms=MARKET_ANALYSIS_OUTPUT_INTERVAL_MS):
        self.history_size = history_size
        self.sma_short = sma_short
        self.sma_long = sma_long
        self.vol_period = vol_period
        self.liquidity_window_ms = liquidity_window_sec * 1000
        self.spread_threshold = spread_threshold
        self.output_interval_ms = output_interval_ms

        # State descriptors
        self.prices = None
        self.depths = None
        self.last_analysis_time = None

    def open(self, runtime_context):
        price_state_desc = ListStateDescriptor("prices", Types.TUPLE([Types.LONG(), Types.DOUBLE()]))
        self.prices = runtime_context.get_list_state(price_state_desc)
        depth_state_desc = ListStateDescriptor("depths", Types.TUPLE([Types.LONG(), Types.DOUBLE(), Types.DOUBLE()]))
        self.depths = runtime_context.get_list_state(depth_state_desc)
        last_time_desc = ValueStateDescriptor("last_analysis_time", BasicTypeInfo.LONG_TYPE_INFO())
        self.last_analysis_time = runtime_context.get_state(last_time_desc)
        logger.info(f"MarketAnalysisProcessor opened for key.")


    def add_to_list_state(self, state: ListStateDescriptor, value, max_size):
        """Helper to add to ListState and keep it trimmed."""
        items = list(state.get()) if state.get() is not None else []
        # Avoid adding duplicates (based on timestamp) - simple check
        if not items or items[-1][0] != value[0]:
             items.append(value)
        # Keep only the most recent max_size items, sorted by timestamp (first element)
        items.sort(key=lambda x: x[0]) # Sort oldest to newest
        updated_items = items[-max_size:] # Keep the tail (most recent)
        state.update(updated_items)
        return updated_items

    def calculate_analysis(self, symbol, current_timestamp_ms):
        """Performs the market data calculations. Returns a dictionary."""
        analysis_results = {
            "symbol": symbol,
            "timestamp_ms": current_timestamp_ms,
            "short_term_label": "Neutral",
            "volatility_score": None,
            "volatility_label": "Neutral",
            "risk_score": None, # Using volatility score as proxy
            "risk_label": "Neutral",
            "liquidity_crunch": False
        }

        # --- Price-based calculations ---
        try:
            price_history = list(self.prices.get()) if self.prices.get() is not None else []
            if not price_history: return analysis_results # Not enough data

            # Ensure sorted by timestamp
            price_history.sort(key=lambda x: x[0])

            # Need at least `vol_period` points for volatility
            if len(price_history) >= self.vol_period:
                df = pd.DataFrame(price_history, columns=['timestamp', 'ltp'])
                # Ensure ltp is numeric, drop NaNs introduced by shift/log
                df['ltp'] = pd.to_numeric(df['ltp'], errors='coerce')
                df.dropna(subset=['ltp'], inplace=True)

                if len(df) >= self.vol_period: # Check again after dropna
                    df['log_return'] = np.log(df['ltp'] / df['ltp'].shift(1))
                    df.dropna(subset=['log_return'], inplace=True)

                    if not df['log_return'].empty:
                        # Use rolling std dev over the volatility period
                        rolling_std = df['log_return'].rolling(window=self.vol_period, min_periods=max(2, self.vol_period // 2)).std().iloc[-1]

                        if pd.notna(rolling_std):
                            # Example: Annualize assuming 252 trading days (adjust if data is intraday)
                            # If data is per minute, sqrt(252 * 6.5 * 60) might be more appropriate (approx trading minutes/year)
                            # Using sqrt(252) here for simplicity, assuming near-daily changes dominate state
                            annualized_vol = rolling_std * np.sqrt(252) * 100 # As percentage
                            analysis_results["volatility_score"] = annualized_vol
                            analysis_results["volatility_label"] = map_value_to_category(
                                annualized_vol, VOLATILITY_THRESHOLDS, VOLATILITY_LABELS
                            )
                            # Risk (using volatility as proxy)
                            analysis_results["risk_score"] = annualized_vol
                            analysis_results["risk_label"] = map_value_to_category(
                                annualized_vol, RISK_THRESHOLDS, RISK_LABELS
                            )

                # Short-term Trend (SMA Crossover)
                if len(df) >= self.sma_long:
                    sma_short_series = df['ltp'].rolling(window=self.sma_short).mean()
                    sma_long_series = df['ltp'].rolling(window=self.sma_long).mean()
                    if not sma_short_series.empty and not sma_long_series.empty:
                        sma_short = sma_short_series.iloc[-1]
                        sma_long = sma_long_series.iloc[-1]
                        if pd.notna(sma_short) and pd.notna(sma_long):
                            if sma_short > sma_long:
                                analysis_results["short_term_label"] = "Bullish"
                            elif sma_short < sma_long:
                                analysis_results["short_term_label"] = "Bearish"
                            # else remains "Neutral"

        except Exception as e:
            logger.warning(f"Price analysis calculation failed for {symbol}: {e}", exc_info=False) # Avoid flooding logs

        # --- Liquidity Crunch Calculation ---
        try:
            depth_history = list(self.depths.get()) if self.depths.get() is not None else []
            if not depth_history: return analysis_results # Not enough data

            depth_history.sort(key=lambda x: x[0])
            recent_depths = [d for d in depth_history if current_timestamp_ms - d[0] <= self.liquidity_window_ms]

            if len(recent_depths) >= 1: # Need at least the latest depth
                latest_ts, latest_bid, latest_ask = recent_depths[-1]

                if latest_bid is not None and latest_ask is not None and latest_bid > 0 and latest_ask > 0:
                    mid_price = (latest_bid + latest_ask) / 2
                    spread_pct = ((latest_ask - latest_bid) / mid_price) * 100 if mid_price > 0 else 0

                    # Simple check: Spread exceeds threshold
                    if spread_pct > self.spread_threshold:
                        analysis_results["liquidity_crunch"] = True
                        # TODO: Add more sophisticated checks (e.g., compare to historical avg spread, check depth quantity)
        except Exception as e:
            logger.warning(f"Liquidity calculation failed for {symbol}: {e}", exc_info=False)

        return analysis_results

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out):
        # value: tuple (type, symbol, timestamp_ms, data_dict)
        element_type, symbol, timestamp_ms, data = value
        try:
            if element_type == 'market':
                ltp = data.get('ltp')
                if ltp is not None:
                    try:
                        self.add_to_list_state(self.prices, (timestamp_ms, float(ltp)), self.history_size)
                    except (ValueError, TypeError) as conv_err:
                         logger.warning(f"Could not convert ltp {ltp} to float for {symbol}: {conv_err}")
                         return # Skip processing if ltp is invalid
                else:
                    # logger.debug(f"Market data for {symbol} missing 'ltp'.")
                    return # Cannot calculate without ltp

            elif element_type == 'depth':
                # Assuming data contains 'bids': [{'price': p, ...}, ...], 'asks': [...]
                best_bid = data.get('bids', [{}])[0].get('price') if data.get('bids') else None
                best_ask = data.get('asks', [{}])[0].get('price') if data.get('asks') else None
                if best_bid is not None and best_ask is not None:
                    try:
                        self.add_to_list_state(self.depths, (timestamp_ms, float(best_bid), float(best_ask)), self.history_size * 2)
                    except (ValueError, TypeError) as conv_err:
                         logger.warning(f"Could not convert bid/ask {best_bid}/{best_ask} to float for {symbol}: {conv_err}")
                         return # Skip processing if depth is invalid
                else:
                    # logger.debug(f"Depth data for {symbol} missing best bid/ask.")
                    return # Cannot calculate without depth

            # Throttle output using processing time timer
            last_output_time = self.last_analysis_time.value() or 0
            if timestamp_ms - last_output_time >= self.output_interval_ms:
                analysis = self.calculate_analysis(symbol, timestamp_ms)
                if analysis:
                    # Output: (symbol, timestamp_ms, short_term_label, vol_score, vol_label, risk_score, risk_label, liquidity_crunch)
                    out.collect(Row(
                        analysis["symbol"],
                        analysis["timestamp_ms"],
                        analysis["short_term_label"],
                        analysis["volatility_score"],
                        analysis["volatility_label"],
                        analysis["risk_score"],
                        analysis["risk_label"],
                        analysis["liquidity_crunch"]
                    ))
                    self.last_analysis_time.update(timestamp_ms)

        except Exception as e:
            logger.error(f"Error processing market/depth element for {symbol}: {value} - {e}", exc_info=True)


class FinalAnalysisCombiner(KeyedCoProcessFunction):
    """Combines Sentiment and Market Analysis results and calculates Mood Index."""

    def __init__(self, mood_sentiment_weight=MOOD_SENTIMENT_WEIGHT, mood_volatility_weight=MOOD_VOLATILITY_WEIGHT):
        self.mood_sentiment_weight = mood_sentiment_weight
        self.mood_volatility_weight = mood_volatility_weight
        self.latest_sentiment = None
        self.latest_market_analysis = None

    def open(self, runtime_context):
        # Define state descriptors with explicit TypeInformation
        sentiment_type_info = Types.ROW([Types.LONG(), Types.DOUBLE(), Types.STRING()])
        sentiment_desc = ValueStateDescriptor("latest_sentiment", sentiment_type_info)
        self.latest_sentiment = runtime_context.get_state(sentiment_desc)

        market_analysis_type_info = Types.ROW([
            Types.LONG(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
            Types.DOUBLE(), Types.STRING(), Types.BOOLEAN()
        ])
        market_analysis_desc = ValueStateDescriptor("latest_market_analysis", market_analysis_type_info)
        self.latest_market_analysis = runtime_context.get_state(market_analysis_desc)
        logger.info(f"FinalAnalysisCombiner opened for key.")


    def process_element1(self, value, ctx: 'KeyedCoProcessFunction.Context', out):
        # Input 1: Sentiment results Row(symbol, timestamp_ms, avg_score, label)
        symbol, timestamp_ms, avg_score, label = value
        self.latest_sentiment.update(Row(timestamp_ms, avg_score, label))
        self.combine_and_emit(symbol, ctx, out)

    def process_element2(self, value, ctx: 'KeyedCoProcessFunction.Context', out):
        # Input 2: Market analysis results Row(symbol, timestamp_ms, short_term, vol_score, vol_label, risk_score, risk_label, liquidity)
        symbol, timestamp_ms, short_term, vol_score, vol_label, risk_score, risk_label, liquidity = value
        self.latest_market_analysis.update(Row(timestamp_ms, short_term, vol_score, vol_label, risk_score, risk_label, liquidity))
        self.combine_and_emit(symbol, ctx, out)

    def combine_and_emit(self, symbol, ctx, out):
        """Calculates final mood index and emits combined result if both inputs are present."""
        try:
            sentiment = self.latest_sentiment.value()
            market = self.latest_market_analysis.value()

            # Only emit if both states have values
            if sentiment and market:
                # Determine the latest timestamp between the two inputs for the output row
                latest_ts_ms = max(sentiment[0], market[0])
                # Convert to timezone-aware datetime for SQL TIMESTAMP WITH TIME ZONE
                output_time = datetime.fromtimestamp(latest_ts_ms / 1000, tz=timezone.utc)

                # Extract values from state Rows
                sentiment_score = sentiment[1]
                sentiment_label = sentiment[2]
                short_term_label = market[1]
                volatility_score = market[2] # Note: index adjusted based on MarketAnalysisProcessor output
                volatility_label = market[3]
                risk_score = market[4]
                risk_label = market[5]
                liquidity_crunch = market[6]

                # --- Calculate Mood Index Score ---
                mood_index_score = 0.0 # Default to neutral
                normalized_vol_score = 0.0

                # Normalize volatility score (example: map 0-10% ann. vol linearly to +1 to -1)
                # High volatility -> lower (more fearful) mood score contribution
                if volatility_score is not None:
                    try:
                        # Clamp volatility score before normalization (e.g., 0 to 20%)
                        clamped_vol = max(0.0, min(20.0, float(volatility_score)))
                        normalized_vol_score = 1.0 - (clamped_vol / 10.0) # Maps 0->1, 10->0, 20->-1
                    except (ValueError, TypeError):
                        normalized_vol_score = 0.0 # Neutral if conversion fails

                # Use sentiment score directly (already -1 to 1)
                safe_sentiment_score = 0.0
                if sentiment_score is not None:
                    try:
                       safe_sentiment_score = max(-1.0, min(1.0, float(sentiment_score)))
                    except (ValueError, TypeError):
                       safe_sentiment_score = 0.0 # Neutral if conversion fails

                mood_index_score = (safe_sentiment_score * self.mood_sentiment_weight) + \
                                   (normalized_vol_score * self.mood_volatility_weight)
                mood_index_label = map_value_to_category(mood_index_score, MOOD_THRESHOLDS, MOOD_LABELS)

                # Round scores for output consistency
                sentiment_score_out = round(safe_sentiment_score, 4) if safe_sentiment_score is not None else None
                volatility_score_out = round(volatility_score, 4) if volatility_score is not None else None
                risk_score_out = round(risk_score, 4) if risk_score is not None else None # Using volatility score as proxy
                mood_index_score_out = round(mood_index_score, 4)

                # Emit final combined row matching the DB schema defined in result_type_info
                output_row = Row(
                    output_time,          # time (TIMESTAMPTZ)
                    symbol,               # stock_symbol
                    sentiment_label,      # sentiment_label
                    short_term_label,     # short_term_label
                    volatility_label,     # volatility_label
                    risk_label,           # risk_label
                    liquidity_crunch,     # liquidity_crunch
                    mood_index_label,     # mood_index_label
                    sentiment_score_out,  # sentiment_score
                    volatility_score_out, # volatility_score
                    risk_score_out,       # risk_score
                    mood_index_score_out  # mood_index_score
                )
                out.collect(output_row)
                # logger.debug(f"[{symbol}] Emitted combined analysis at {output_time}")

                # Optional: Clear state after emitting to only emit on new pairs.
                # If commented out, it emits whenever either input arrives and the other exists.
                # self.latest_sentiment.clear()
                # self.latest_market_analysis.clear()

        except Exception as e:
             logger.error(f"Error in FinalAnalysisCombiner for {symbol}: {e}", exc_info=True)


# --- Main Flink Job ---
def run_enhanced_analysis_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    # Configure checkpointing via environment variables or Flink config file is recommended
    # env.enable_checkpointing(60000)
    # env.get_checkpoint_config().set_checkpoint_storage("file:///opt/flink/checkpoints") # Example

    # --- Define Kafka Sources ---
    # Watermark Strategy for Market/Depth Data (Timestamps are likely epoch seconds)
    market_watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Time.seconds(10)) \
        .with_timestamp_assigner(lambda event, ts: event.get('timestamp', int(time.time())) * 1000) # Use event timestamp, fallback to processing time

    # Watermark Strategy for News/Reddit (Timestamps might be less frequent/reliable)
    text_watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Time.minutes(2)) \
        .with_timestamp_assigner(lambda event, ts: event.get('timestamp', int(time.time())) * 1000) # Use event timestamp, fallback

    # Define Kafka Consumer properties
    kafka_props = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_CONSUMER_GROUP,
        # Add other relevant Kafka consumer properties if needed
        # 'auto.offset.reset': 'latest' # Or 'earliest'
    }

    # 1. News Source
    news_consumer = FlinkKafkaConsumer(
        topics=KAFKA_SOURCE_NEWS_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={**kafka_props, 'group.id': KAFKA_CONSUMER_GROUP + "_news"}
    )
    news_stream_raw = env.add_source(news_consumer).name("KafkaNewsSource")
    news_stream = news_stream_raw \
        .map(lambda msg: json.loads(msg), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .name("ParseNewsJson") \
        .assign_timestamps_and_watermarks(text_watermark_strategy)

    # 2. Reddit Source
    reddit_consumer = FlinkKafkaConsumer(
        topics=KAFKA_SOURCE_REDDIT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={**kafka_props, 'group.id': KAFKA_CONSUMER_GROUP + "_reddit"}
    )
    reddit_stream_raw = env.add_source(reddit_consumer).name("KafkaRedditSource")
    reddit_stream = reddit_stream_raw \
        .map(lambda msg: json.loads(msg), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .name("ParseRedditJson") \
        .assign_timestamps_and_watermarks(text_watermark_strategy)

    # 3. Market Data Source (Quotes/Ticks from Dhan)
    market_data_consumer = FlinkKafkaConsumer(
        topics=KAFKA_SOURCE_MARKET_DATA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={**kafka_props, 'group.id': KAFKA_CONSUMER_GROUP + "_market"}
    )
    market_data_stream_raw = env.add_source(market_data_consumer).name("KafkaMarketDataSource")
    market_data_stream = market_data_stream_raw \
        .map(lambda msg: json.loads(msg), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())) \
        .name("ParseMarketDataJson") \
        .assign_timestamps_and_watermarks(market_watermark_strategy) \
        .map(lambda x: Row(x['symbol'], x['timestamp'] * 1000 if 'timestamp' in x else int(time.time()*1000), x), # Add timestamp_ms
             output_type=Types.ROW([Types.STRING(), Types.LONG(), Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())]))

    # 4. Market Depth Source (from Dhan)
    depth_consumer = FlinkKafkaConsumer(
        topics=KAFKA_SOURCE_DEPTH_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={**kafka_props, 'group.id': KAFKA_CONSUMER_GROUP + "_depth"}
    )
    depth_stream_raw = env.add_source(depth_consumer).name("KafkaDepthSource")
    depth_stream = depth_stream_raw \
        .map(lambda msg: json.loads(msg), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())) \
        .name("ParseDepthJson") \
        .assign_timestamps_and_watermarks(market_watermark_strategy) \
        .map(lambda x: Row(x['symbol'], x['timestamp'] * 1000 if 'timestamp' in x else int(time.time()*1000), x), # Add timestamp_ms
             output_type=Types.ROW([Types.STRING(), Types.LONG(), Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())]))


    # --- Process Sentiment Stream ---
    sentiment_input_stream = news_stream.union(reddit_stream) \
        .map(lambda x: { # Ensure consistent structure
            'text': clean_text(x.get('headline', x.get('title', '')) + " " + x.get('summary', x.get('text', ''))),
            'timestamp': x.get('timestamp', int(time.time())) # Epoch seconds
        }).name("ExtractTextAndTimestamp") \
        .filter(lambda x: x['text']) \
        .flat_map(lambda x: [
            (symbol, get_vader_sentiment_score(x['text']), x['timestamp'] * 1000) # symbol, score, timestamp_ms
            for symbol in extract_symbols(x['text'])
        ]).name("CalculateSentimentAndExtractSymbols") \
        .filter(lambda x: x[0] is not None and x[1] is not None).name("FilterValidSentiment")

    # Key by symbol and process sentiment using event time timers
    processed_sentiment = sentiment_input_stream \
        .key_by(lambda x: x[0]) \
        .process(SentimentProcessor(window_minutes=SENTIMENT_WINDOW_MINUTES)) \
        .name("AggregateSentiment") \
        .returns(Types.ROW([Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.STRING()])) # symbol, timestamp_ms, avg_score, label

    # --- Process Market Data and Depth Streams ---
    market_input_stream = market_data_stream.map(
        lambda x: ('market', x[0], x[1], x[2]), # type, symbol, timestamp_ms, data_dict
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())])
    ).name("FormatMarketData")

    depth_input_stream = depth_stream.map(
        lambda x: ('depth', x[0], x[1], x[2]), # type, symbol, timestamp_ms, data_dict
         output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())])
    ).name("FormatDepthData")

    combined_market_stream = market_input_stream.union(depth_input_stream)

    processed_market_analysis = combined_market_stream \
        .key_by(lambda x: x[1]) \
        .process(MarketAnalysisProcessor(
            history_size=MARKET_DATA_HISTORY_SIZE,
            sma_short=SMA_SHORT_PERIOD, sma_long=SMA_LONG_PERIOD,
            vol_period=VOLATILITY_PERIOD,
            liquidity_window_sec=LIQUIDITY_WINDOW_SECONDS,
            spread_threshold=LIQUIDITY_SPREAD_THRESHOLD_PCT,
            output_interval_ms=MARKET_ANALYSIS_OUTPUT_INTERVAL_MS
        )) \
        .name("CalculateMarketAnalysis") \
        .returns(Types.ROW([ # Output type matching MarketAnalysisProcessor output
            Types.STRING(), Types.LONG(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
            Types.DOUBLE(), Types.STRING(), Types.BOOLEAN()
        ])) # symbol, timestamp_ms, short_term, vol_score, vol_label, risk_score, risk_label, liquidity

    # --- Combine Sentiment and Market Analysis ---
    # Connect the two processed streams keyed by stock symbol
    final_analysis_stream = processed_sentiment \
        .connect(processed_market_analysis) \
        .key_by(lambda s: s[0], lambda m: m[0]) \
        .process(FinalAnalysisCombiner(
            mood_sentiment_weight=MOOD_SENTIMENT_WEIGHT,
            mood_volatility_weight=MOOD_VOLATILITY_WEIGHT
        )) \
        .name("CombineAnalysisAndMood") \
        .returns(result_type_info) # Use the defined output type info for the final combined row

    # --- Define JDBC Sink ---
    # Construct the SQL INSERT statement dynamically based on field names
    column_names = ', '.join(f'"{col}"' for col in result_field_names) # Quote column names
    placeholders = ', '.join(['?'] * len(result_field_names))
    sql_insert = f"INSERT INTO {DB_TABLE} ({column_names}) VALUES ({placeholders})"
    logger.info(f"Using SQL Sink Statement: {sql_insert}")

    jdbc_sink = JdbcSink.sink(
        sql_insert,
        result_type_info, # Use the defined TypeInformation for the final Row
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(DB_URL)
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(DB_USER)
            .with_password(DB_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(100) # Adjust batch size as needed
            .with_batch_interval_ms(2000) # Adjust interval as needed
            .with_max_retries(5) # Retry on failure
            .build()
    )

    # Add the sink to the final stream
    final_analysis_stream.add_sink(jdbc_sink).name("TimescaleDBSink")

    # --- Execute Job ---
    logger.info("Executing Flink Enhanced Analysis Job...")
    # Give the job a name for monitoring in the Flink UI
    env.execute("Enhanced Financial Analysis Job")

if __name__ == '__main__':
    run_enhanced_analysis_job()


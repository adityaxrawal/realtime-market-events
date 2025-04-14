# data-pipeline/flink-jobs/sentiment_analysis_job.py
# Apache Flink Job using Python Table API for real-time sentiment analysis.
# --- VERSION 4: Trying alternative filter syntax for linter ---

import os
import logging
import json
import re
from datetime import datetime

# --- Flink Setup ---
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema
from pyflink.table.udf import udf
# Import expressions for Table API method chaining
from pyflink.table.expressions import col, lit, call, coalesce, upper

# --- NLP Setup (NLTK VADER) ---
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
        SentimentIntensityAnalyzer = None

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:9092")
KAFKA_SOURCE_TOPICS = os.getenv("KAFKA_SOURCE_TOPICS", "raw_news,raw_reddit_posts")
KAFKA_CONSUMER_GROUP = "flink_sentiment_consumer_group_v2"

DB_URL = os.getenv("FLINK_DB_URL", "jdbc:postgresql://timescaledb:5432/postgres")
DB_TABLE = os.getenv("FLINK_DB_TABLE", "stock_sentiment")
DB_USER = os.getenv("FLINK_DB_USER", "postgres")
DB_PASSWORD = os.getenv("FLINK_DB_PASSWORD", "your_strong_password") # CHANGE or load securely

# --- Text Cleaning and Symbol Extraction (Functions remain the same) ---
def clean_text(text):
    """Basic text cleaning: remove URLs, mentions, hashtags, special chars, lowercase."""
    if not isinstance(text, str): return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'[^\w\s.,$!?\'`%/-]', '', text)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text).strip()
    return text

NIFTY50_SYMBOLS = set([
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO",
    "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL", "CIPLA", "COALINDIA",
    "DRREDDY", "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK",
    "INFY", "JSWSTEEL", "JIOFIN", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC",
    "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "TRENT", "ULTRACEMCO", "WIPRO", "NIFTY 50"
])

def extract_symbols(text):
    """Extracts potential stock symbols ($XXX format or known Nifty 50 names)."""
    if not isinstance(text, str): return []
    symbols = set()
    text_upper = text.upper()
    cashtags = re.findall(r'\$[A-Z]{1,10}', text_upper)
    for tag in cashtags:
        symbols.add(tag[1:])
    for nifty_symbol in NIFTY50_SYMBOLS:
        pattern = r'\b' + re.escape(nifty_symbol) + r'\b'
        if re.search(pattern, text_upper):
            symbols.add(nifty_symbol)
    return list(symbols) if symbols else []

# --- Sentiment Analysis UDF (Remains the same) ---
vader_analyzer = SentimentIntensityAnalyzer() if SentimentIntensityAnalyzer else None
@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("compound", DataTypes.DOUBLE()),
    DataTypes.FIELD("label", DataTypes.STRING())
]))
def get_vader_sentiment(text):
    """Calculates sentiment using VADER."""
    if not vader_analyzer or not isinstance(text, str) or not text:
        return {"compound": 0.0, "label": "neutral"}
    try:
        vs = vader_analyzer.polarity_scores(text)
        compound_score = vs['compound']
        if compound_score >= 0.05: label = "positive"
        elif compound_score <= -0.05: label = "negative"
        else: label = "neutral"
        return {"compound": compound_score, "label": label}
    except Exception as e:
        print(f"ERROR: VADER sentiment analysis failed: {e} for text: {text[:100]}...")
        return {"compound": 0.0, "label": "neutral"}

# --- Flink Job Execution ---
def run_sentiment_analysis_job():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    # 1. Define Kafka Source Table (Same as before)
    source_ddl = f"""
        CREATE TABLE kafka_raw_input (
            `id` STRING, `source` STRING, `timestamp` BIGINT, `headline` STRING,
            `summary` STRING, `title` STRING, `text` STRING, `url` STRING,
            `fetch_time` STRING,
            `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 0),
            WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka', 'topic' = '{KAFKA_SOURCE_TOPICS}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = '{KAFKA_CONSUMER_GROUP}',
            'scan.startup.mode' = 'latest-offset', 'format' = 'json',
            'json.fail-on-missing-field' = 'false', 'json.ignore-parse-errors' = 'true'
        )"""
    t_env.execute_sql(source_ddl)
    logger.info(f"Kafka source table 'kafka_raw_input' created for topics: {KAFKA_SOURCE_TOPICS}")

    # 2. Register UDFs (Same as before)
    t_env.create_temporary_function("get_sentiment", get_vader_sentiment)
    t_env.create_temporary_function("clean_text", udf(clean_text, result_type=DataTypes.STRING()))
    t_env.create_temporary_function("extract_symbols", udf(extract_symbols, result_type=DataTypes.ARRAY(DataTypes.STRING())))
    logger.info("UDFs registered.")

    # 3. Define Processing Logic with Relevance Check
    processed_table = t_env.from_path("kafka_raw_input") \
        .add_columns(call('clean_text', coalesce(col('headline'), col('title'), lit('')) + lit(' ') + coalesce(col('summary'), col('text'), lit(''))).alias("full_cleaned_text")) \
        .add_columns(call('extract_symbols', col('full_cleaned_text')).alias("extracted_symbols")) \
        .filter(col('extracted_symbols').is_not_null() & (call("CARDINALITY", col('extracted_symbols')) > 0)) \
        .add_columns(call('get_sentiment', col('full_cleaned_text')).alias("sentiment_result")) \
        .select(
            col('event_time'), col('source'), col('id').alias('source_id'), col('url'),
            coalesce(col('headline'), col('title')).alias('headline_or_title'),
            coalesce(col('summary'), col('text')).alias('summary_or_text'),
            col('extracted_symbols'),
            col('sentiment_result')['compound'].alias('sentiment_score'),
            col('sentiment_result')['label'].alias('sentiment_label')
        ) \
        .flat_map(lambda row: [(symbol, row) for symbol in row['extracted_symbols']]) \
        .alias("stock_symbol, original_row") \
        .select(
            col('original_row')['event_time'].alias('time'), col('stock_symbol'),
            col('original_row')['source'].alias('source'),
            col('original_row')['sentiment_score'].alias('sentiment_score'),
            col('original_row')['sentiment_label'].alias('sentiment_label'),
            col('original_row')['headline_or_title'].alias('headline'),
            col('original_row')['summary_or_text'].alias('summary'),
            col('original_row')['source_id'].alias('source_id'),
            col('original_row')['url'].alias('url')
        ) \
        .filter( # *** REVISED RELEVANCE FILTER using Method Chaining ***
            col('headline').is_null()
            .Or(
                col('headline').is_not_null()
                .And(
                    # Check if the symbol appears as a whole word or cashtag in the headline
                    upper(col('headline')).like(concat(lit('%'), upper(col('stock_symbol')), lit('%')))
                    .Or(upper(col('headline')).like(concat(lit('%$'), upper(col('stock_symbol')), lit('%'))))
                    # Note: Using concat explicitly, though '+' might also work depending on Flink version/planner
                )
            )
        )

    logger.info("Processing logic with revised relevance filter defined.")

    # 4. Define JDBC Sink Table (Same as before)
    sink_ddl = f"""
        CREATE TABLE jdbc_sentiment_sink (
            `time` TIMESTAMP(3) WITH LOCAL TIME ZONE, `stock_symbol` VARCHAR(20),
            `source` VARCHAR(50), `sentiment_score` DOUBLE, `sentiment_label` VARCHAR(10),
            `headline` TEXT, `summary` TEXT, `source_id` VARCHAR(255), `url` TEXT
        ) WITH (
            'connector' = 'jdbc', 'url' = '{DB_URL}', 'table-name' = '{DB_TABLE}',
            'username' = '{DB_USER}', 'password' = '{DB_PASSWORD}',
            'driver' = 'org.postgresql.Driver', 'sink.buffer-flush.max-rows' = '100',
            'sink.buffer-flush.interval' = '2s'
        )"""
    t_env.execute_sql(sink_ddl)
    logger.info(f"JDBC sink table 'jdbc_sentiment_sink' created for table '{DB_TABLE}'.")

    # 5. Insert Filtered Data into JDBC Sink
    statement_set = t_env.create_statement_set()
    statement_set.add_insert("jdbc_sentiment_sink", processed_table)

    logger.info("Executing Flink job statement set...")
    try:
        table_result = statement_set.execute()
        logger.info("Flink job submitted successfully.")
        job_client = table_result.get_job_client()
        if job_client: logger.info(f"Job Client available. Status: {job_client.get_job_status()}")
        else: logger.warning("Could not get Job Client after submission.")
    except Exception as e:
        logger.error(f"Error submitting or executing Flink job: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    logger.info("Starting Flink Sentiment Analysis Job (v4 - revised filter syntax)...")
    run_sentiment_analysis_job()
    logger.info("Flink job submission process finished.")

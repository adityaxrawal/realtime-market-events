import os
import time
import logging
import json
import requests # Library for making HTTP requests
from kafka import KafkaProducer # Kafka client library
from kafka.errors import NoBrokersAvailable, KafkaError # Kafka error types
from dotenv import load_dotenv # Library to load environment variables from .env file
from datetime import datetime, timedelta, timezone # For handling dates and times

# --- Configuration ---

# Load environment variables from a .env file in the same directory.
# This is useful for local development but less critical when running in Docker
# where variables are typically passed via docker-compose.
load_dotenv()

# Configure logging to output informational messages and errors.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Get Configuration from Environment Variables ---
# Fetch News API key, providing a default placeholder if not set.
# IMPORTANT: Replace 'YOUR_NEWS_API_KEY_HERE' in your .env file or environment.
NEWS_API_KEY = os.getenv('NEWS_API_KEY', 'YOUR_NEWS_API_KEY_HERE')

# Kafka broker address. 'kafka:9092' is the typical service name and port within Docker Compose network.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

# Kafka topic where raw news articles will be published.
KAFKA_TOPIC_NEWS = os.getenv('KAFKA_TOPIC_NEWS', 'raw_news')

# How often (in seconds) to poll the News API. Default is 900 seconds (15 minutes).
FETCH_INTERVAL_SECONDS = int(os.getenv('NEWS_FETCH_INTERVAL_SECONDS', 900))

# NewsAPI endpoint URL. '/everything' allows searching all articles.
NEWS_API_ENDPOINT = "https://newsapi.org/v2/everything"

# --- Search Parameters ---
# Define keywords relevant to the financial analysis. Customize this list extensively.
# Using specific company names, indices, and financial terms improves relevance.
SEARCH_KEYWORDS = [
    "Nifty 50", "Sensex", "Indian stock market", "RBI", "SEBI",
    "Reliance Industries", "TCS", "HDFC Bank", "Infosys", "ICICI Bank",
    "stock alert", "earnings report India", "IPO India", "market volatility",
    "interest rates India", "inflation India",
    # Add more specific keywords based on the stocks/sectors you are tracking
]
# Combine keywords into a query string suitable for NewsAPI (using OR logic).
# Using quotes around keywords can help match exact phrases.
NEWS_API_QUERY = " OR ".join(f'"{keyword}"' for keyword in SEARCH_KEYWORDS)

# File to store the state (timestamp of the last fetched article)
STATE_FILE = 'news_producer_state.json'

# --- Helper Functions ---

def load_last_timestamp():
    """
    Loads the timestamp of the most recently successfully processed article from the state file.
    Returns a timezone-aware datetime object (UTC).
    If the file doesn't exist or is invalid, defaults to 1 hour ago.
    """
    try:
        # Open the state file for reading
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            # Get the timestamp string from the state
            ts_str = state.get('last_timestamp_utc')
            if ts_str:
                # Parse the ISO format string back into a datetime object.
                # Ensure it's treated as UTC (replace Z with +00:00 offset).
                return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except FileNotFoundError:
        # If the file doesn't exist, log info and return a default time
        logger.info(f"State file '{STATE_FILE}' not found. Will fetch news from the last hour.")
    except (json.JSONDecodeError, Exception) as e:
        # Log errors during file reading or parsing
        logger.error(f"Error loading state from '{STATE_FILE}': {e}")
    # Default: If state can't be loaded, fetch news from the last hour to avoid missing too much
    return datetime.now(timezone.utc) - timedelta(hours=1)

def save_last_timestamp(timestamp_utc):
    """
    Saves the provided timezone-aware UTC timestamp to the state file in ISO format.
    """
    try:
        # Ensure the timestamp is UTC before formatting
        if timestamp_utc.tzinfo is None:
             timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
        else:
             timestamp_utc = timestamp_utc.astimezone(timezone.utc)

        # Format the timestamp into ISO 8601 format with 'Z' for UTC
        state = {'last_timestamp_utc': timestamp_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}
        # Write the state to the JSON file
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        logger.debug(f"Saved last timestamp to '{STATE_FILE}': {state['last_timestamp_utc']}")
    except Exception as e:
        # Log errors during file writing
        logger.error(f"Error saving state to '{STATE_FILE}': {e}")

def create_kafka_producer(broker):
    """
    Attempts to create and connect a KafkaProducer instance.
    Retries connection if brokers are initially unavailable.
    Returns the producer instance or None if connection fails after retries.
    """
    producer = None
    retries = 5 # Number of connection attempts
    wait_time = 15 # Seconds to wait between retries
    while retries > 0 and producer is None:
        try:
            # Initialize the KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=broker, # Kafka broker address(es)
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize message values as JSON bytes
                linger_ms=100,       # Batch messages for up to 100ms
                retries=3,           # Retry sending failed messages 3 times
                request_timeout_ms=30000 # Timeout for producer requests
            )
            logger.info(f"Kafka producer connection established to {broker}")
            return producer # Return the connected producer
        except NoBrokersAvailable:
            # Handle case where Kafka broker is not reachable
            logger.warning(f"Kafka broker at {broker} not available. Retrying in {wait_time} seconds... ({retries} retries left)")
            retries -= 1
            time.sleep(wait_time)
        except KafkaError as e:
            # Handle other Kafka-related errors during initialization
             logger.error(f"Kafka error during producer creation: {e}")
             retries -=1
             time.sleep(wait_time)
        except Exception as e:
            # Handle unexpected errors
             logger.error(f"Unexpected error creating Kafka producer: {e}")
             retries -= 1
             time.sleep(wait_time)


    # If connection failed after all retries
    if producer is None:
         logger.error(f"FATAL: Failed to connect to Kafka broker at {broker} after multiple retries.")
         # Consider exiting or raising an exception depending on desired behavior
    return None # Indicate failure

def fetch_news(api_key, query, from_timestamp_utc):
    """
    Fetches news articles from the NewsAPI endpoint based on the query and timestamp.
    Returns a list of article dictionaries or an empty list on error.
    """
    # Check if the API key is configured
    if api_key == 'YOUR_NEWS_API_KEY_HERE' or not api_key:
        logger.error("News API key is not configured. Skipping fetch.")
        return []

    # Format the 'from' timestamp parameter required by NewsAPI (ISO 8601 UTC)
    from_param = from_timestamp_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Define query parameters for the NewsAPI request
    params = {
        'q': query,                # The search query string (keywords)
        'apiKey': api_key,         # Your API key
        'language': 'en',          # Filter for English articles
        'sortBy': 'publishedAt',   # Order results by publication time (newest first)
        'pageSize': 100,           # Request maximum articles per page (check API limits)
        'from': from_param         # Fetch articles published after this time
    }
    logger.info(f"Fetching news with query: '{query[:50]}...' from {from_param}")

    try:
        # Make the GET request to the NewsAPI endpoint
        response = requests.get(NEWS_API_ENDPOINT, params=params, timeout=30) # 30-second timeout
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        data = response.json()

        # Check the status field in the API response
        if data.get('status') == 'ok':
            articles = data.get('articles', [])
            logger.info(f"Fetched {len(articles)} articles (Total results available: {data.get('totalResults')}).")
            return articles # Return the list of articles
        else:
            # Log API-specific errors reported in the response
            logger.error(f"News API returned an error: {data.get('code')} - {data.get('message')}")
            return []

    except requests.exceptions.RequestException as e:
        # Handle network-related errors (timeout, connection error, etc.)
        logger.error(f"Network error fetching news from NewsAPI: {e}")
        return []
    except Exception as e:
        # Handle any other unexpected errors during the fetch
        logger.error(f"An unexpected error occurred during news fetching: {e}")
        return []

# --- Main Execution Loop ---

if __name__ == "__main__":
    logger.info("--- Starting News Producer Script ---")

    # Attempt to create the Kafka producer
    producer = create_kafka_producer(KAFKA_BROKER)

    # Load the timestamp of the last successfully processed article
    last_timestamp = load_last_timestamp()
    logger.info(f"Initial last timestamp loaded: {last_timestamp.isoformat()}")

    # Start fetching from a second after the last known timestamp to avoid duplicates
    fetch_from_timestamp = last_timestamp + timedelta(seconds=1)

    # Main loop to continuously fetch and publish news
    while True:
        # Check Kafka connection, attempt reconnect if necessary
        if producer is None or producer.bootstrap_connected() is False:
             logger.warning("Kafka producer is not connected. Attempting to reconnect...")
             producer = create_kafka_producer(KAFKA_BROKER)
             # If reconnection fails, wait longer before the next cycle
             if producer is None:
                  logger.error("Failed to reconnect to Kafka. Sleeping for 60 seconds.")
                  time.sleep(60)
                  continue # Skip this fetch cycle

        # Fetch new articles from the API
        articles = fetch_news(NEWS_API_KEY, NEWS_API_QUERY, fetch_from_timestamp)

        # Process fetched articles if any were returned
        if articles:
            newest_timestamp_in_batch = fetch_from_timestamp # Track the latest timestamp in this batch

            # Iterate through articles, process, and send to Kafka
            for article in articles:
                # --- Data Processing & Formatting ---
                published_at_str = article.get('publishedAt')
                # Skip article if timestamp is missing
                if not published_at_str:
                    logger.warning(f"Skipping article due to missing 'publishedAt': {article.get('title')}")
                    continue

                # Parse the timestamp string into a timezone-aware datetime object
                try:
                    article_timestamp = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"Could not parse timestamp: {published_at_str}. Skipping article.")
                    continue

                # Sanity check: Ensure we don't re-process articles older than the last known one
                # (This helps if the API unexpectedly returns older articles)
                if article_timestamp <= last_timestamp:
                    logger.debug(f"Skipping already processed article: {article.get('title')} ({published_at_str})")
                    continue

                # Create the message payload for Kafka
                message = {
                    'source_type': 'newsapi', # Identify the source
                    'source_name': article.get('source', {}).get('name'),
                    'author': article.get('author'),
                    'title': article.get('title'),
                    'description': article.get('description'), # Often a good summary
                    'url': article.get('url'),
                    'published_at_utc': article_timestamp.isoformat(), # Store timestamp consistently as ISO string
                    'content': article.get('content'), # Note: NewsAPI content is often truncated
                    'fetched_at_utc': datetime.now(timezone.utc).isoformat() # Timestamp when fetched
                }

                # --- Send to Kafka ---
                try:
                    # Send the message to the configured Kafka topic
                    # The value is automatically serialized to JSON bytes by the producer config
                    future = producer.send(KAFKA_TOPIC_NEWS, value=message)
                    # Optional: Block until message is sent (or timeout) - uncomment for debugging
                    # record_metadata = future.get(timeout=10)
                    # logger.debug(f"Sent message: {record_metadata.topic}/{record_metadata.partition} @ {record_metadata.offset}")
                except KafkaError as e:
                    # Log Kafka-specific errors during sending
                    logger.error(f"Failed to send message to Kafka topic '{KAFKA_TOPIC_NEWS}': {e}")
                    # Consider adding more robust error handling here (e.g., retry, dead-letter queue)
                except Exception as e:
                     # Log any other unexpected errors during sending
                     logger.error(f"Unexpected error sending message to Kafka: {e}")

                # Update the timestamp marker if this article is newer
                if article_timestamp > newest_timestamp_in_batch:
                    newest_timestamp_in_batch = article_timestamp
            # --- End of article loop ---

            # After processing all articles in the batch, update the persistent state
            if newest_timestamp_in_batch > last_timestamp:
                logger.info(f"Updating last timestamp from {last_timestamp.isoformat()} to {newest_timestamp_in_batch.isoformat()}")
                last_timestamp = newest_timestamp_in_batch
                save_last_timestamp(last_timestamp) # Persist the new timestamp
                # Set the start time for the *next* fetch slightly after the newest article found
                fetch_from_timestamp = last_timestamp + timedelta(seconds=1)
            else:
                # If no newer articles were found in this batch, keep the next fetch time the same
                 logger.info("No articles newer than the last timestamp were processed in this batch.")


        # Wait for the configured interval before the next fetch cycle
        logger.info(f"Sleeping for {FETCH_INTERVAL_SECONDS} seconds before next fetch...")
        time.sleep(FETCH_INTERVAL_SECONDS)
        # --- End of main loop ---


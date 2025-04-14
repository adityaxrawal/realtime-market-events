# data-pipeline/producers/news_producer.py
import os
import time
import json
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- Configuration ---
load_dotenv() # Load environment variables from .env file in the project root
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use environment variables with defaults
# KAFKA_BROKER should point to the EXTERNAL listener defined in docker-compose.yml if running this script outside Docker
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = "raw_news"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_API_URL = "https://finnhub.io/api/v1/news"
# Focus on general market news initially, can be expanded
NEWS_CATEGORY = "general" # Finnhub categories: general, forex, crypto, merger
FETCH_INTERVAL_SECONDS = 300 # Fetch every 5 minutes (adjust based on API limits and needs)
RECONNECT_DELAY_SECONDS = 10 # Delay before retrying Kafka connection

# Validate Finnhub API Key
if not FINNHUB_API_KEY:
    logger.error("FINNHUB_API_KEY not found in environment variables. Please set it in your .env file.")
    exit(1)

# --- Kafka Producer Setup ---
def create_kafka_producer():
    """Creates and returns a KafkaProducer instance with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize messages as JSON
                retries=5, # Number of retries on send failure
                retry_backoff_ms=1000 # Wait 1s before retrying
            )
            logger.info(f"KafkaProducer connected to {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
             logger.error(f"Kafka broker at {KAFKA_BROKER} not available. Is Kafka running and the port mapped correctly? Retrying in {RECONNECT_DELAY_SECONDS}s...")
             time.sleep(RECONNECT_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"Failed to connect Kafka Producer to {KAFKA_BROKER}: {e}. Retrying in {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)

# --- Finnhub API Fetching ---
def fetch_finnhub_news(last_fetch_time=None):
    """
    Fetches news from Finnhub API based on category.
    Note: Free tier limitations mean we fetch the latest general news without fine-grained filtering.
    """
    params = {
        'category': NEWS_CATEGORY,
        'token': FINNHUB_API_KEY,
    }
    logger.info(f"Fetching news from Finnhub for category: {NEWS_CATEGORY}")
    try:
        response = requests.get(FINNHUB_API_URL, params=params, timeout=15) # 15-second timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        news_items = response.json()

        # Validate response format
        if not isinstance(news_items, list):
            logger.warning(f"Unexpected response format from Finnhub: {type(news_items)}. Expected list.")
            return []

        logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
        return news_items
    except requests.exceptions.Timeout:
        logger.error("Timeout occurred while fetching news from Finnhub.")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching news from Finnhub: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding Finnhub API response: {e}")
        return []
    except Exception as e:
         logger.error(f"An unexpected error occurred during Finnhub fetch: {e}", exc_info=True)
         return []

# --- Main Loop ---
def run_producer():
    """Main loop to fetch news and send to Kafka."""
    producer = create_kafka_producer()
    # last_id_processed = None # Optional: track last ID if API supported it well for deduplication

    while True:
        try:
            news_data = fetch_finnhub_news() # Fetch latest news
            processed_count = 0
            if news_data:
                # Process news items (e.g., send to Kafka)
                for item in news_data:
                    # Basic filtering (can be enhanced)
                    if 'headline' not in item or not item['headline'] or 'id' not in item:
                        logger.debug(f"Skipping news item due to missing headline or id: {item.get('id', 'N/A')}")
                        continue

                    # Structure the message for Kafka
                    message = {
                        'id': item.get('id'),
                        'source': item.get('source'),
                        'timestamp': item.get('datetime'), # Unix timestamp provided by Finnhub
                        'headline': item.get('headline'),
                        'summary': item.get('summary'),
                        'url': item.get('url'),
                        'category': item.get('category'),
                        'related': item.get('related'), # Often contains stock symbols
                        'fetch_time': datetime.utcnow().isoformat() # Add timestamp when we fetched it
                    }
                    # Send the message to the Kafka topic
                    producer.send(KAFKA_TOPIC, value=message)
                    logger.debug(f"Sent news item ID {item.get('id')} to Kafka topic '{KAFKA_TOPIC}'.")
                    processed_count += 1

                # Ensure messages are sent before sleeping
                if processed_count > 0:
                    producer.flush()
                    logger.info(f"Successfully sent {processed_count} items to Kafka.")
                else:
                    logger.info("No new valid news items processed in this cycle.")

        except Exception as e:
            logger.error(f"Error in main producer loop: {e}", exc_info=True)
            # Attempt to recreate producer if connection is likely lost
            if "Broker not available" in str(e) or "Connection refused" in str(e) or isinstance(e, (ConnectionRefusedError, NoBrokersAvailable)):
                logger.info("Attempting to recreate Kafka producer...")
                if producer:
                     try: producer.close()
                     except Exception as close_err: logger.warning(f"Error closing producer: {close_err}")
                producer = create_kafka_producer() # Reconnect
            else:
                 # For other errors, wait before retrying the fetch/send cycle
                 time.sleep(FETCH_INTERVAL_SECONDS) # Wait before next cycle on other errors

        # Wait before the next fetch cycle
        logger.info(f"Sleeping for {FETCH_INTERVAL_SECONDS} seconds...")
        time.sleep(FETCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    logger.info("Starting Finnhub News Producer...")
    run_producer()

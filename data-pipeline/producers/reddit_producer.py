# data-pipeline/producers/reddit_producer.py
import os
import time
import json
import logging
import praw
from praw.exceptions import PRAWException
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration ---
load_dotenv() # Load environment variables from .env file in the project root
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092") # Use port exposed in docker-compose
KAFKA_TOPIC = "raw_reddit_posts"

# Reddit API Credentials (from .env file)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

# Subreddits to monitor (adjust as needed)
# Consider adding more finance-specific subreddits if relevant
SUBREDDITS_TO_MONITOR = [
    "IndianStockMarket",
    "StockMarket",
    "stocks",
    "investing",
    "wallstreetbets", # Can be noisy, but sometimes relevant
    "personalfinanceindia",
    "IndiaInvestments"
]
# Limit posts fetched per subreddit per cycle to respect rate limits (100 QPM free tier)
# Fetching fewer posts more often might be better than many posts less often.
POST_LIMIT_PER_SUB = 10
# Interval between checking for new posts (adjust carefully based on rate limits)
SLEEP_INTERVAL_SECONDS = 60 # Check every 1 minute
RECONNECT_DELAY_SECONDS = 15 # Delay before retrying client connections

# --- Input Validation ---
if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT]):
    logger.error("Reddit API credentials (CLIENT_ID, CLIENT_SECRET, USER_AGENT) not found. Please set them in your .env file.")
    exit(1)
if "your_descriptive_user_agent" in REDDIT_USER_AGENT or "YourUsername" in REDDIT_USER_AGENT:
    logger.warning("Please set a unique and descriptive Reddit User Agent in your .env file for API compliance.")
    # Consider exiting if not set: exit(1)

# --- Kafka Producer Setup ---
def create_kafka_producer():
    """Creates and returns a KafkaProducer instance with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize messages as JSON
                retries=5,
                retry_backoff_ms=1000
            )
            logger.info(f"KafkaProducer connected to {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
             logger.error(f"Kafka broker at {KAFKA_BROKER} not available. Retrying in {RECONNECT_DELAY_SECONDS}s...")
             time.sleep(RECONNECT_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"Failed to connect Kafka Producer to {KAFKA_BROKER}: {e}. Retrying in {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)

# --- Reddit Client Setup ---
def create_reddit_client():
     """Creates and returns a PRAW Reddit instance with retry logic."""
     while True:
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
                # Read-only mode is sufficient for fetching posts
                # check_for_async=False # Can sometimes help with compatibility
            )
            # Test connection by fetching a known attribute (lazy operation)
            _ = reddit.config.reddit_url
            logger.info(f"Reddit client initialized successfully with user agent: {REDDIT_USER_AGENT}")
            return reddit
        except PRAWException as e:
            logger.error(f"PRAWException during Reddit client initialization: {e}. Retrying in {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"Failed to initialize Reddit client: {e}. Retrying in {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)

# --- Data Fetching and Processing ---
def fetch_and_send_posts(reddit, producer, processed_post_ids):
    """Fetches new posts from monitored subreddits and sends them to Kafka."""
    total_new_posts_sent = 0
    subreddit_string = "+".join(SUBREDDITS_TO_MONITOR) # PRAW syntax for multi-reddit
    logger.info(f"Fetching up to {POST_LIMIT_PER_SUB} new posts from subreddits: {subreddit_string}")

    try:
        subreddit = reddit.subreddit(subreddit_string)
        # Fetch new posts, limited to avoid hitting rate limits quickly
        # PRAW handles pagination implicitly when iterating .new()
        posts_fetched_this_cycle = 0
        for submission in subreddit.new(limit=POST_LIMIT_PER_SUB * len(SUBREDDITS_TO_MONITOR)): # Fetch a bit more to ensure we get enough new ones
            if posts_fetched_this_cycle >= POST_LIMIT_PER_SUB: # Apply per-cycle limit strictly
                 break
            if submission.id in processed_post_ids:
                # logger.debug(f"Skipping already processed post ID {submission.id}")
                continue # Skip already processed posts

            # Structure the message for Kafka
            message = {
                'id': submission.id,
                'title': submission.title,
                'text': submission.selftext, # Body text for self-posts, empty for link posts
                'author': str(submission.author), # Convert Redditor object to string, might be None
                'timestamp': int(submission.created_utc), # Unix timestamp (seconds)
                'subreddit': submission.subreddit.display_name,
                'url': submission.url, # URL of the post itself (permalink) or linked content
                'permalink': f"https://www.reddit.com{submission.permalink}", # Full permalink
                'score': submission.score,
                'num_comments': submission.num_comments,
                'is_self': submission.is_self, # True if it's a text post
                'fetch_time': datetime.utcnow().isoformat() # Timestamp when we fetched it
            }
            producer.send(KAFKA_TOPIC, value=message)
            logger.debug(f"Sent post ID {submission.id} from r/{message['subreddit']} to Kafka.")
            processed_post_ids.add(submission.id) # Add to processed set
            total_new_posts_sent += 1
            posts_fetched_this_cycle += 1
            # Small delay between processing posts to be slightly gentler on API
            time.sleep(0.1)

    except PRAWException as e:
        logger.error(f"PRAW error fetching posts: {e}")
        # Handle specific errors like rate limits if needed
        if "received 429" in str(e).lower() or "Too Many Requests" in str(e):
            logger.warning("Rate limit likely hit. Sleeping for longer interval...")
            time.sleep(SLEEP_INTERVAL_SECONDS * 3) # Exponential backoff might be better
        # Handle authentication errors separately if using user context
        elif "invalid_grant" in str(e).lower() or "invalid_request" in str(e).lower():
             logger.error(f"Authentication error with Reddit API: {e}. Check credentials.")
             # Consider stopping the script or implementing credential refresh logic
             raise e # Re-raise to potentially trigger client recreation
        else:
             # Wait before retrying on other PRAW errors
             time.sleep(RECONNECT_DELAY_SECONDS)

    except Exception as e:
        logger.error(f"Unexpected error fetching/sending posts: {e}", exc_info=True)
        time.sleep(RECONNECT_DELAY_SECONDS) # Wait before retrying

    if total_new_posts_sent > 0:
        try:
            producer.flush() # Ensure messages are sent before logging success
            logger.info(f"Processed and sent {total_new_posts_sent} new posts to Kafka.")
        except Exception as flush_err:
             logger.error(f"Error flushing Kafka producer: {flush_err}")
             # Consider producer reconnection logic here as well
    else:
        logger.info("No new posts found or processed in this cycle.")

    # --- Optional: Fetch Comments ---
    # Fetching comments for each post significantly increases API calls and complexity.
    # Recommended to handle comment fetching/processing in a separate consumer/Flink job
    # triggered by the raw_reddit_posts topic to manage rate limits and logic separation.


# --- Main Loop ---
def run_producer():
    """Main loop to fetch Reddit posts and send to Kafka."""
    producer = create_kafka_producer()
    reddit = create_reddit_client()
    # Use a simple in-memory set for processed IDs. For long-running, fault-tolerant
    # applications, use a persistent store (like Redis, DB, or Flink state) instead.
    processed_post_ids = set()

    while True:
        try:
            fetch_and_send_posts(reddit, producer, processed_post_ids)

            # Optional: Simple mechanism to prevent the in-memory set from growing indefinitely
            if len(processed_post_ids) > 50000: # Example limit
                logger.warning("Processed ID set exceeds limit. Clearing older entries.")
                # This is a basic strategy; might reprocess old posts on restart.
                # A time-based eviction or persistent store is more robust.
                sorted_ids = sorted(list(processed_post_ids), key=lambda x: int(x, 36), reverse=True) # Sort by approx creation time
                processed_post_ids = set(sorted_ids[:25000]) # Keep roughly half

        except PRAWException as e:
             # Attempt to recreate Reddit client if PRAW exception occurs in main loop
             logger.error(f"PRAWException in main loop, attempting to recreate Reddit client: {e}")
             reddit = create_reddit_client() # Recreate client
             time.sleep(RECONNECT_DELAY_SECONDS) # Wait after recreating
        except Exception as e:
             logger.error(f"Error in main Reddit producer loop: {e}", exc_info=True)
             # Attempt to recreate Kafka producer if connection seems lost
             if "Broker not available" in str(e) or "Connection refused" in str(e) or isinstance(e, (ConnectionRefusedError, NoBrokersAvailable)):
                  logger.info("Attempting to recreate Kafka producer...")
                  if producer:
                       try: producer.close()
                       except Exception as close_err: logger.warning(f"Error closing producer: {close_err}")
                  producer = create_kafka_producer() # Reconnect
             else:
                  # Wait before retrying general fetch cycle for other errors
                   time.sleep(SLEEP_INTERVAL_SECONDS)

        # Wait before the next fetch cycle
        logger.info(f"Sleeping for {SLEEP_INTERVAL_SECONDS} seconds before next Reddit fetch...")
        time.sleep(SLEEP_INTERVAL_SECONDS)

if __name__ == "__main__":
    logger.info("Starting Reddit Producer...")
    run_producer()


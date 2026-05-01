import asyncio
import json
import os
import logging
from collections import deque
from datetime import datetime, timezone, timedelta
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [ALERT-WORKER] - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SENTIMENT_TOPIC = "sentiment-results"

# Thresholds
ALERT_TIME_WINDOW_MINUTES = 15
ALERT_CONSECUTIVE_NEGATIVE = 5

class AlertSystem:
    def __init__(self):
        self.ticker_history = {}  # ticker -> deque of (timestamp, sentiment)

    def process_result(self, ticker, sentiment):
        now = datetime.now(timezone.utc)
        
        if ticker not in self.ticker_history:
            self.ticker_history[ticker] = deque()
            
        history = self.ticker_history[ticker]
        history.append((now, sentiment))
        
        # Prune old records outside the time window
        cutoff = now - timedelta(minutes=ALERT_TIME_WINDOW_MINUTES)
        while history and history[0][0] < cutoff:
            history.popleft()
            
        # Check condition: If we have enough negative records recently
        negative_count = sum(1 for _, sent in history if sent == 'negative')
        if negative_count >= ALERT_CONSECUTIVE_NEGATIVE:
            self.trigger_alert(ticker, negative_count)
            history.clear() # Reset after triggering
            
    def trigger_alert(self, ticker, count):
        logger.warning(f"🚨 CRITICAL ALERT TRIGGERED: {ticker} has {count} negative articles in the last {ALERT_TIME_WINDOW_MINUTES} minutes!")
        # Here you would implement requests.post() to Discord/Slack webhooks.

async def main():
    logger.info("Starting Alert Worker...")
    consumer = AIOKafkaConsumer(
        SENTIMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert_group",
        auto_offset_reset="latest" # We only care about new alerts from now on
    )
    
    await consumer.start()
    alerter = AlertSystem()
    
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
                ticker = data.get("company_ticker")
                sentiment = data.get("sentiment")
                if ticker and sentiment:
                    alerter.process_result(ticker, sentiment)
            except Exception as e:
                logger.error(f"Error parsing sentiment message: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Alert Worker shutting down.")
